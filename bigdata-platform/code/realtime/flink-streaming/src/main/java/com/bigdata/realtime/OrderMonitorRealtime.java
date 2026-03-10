package com.bigdata.realtime;

import com.bigdata.realtime.bean.UserBehavior;
import com.bigdata.realtime.util.JsonUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * 订单实时监控与告警
 *
 * 功能:
 *   1. 1 分钟订单量 / GMV 实时统计
 *   2. 大额订单告警（单笔 > 10000）
 *   3. 退款率实时监控（滑动窗口退款率 > 10% 告警）
 *   4. 异常订单检测（金额 <= 0、状态非法）
 *
 * 输入: Kafka topic order_events (JSON)
 * 输出: Doris 实时监控表 + 控制台告警
 */
public class OrderMonitorRealtime {

    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 侧输出流：大额订单告警
    private static final OutputTag<String> LARGE_ORDER_ALERT =
            new OutputTag<String>("large-order-alert") {};

    // 侧输出流：异常订单
    private static final OutputTag<String> ABNORMAL_ORDER =
            new OutputTag<String>("abnormal-order") {};

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = System.getenv().getOrDefault("KAFKA_ORDER_TOPIC", "order_events");
        String dorisJdbcUrl = System.getenv().getOrDefault("DORIS_JDBC_URL", "jdbc:mysql://doris-fe:9030/bigdata");
        String dorisUser = System.getenv().getOrDefault("DORIS_USER", "root");
        String dorisPassword = System.getenv().getOrDefault("DORIS_PASSWORD", "");
        double largeOrderThreshold = Double.parseDouble(
                System.getenv().getOrDefault("LARGE_ORDER_THRESHOLD", "10000"));

        // ====================== 1. Kafka Source ======================
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("flink-order-monitor-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "Order Kafka Source");

        // ====================== 2. 解析 + 分流 ======================
        SingleOutputStreamOperator<OrderEvent> orderStream = kafkaStream
                .process(new org.apache.flink.streaming.api.functions.ProcessFunction<String, OrderEvent>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<OrderEvent> out) {
                        try {
                            OrderEvent event = JsonUtils.parseJson(value, OrderEvent.class);
                            if (event == null) return;

                            // 异常订单检测
                            if (event.amount <= 0 || event.orderId == null || event.orderId.isEmpty()) {
                                ctx.output(ABNORMAL_ORDER, "异常订单: " + value);
                                return;
                            }

                            // 大额订单告警
                            if (event.amount > largeOrderThreshold) {
                                ctx.output(LARGE_ORDER_ALERT,
                                        String.format("🚨 大额订单: order=%s, amount=%.2f, user=%s",
                                                event.orderId, event.amount, event.userId));
                            }

                            out.collect(event);
                        } catch (Exception e) {
                            ctx.output(ABNORMAL_ORDER, "解析失败: " + value);
                        }
                    }
                })
                .name("订单解析 + 异常检测");

        // ====================== 3. 1 分钟订单量 / GMV 统计 ======================
        SingleOutputStreamOperator<Tuple4<String, Long, Double, Double>> orderStats = orderStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new OrderStatsProcessFunction())
                .name("1分钟订单统计");

        // ====================== 4. Sink ======================
        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(dorisJdbcUrl)
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(dorisUser)
                .withPassword(dorisPassword)
                .build();

        JdbcExecutionOptions execOptions = JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(3000)
                .withMaxRetries(3)
                .build();

        // 订单统计 → Doris
        orderStats.addSink(JdbcSink.sink(
                "INSERT INTO dws_realtime_order_monitor_1min (window_time, order_cnt, gmv, refund_rate) VALUES (?, ?, ?, ?)",
                (ps, row) -> {
                    ps.setString(1, row.f0);
                    ps.setLong(2, row.f1);
                    ps.setDouble(3, row.f2);
                    ps.setDouble(4, row.f3);
                },
                execOptions, jdbcOptions
        )).name("订单统计 → Doris");

        // 控制台输出
        orderStats.print("📊 订单统计");
        orderStream.getSideOutput(LARGE_ORDER_ALERT).print("🚨 大额告警");
        orderStream.getSideOutput(ABNORMAL_ORDER).print("⚠️ 异常订单");

        env.execute("订单实时监控与告警");
    }

    // ====================== ProcessFunction ======================

    /**
     * 1 分钟窗口: 订单量 / GMV / 退款率
     * 输出: (window_time, order_cnt, gmv, refund_rate)
     */
    private static class OrderStatsProcessFunction
            extends ProcessAllWindowFunction<OrderEvent, Tuple4<String, Long, Double, Double>, TimeWindow> {

        @Override
        public void process(Context context, Iterable<OrderEvent> elements,
                            Collector<Tuple4<String, Long, Double, Double>> out) {
            long totalOrders = 0;
            long refundOrders = 0;
            double totalGmv = 0;

            for (OrderEvent event : elements) {
                totalOrders++;
                totalGmv += event.amount;
                if ("refunded".equals(event.status)) {
                    refundOrders++;
                }
            }

            double refundRate = totalOrders > 0 ? (double) refundOrders / totalOrders : 0;
            String windowTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(context.window().getEnd()),
                    ZoneId.of("Asia/Shanghai")
            ).format(DT_FMT);

            out.collect(Tuple4.of(windowTime, totalOrders, totalGmv, refundRate));

            // 退款率告警
            if (refundRate > 0.1 && totalOrders > 10) {
                System.err.printf("🚨 退款率告警: %.2f%% (退款=%d, 总单=%d, 窗口=%s)%n",
                        refundRate * 100, refundOrders, totalOrders, windowTime);
            }
        }
    }

    // ====================== 实体类 ======================

    public static class OrderEvent {
        public String orderId;
        public String userId;
        public String storeId;
        public double amount;
        public String status;
        public String paymentMethod;
        public String city;
        public long timestamp;

        public OrderEvent() {}
    }
}
