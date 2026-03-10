package com.bigdata.realtime;

import com.bigdata.realtime.bean.UserBehavior;
import com.bigdata.realtime.util.JsonUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

/**
 * 实时用户行为分析（5 分钟滚动窗口）
 *
 * 输入: Kafka topic user_behavior (JSON)
 * 输出:
 *   1. 5 分钟 PV/UV → Doris dws_realtime_pv_uv_5min
 *   2. 行为类型分布 → Doris dws_realtime_action_5min
 *   3. 热门商品 TOP → Doris dws_realtime_product_hot_5min
 */
public class UserBehaviorRealtime {

    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {

        // ====================== 1. 初始化 ======================
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.getConfig().setAutoWatermarkInterval(1000L);

        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "user_behavior");
        String dorisJdbcUrl = System.getenv().getOrDefault("DORIS_JDBC_URL", "jdbc:mysql://doris-fe:9030/bigdata");
        String dorisUser = System.getenv().getOrDefault("DORIS_USER", "root");
        String dorisPassword = System.getenv().getOrDefault("DORIS_PASSWORD", "");

        // ====================== 2. Kafka Source ======================
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("flink-user-behavior-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // ====================== 3. 数据清洗 + 水位线 ======================
        SingleOutputStreamOperator<UserBehavior> cleanStream = kafkaStream
                .flatMap((String value, Collector<UserBehavior> out) -> {
                    try {
                        UserBehavior behavior = JsonUtils.parseJson(value, UserBehavior.class);
                        if (behavior != null && behavior.isValid()) {
                            out.collect(behavior);
                        }
                    } catch (Exception e) {
                        // 跳过解析失败的消息
                    }
                })
                .returns(UserBehavior.class)
                .name("JSON 解析与数据清洗");

        SingleOutputStreamOperator<UserBehavior> watermarked = cleanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, ts) -> event.getTimestamp())
                                .withIdleness(Duration.ofSeconds(60))
                );

        // ====================== 4. PV / UV 统计 ======================
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> pvUvStream = watermarked
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new PvUvProcessFunction())
                .name("PV/UV 统计");

        // ====================== 5. 行为类型分布 ======================
        SingleOutputStreamOperator<Tuple3<String, String, Long>> actionStream = watermarked
                .keyBy(UserBehavior::getAction)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new ActionCountProcessFunction())
                .name("行为类型统计");

        // ====================== 6. 热门商品 ======================
        SingleOutputStreamOperator<Tuple3<String, String, Long>> productStream = watermarked
                .keyBy(UserBehavior::getProductId)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new ProductCountProcessFunction())
                .name("热门商品统计");

        // ====================== 7. Sink 输出 ======================
        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(dorisJdbcUrl)
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(dorisUser)
                .withPassword(dorisPassword)
                .build();

        JdbcExecutionOptions execOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(5000)
                .withMaxRetries(3)
                .build();

        // 7a. PV/UV → Doris
        pvUvStream.addSink(JdbcSink.sink(
                "INSERT INTO dws_realtime_pv_uv_5min (window_time, pv, uv) VALUES (?, ?, ?)",
                (ps, row) -> {
                    ps.setString(1, row.f0);
                    ps.setLong(2, row.f1);
                    ps.setLong(3, row.f2);
                },
                execOptions, jdbcOptions
        )).name("PV/UV → Doris");

        // 7b. 行为类型 → Doris
        actionStream.addSink(JdbcSink.sink(
                "INSERT INTO dws_realtime_action_5min (window_time, action, action_cnt) VALUES (?, ?, ?)",
                (ps, row) -> {
                    ps.setString(1, row.f0);
                    ps.setString(2, row.f1);
                    ps.setLong(3, row.f2);
                },
                execOptions, jdbcOptions
        )).name("行为类型 → Doris");

        // 7c. 热门商品 → Doris
        productStream.addSink(JdbcSink.sink(
                "INSERT INTO dws_realtime_product_hot_5min (window_time, product_id, click_cnt) VALUES (?, ?, ?)",
                (ps, row) -> {
                    ps.setString(1, row.f0);
                    ps.setString(2, row.f1);
                    ps.setLong(3, row.f2);
                },
                execOptions, jdbcOptions
        )).name("热门商品 → Doris");

        // 同时保留控制台输出（开发调试用）
        pvUvStream.print("PV/UV");
        actionStream.print("行为类型");
        productStream.print("热门商品");

        // ====================== 8. 执行 ======================
        env.execute("实时用户行为分析 (5分钟窗口)");
    }

    // ====================== ProcessFunction 实现 ======================

    /**
     * PV/UV 全窗口统计
     * 输出: (window_time, pv, uv)
     */
    private static class PvUvProcessFunction
            extends ProcessAllWindowFunction<UserBehavior, Tuple3<String, Long, Long>, TimeWindow> {

        @Override
        public void process(Context context, Iterable<UserBehavior> elements,
                            Collector<Tuple3<String, Long, Long>> out) {
            Set<String> userIds = new HashSet<>();
            long pv = 0;

            for (UserBehavior behavior : elements) {
                pv++;
                userIds.add(behavior.getUserId());
            }

            String windowTime = formatWindowTime(context.window().getEnd());
            out.collect(Tuple3.of(windowTime, pv, (long) userIds.size()));
        }
    }

    /**
     * 行为类型分布统计
     * 输出: (window_time, action, count)
     */
    private static class ActionCountProcessFunction
            extends ProcessWindowFunction<UserBehavior, Tuple3<String, String, Long>, String, TimeWindow> {

        @Override
        public void process(String action, Context context, Iterable<UserBehavior> elements,
                            Collector<Tuple3<String, String, Long>> out) {
            long count = 0;
            for (UserBehavior ignored : elements) {
                count++;
            }
            String windowTime = formatWindowTime(context.window().getEnd());
            out.collect(Tuple3.of(windowTime, action, count));
        }
    }

    /**
     * 热门商品统计
     * 输出: (window_time, product_id, click_count)
     */
    private static class ProductCountProcessFunction
            extends ProcessWindowFunction<UserBehavior, Tuple3<String, String, Long>, String, TimeWindow> {

        @Override
        public void process(String productId, Context context, Iterable<UserBehavior> elements,
                            Collector<Tuple3<String, String, Long>> out) {
            long count = 0;
            for (UserBehavior ignored : elements) {
                count++;
            }
            String windowTime = formatWindowTime(context.window().getEnd());
            out.collect(Tuple3.of(windowTime, productId, count));
        }
    }

    // ====================== 辅助方法 ======================

    private static String formatWindowTime(long windowEnd) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(windowEnd), ZoneId.of("Asia/Shanghai"))
                .format(DT_FMT);
    }
}
