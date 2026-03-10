package com.bigdata.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Kafka 工具类
 * 提供生产者/消费者封装
 */
public class KafkaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // ====================== 生产者 ======================

    /**
     * 创建 Kafka 生产者
     */
    public static KafkaProducer<String, String> createProducer(String bootstrapServers) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        return new KafkaProducer<>(props);
    }

    /**
     * 创建带有自定义配置的生产者
     */
    public static KafkaProducer<String, String> createProducer(String bootstrapServers, Map<String, Object> customConfig) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        if (customConfig != null) {
            props.putAll(customConfig);
        }

        return new KafkaProducer<>(props);
    }

    /**
     * 发送字符串消息
     */
    public static void sendMessage(KafkaProducer<String, String> producer, String topic, String key, String value) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Kafka 发送失败: topic={}, key={}", topic, key, exception);
                } else {
                    LOG.debug("Kafka 发送成功: topic={}, partition={}, offset={}",
                            topic, metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            LOG.error("Kafka 发送异常: topic={}, key={}", topic, key, e);
        }
    }

    /**
     * 发送对象消息（自动转为 JSON）
     */
    public static <T> void sendObject(KafkaProducer<String, String> producer, String topic, String key, T object) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(object);
            sendMessage(producer, topic, key, json);
        } catch (JsonProcessingException e) {
            LOG.error("对象转 JSON 失败: {} {}", key, object, e);
        }
    }

    /**
     * 同步发送消息
     */
    public static void sendSync(KafkaProducer<String, String> producer, String topic, String key, String value) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record).get();
            LOG.debug("Kafka 同步发送成功: topic={}, key={}", topic, key);
        } catch (Exception e) {
            LOG.error("Kafka 同步发送失败: topic={}, key={}", topic, key, e);
        }
    }

    // ====================== 消费者 ======================

    /**
     * 创建 Kafka 消费者
     */
    public static KafkaConsumer<String, String> createConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

        return new KafkaConsumer<>(props);
    }

    /**
     * 消费消息（回调处理）
     */
    public static void consume(String bootstrapServers, String groupId, String topic,
                               MessageHandler messageHandler) {

        KafkaConsumer<String, String> consumer = createConsumer(bootstrapServers, groupId);
        consumer.subscribe(Collections.singletonList(topic));

        AtomicBoolean running = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            consumer.wakeup();
            LOG.info("Kafka 消费者收到关闭信号");
        }));

        LOG.info("Kafka 消费者已启动: topic={}, groupId={}", topic, groupId);

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    try {
                        for (ConsumerRecord<String, String> record : records) {
                            messageHandler.handle(
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    record.key(),
                                    record.value(),
                                    record.timestamp()
                            );
                        }

                        consumer.commitSync();
                        LOG.debug("已提交偏移量: 共 {} 条消息", records.count());

                    } catch (Exception e) {
                        LOG.error("消息处理失败，回滚偏移量", e);
                        Map<TopicPartition, OffsetAndMetadata> offsets = records.partitions()
                                .stream()
                                .collect(Collectors.toMap(
                                        partition -> partition,
                                        partition -> new OffsetAndMetadata(
                                                records.records(partition).get(0).offset()
                                        )
                                ));
                        consumer.commitSync(offsets);
                    }
                }
            }
        } catch (WakeupException e) {
            if (running.get()) throw e;
        } finally {
            consumer.close();
            LOG.info("Kafka 消费者已关闭");
        }
    }

    /**
     * 消费消息并转为对象
     */
    public static <T> void consumeObject(String bootstrapServers, String groupId, String topic,
                                         Class<T> clazz, ObjectMessageHandler<T> messageHandler) {

        consume(bootstrapServers, groupId, topic, (t, partition, offset, key, value, timestamp) -> {
            try {
                T object = OBJECT_MAPPER.readValue(value, clazz);
                messageHandler.handle(t, partition, offset, key, object, timestamp);
            } catch (JsonProcessingException e) {
                LOG.error("JSON 解析失败: topic={}, key={}, value={}", t, key, value, e);
            }
        });
    }

    // ====================== 回调接口 ======================

    @FunctionalInterface
    public interface MessageHandler {
        void handle(String topic, int partition, long offset, String key, String value, long timestamp);
    }

    @FunctionalInterface
    public interface ObjectMessageHandler<T> {
        void handle(String topic, int partition, long offset, String key, T value, long timestamp);
    }

    // ====================== 关闭资源 ======================

    public static void closeProducer(KafkaProducer<String, String> producer) {
        if (producer != null) {
            try {
                producer.flush();
                producer.close();
                LOG.info("Kafka 生产者已关闭");
            } catch (Exception e) {
                LOG.error("关闭生产者失败", e);
            }
        }
    }

    public static void closeConsumer(KafkaConsumer<String, String> consumer) {
        if (consumer != null) {
            try {
                consumer.close();
                LOG.info("Kafka 消费者已关闭");
            } catch (Exception e) {
                LOG.error("关闭消费者失败", e);
            }
        }
    }
}
