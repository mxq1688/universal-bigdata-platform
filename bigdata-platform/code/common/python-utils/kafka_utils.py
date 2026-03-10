"""
Kafka 工具类 (Python 版)
基于 kafka-python，提供生产者/消费者封装
"""
import json
import logging
import os
from typing import Callable, Optional

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaUtils:
    """Kafka 生产者/消费者工具"""

    def __init__(self, bootstrap_servers="localhost:9092"):
        self.bootstrap_servers = bootstrap_servers

    # ====================== 生产者 ======================

    def create_producer(self, **kwargs):
        """创建 Kafka 生产者"""
        defaults = {
            "bootstrap_servers": self.bootstrap_servers,
            "key_serializer": lambda k: k.encode("utf-8") if k else None,
            "value_serializer": lambda v: v.encode("utf-8") if isinstance(v, str) else json.dumps(v).encode("utf-8"),
            "acks": "all",
            "retries": 3,
            "batch_size": 16384,
            "linger_ms": 5,
            "buffer_memory": 33554432,
        }
        defaults.update(kwargs)
        producer = KafkaProducer(**defaults)
        logger.info("Kafka 生产者已创建: %s", self.bootstrap_servers)
        return producer

    def send_message(self, producer, topic, key, value):
        """发送消息"""
        try:
            future = producer.send(topic, key=key, value=value)
            metadata = future.get(timeout=10)
            logger.debug("发送成功: topic=%s partition=%s offset=%s",
                         topic, metadata.partition, metadata.offset)
            return metadata
        except KafkaError as e:
            logger.error("发送失败: topic=%s key=%s error=%s", topic, key, e)
            raise

    def send_batch(self, producer, topic, messages):
        """批量发送消息: messages = [(key, value), ...]"""
        futures = []
        for key, value in messages:
            future = producer.send(topic, key=key, value=value)
            futures.append(future)
        producer.flush()

        success = 0
        for future in futures:
            try:
                future.get(timeout=10)
                success += 1
            except KafkaError as e:
                logger.error("批量发送部分失败: %s", e)

        logger.info("批量发送完成: %s/%s 条成功", success, len(messages))
        return success

    # ====================== 消费者 ======================

    def create_consumer(self, group_id, topics, **kwargs):
        """创建 Kafka 消费者"""
        defaults = {
            "bootstrap_servers": self.bootstrap_servers,
            "group_id": group_id,
            "key_deserializer": lambda k: k.decode("utf-8") if k else None,
            "value_deserializer": lambda v: v.decode("utf-8") if v else None,
            "auto_offset_reset": "latest",
            "enable_auto_commit": False,
            "max_poll_records": 500,
        }
        defaults.update(kwargs)

        consumer = KafkaConsumer(**defaults)
        consumer.subscribe(topics if isinstance(topics, list) else [topics])
        logger.info("Kafka 消费者已创建: %s group=%s topics=%s",
                     self.bootstrap_servers, group_id, topics)
        return consumer

    def consume(self, group_id, topic, handler: Callable, json_decode=False,
                poll_timeout_ms=100, max_messages=None):
        """
        消费消息

        Args:
            group_id: 消费者组 ID
            topic: 主题
            handler: 消息处理函数 handler(key, value, partition, offset, timestamp)
            json_decode: 是否自动解析 JSON
            poll_timeout_ms: 拉取超时
            max_messages: 最大消费条数 (None=无限)
        """
        consumer = self.create_consumer(group_id, topic)
        count = 0

        try:
            logger.info("开始消费: topic=%s group=%s", topic, group_id)
            while True:
                records = consumer.poll(timeout_ms=poll_timeout_ms)

                for tp, messages in records.items():
                    for msg in messages:
                        try:
                            value = msg.value
                            if json_decode and value:
                                value = json.loads(value)

                            handler(msg.key, value, msg.partition, msg.offset, msg.timestamp)
                            count += 1

                        except Exception as e:
                            logger.error("消息处理失败: partition=%s offset=%s error=%s",
                                         msg.partition, msg.offset, e)

                if records:
                    consumer.commit()
                    logger.debug("已提交偏移量, 累计处理 %s 条", count)

                if max_messages and count >= max_messages:
                    logger.info("达到最大消费数 %s, 停止消费", max_messages)
                    break

        except KeyboardInterrupt:
            logger.info("消费者收到中断信号")
        finally:
            consumer.close()
            logger.info("消费者已关闭, 共处理 %s 条消息", count)

    @classmethod
    def from_env(cls):
        """从环境变量创建"""
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        )
