#!/usr/bin/env python3
"""
Kafka → HDFS (ODS 层) 同步脚本
将 Kafka 中的用户行为日志消费后写入 HDFS ODS 层
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('SyncKafkaToODS')


def consume_kafka_to_hdfs(topic, date_str, hdfs_path, bootstrap_servers, group_id,
                          max_records=None, timeout_sec=300):
    """
    从 Kafka 消费消息并写入 HDFS

    实际生产环境推荐使用:
    - Flink Kafka → HDFS Sink（流式写入）
    - DataX kafkareader → hdfswriter
    - Flume Kafka Source → HDFS Sink

    本脚本用于轻量级场景 / 补数
    """
    try:
        from kafka import KafkaConsumer
    except ImportError:
        logger.error("kafka-python 未安装: pip install kafka-python")
        return False

    try:
        from hdfs import InsecureClient
        hdfs_client = InsecureClient(
            os.getenv("HDFS_WEBHDFS_URL", "http://localhost:9870"),
            user=os.getenv("HDFS_USER", "hadoop")
        )
        use_hdfs = True
    except ImportError:
        logger.warning("hdfs 库未安装，将写入本地文件")
        use_hdfs = False

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_sec * 1000,
        value_deserializer=lambda v: v.decode('utf-8') if v else None,
        max_poll_records=500,
    )

    logger.info("🚀 开始消费 Kafka: topic=%s, group=%s, date=%s", topic, group_id, date_str)

    records = []
    count = 0
    start_time = datetime.now()

    try:
        for msg in consumer:
            if msg.value is None:
                continue

            try:
                data = json.loads(msg.value)

                # 过滤日期（只要当天的数据）
                event_time = data.get("timestamp") or data.get("event_time") or data.get("create_time", "")
                if date_str not in str(event_time)[:10]:
                    continue

                records.append(msg.value)
                count += 1

                if count % 10000 == 0:
                    logger.info("  已消费 %s 条...", count)

                if max_records and count >= max_records:
                    logger.info("达到最大消费数 %s", max_records)
                    break

            except json.JSONDecodeError:
                records.append(msg.value)
                count += 1

        consumer.commit()

    except KeyboardInterrupt:
        logger.info("收到中断信号")
    finally:
        consumer.close()

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("消费完成: %s 条, 耗时 %.1f 秒", count, elapsed)

    if not records:
        logger.warning("⚠️ 没有消费到数据")
        return True

    # 写入文件
    output_content = "\n".join(records) + "\n"
    output_filename = f"{topic}_{date_str.replace('-', '')}.jsonl"

    if use_hdfs:
        full_path = f"{hdfs_path}/{output_filename}"
        try:
            hdfs_client.write(full_path, output_content.encode('utf-8'), overwrite=True)
            logger.info("✅ 写入 HDFS: %s (%s 条)", full_path, count)
        except Exception as e:
            logger.error("❌ HDFS 写入失败: %s", e)
            return False
    else:
        local_dir = f"/tmp/ods_kafka/{topic}/dt={date_str}"
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, output_filename)
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write(output_content)
        logger.info("✅ 写入本地: %s (%s 条)", local_path, count)

    return True


def create_hive_partition(table_name, date_str):
    """创建 Hive ODS 表分区"""
    import subprocess
    hive_sql = f"ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION(dt='{date_str}');"
    logger.info("创建 Hive 分区: %s/dt=%s", table_name, date_str)

    try:
        result = subprocess.run(f'hive -e "{hive_sql}"', shell=True,
                                capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            logger.info("✅ 分区创建成功")
        else:
            logger.warning("⚠️ 分区创建失败 (可能已存在)")
    except Exception as e:
        logger.warning("⚠️ Hive 分区创建异常: %s", e)


def sync_kafka_to_ods(date_str=None):
    """同步所有 Kafka topic 到 HDFS ODS 层"""
    if not date_str:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    hdfs_base = os.getenv("HDFS_ODS_BASE", "/user/hive/warehouse/ods_kafka.db")

    topics = [
        {
            'topic': 'user_behavior',
            'hive_table': 'ods_kafka.ods_user_behavior',
            'hdfs_path': f'{hdfs_base}/ods_user_behavior/dt={date_str}',
            'group_id': 'sync-user-behavior-ods',
        },
        {
            'topic': 'order_events',
            'hive_table': 'ods_kafka.ods_order_events',
            'hdfs_path': f'{hdfs_base}/ods_order_events/dt={date_str}',
            'group_id': 'sync-order-events-ods',
        },
    ]

    logger.info("=" * 60)
    logger.info("开始 Kafka → HDFS ODS 同步, 日期: %s", date_str)
    logger.info("=" * 60)

    success_count = 0
    fail_count = 0

    for t in topics:
        ok = consume_kafka_to_hdfs(
            topic=t['topic'],
            date_str=date_str,
            hdfs_path=t['hdfs_path'],
            bootstrap_servers=bootstrap_servers,
            group_id=t['group_id'],
        )

        if ok:
            create_hive_partition(t['hive_table'], date_str)
            success_count += 1
        else:
            fail_count += 1

    logger.info("=" * 60)
    logger.info("Kafka 同步完成: 成功=%s, 失败=%s", success_count, fail_count)
    logger.info("=" * 60)

    return fail_count == 0


def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    success = sync_kafka_to_ods(date_str=date_str)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
