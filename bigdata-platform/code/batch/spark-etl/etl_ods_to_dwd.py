#!/usr/bin/env python3
"""
数据仓库分层 ETL - ODS → DWD
从 ODS 层清洗加工到 DWD 明细层
"""
import os, sys, datetime, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_spark_session(job_name):
    spark = SparkSession.builder.appName(f"数仓_{job_name}") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def main(date_str=None):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if not date_str: date_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 处理日期: {date_str}")

    spark = get_spark_session("ODS_TO_DWD")
    try:
        # ====================== 1. 读取 ODS 数据 ======================
        logging.info("读取 ODS 层数据...")
        ods_order = spark.read.table(f"ods.ods_mysql_user_order")
        ods_behavior = spark.read.table(f"ods.ods_kafka_user_behavior")

        # ====================== 2. 订单数据清洗 ======================
        logging.info("清洗订单数据...")
        dwd_order = ods_order \
            .filter(col("id").isNotNull() & (col("total_amount") > 0)) \
            .withColumn("order_time", to_timestamp("create_time", "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("dt", substring("create_time", 1, 10)) \
            .withColumn("is_refund", when(col("status") == "refunded", 1).otherwise(0)) \
            .dropDuplicates(["id"]) \
            .select(
                col("id").alias("order_id"),
                col("user_id"),
                col("product_id"),
                col("amount").alias("order_amount"),
                col("total_amount"),
                col("quantity"),
                col("status").alias("order_status"),
                col("pay_method"),
                col("city"),
                col("is_refund"),
                col("order_time"),
                col("dt")
            )

        # ====================== 3. 用户行为清洗 ======================
        logging.info("清洗用户行为数据...")
        valid_actions = (col("action").isin(["view","cart","order","pay"]))
        dwd_behavior = ods_behavior \
            .filter(valid_actions & col("user_id").isNotNull() & col("product_id").isNotNull()) \
            .withColumn("behavior_time", to_timestamp("timestamp")) \
            .withColumn("dt", substring("timestamp", 1, 10)) \
            .withColumn("action_weight",
                when(col("action") == "pay", 10)
                .when(col("action") == "order", 8)
                .when(col("action") == "cart", 3)
                .otherwise(1)
            ) \
            .select(
                col("user_id"),
                col("product_id"),
                col("action"),
                col("action_weight"),
                col("category"),
                col("device"),
                col("channel"),
                col("city"),
                col("session_id"),
                col("behavior_time"),
                col("dt")
            )

        # ====================== 4. 写 DWD 层 ======================
        logging.info("写入 DWD 层...")
        dwd_order.write.mode("overwrite").partitionBy("dt") \
            .saveAsTable("dwd.dwd_trade_order_detail")

        dwd_behavior.write.mode("overwrite").partitionBy("dt") \
            .saveAsTable("dwd.dwd_user_behavior_detail")

        # ====================== 5. 数据质量检查 ======================
        logging.info("数据质量检查...")
        order_count = dwd_order.count()
        behavior_count = dwd_behavior.count()

        if order_count == 0 or behavior_count == 0:
            raise Exception(f"❌ 数据量异常: 订单={order_count}, 行为={behavior_count}")

        logging.info(f"✅ 处理完成: 订单={order_count}条, 用户行为={behavior_count}条")

    except Exception as e:
        logging.error(f"❌ ETL 失败: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    try:
        date = sys.argv[1] if len(sys.argv) > 1 else None
        main(date_str=date)
    except Exception as e:
        print(f"运行失败: {e}")
        sys.exit(1)
