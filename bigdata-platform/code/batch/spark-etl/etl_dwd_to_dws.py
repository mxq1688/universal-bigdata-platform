#!/usr/bin/env python3
"""
数据仓库分层 ETL - DWD → DWS
从 DWD 明细层聚合到 DWS 汇总层
"""
import os, sys, datetime, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def get_spark_session(job_name):
    spark = SparkSession.builder.appName(f"数仓_{job_name}") \
        .config("spark.sql.shuffle.partitions", "8") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def main(date_str=None):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if not date_str: date_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 处理日期: {date_str}")

    spark = get_spark_session("DWD_TO_DWS")
    try:
        # ====================== 1. 读取 DWD 数据 ======================
        logging.info("读取 DWD 层数据...")
        order_detail = spark.read.table("dwd.dwd_trade_order_detail") \
            .filter(col("dt") == date_str)
        behavior_detail = spark.read.table("dwd.dwd_user_behavior_detail") \
            .filter(col("dt") == date_str)

        # ====================== 2. 门店日汇总 ======================
        logging.info("计算门店日汇总...")
        dws_store = order_detail \
            .groupBy("city") \
            .agg(
                countDistinct("user_id").alias("user_cnt"),
                countDistinct("order_id").alias("order_cnt"),
                sum("order_amount").alias("pay_amount"),
                avg("order_amount").alias("avg_order_price"),
                sum("is_refund").alias("refund_cnt"),
                max("order_time").alias("last_order_time")
            ) \
            .withColumn("refund_rate", round("refund_cnt" / "order_cnt", 4)) \
            .withColumn("gross_margin", round(("pay_amount" * 0.6), 2)) \
            .withColumn("dt", lit(date_str)) \
            .withColumn("load_time", current_timestamp())

        # ====================== 3. 用户行为汇总 ======================
        logging.info("计算用户行为汇总...")
        dws_behavior = behavior_detail \
            .groupBy("category", "action") \
            .agg(
                count("*").alias("action_cnt"),
                countDistinct("user_id").alias("user_cnt"),
                countDistinct("session_id").alias("session_cnt"),
                avg("action_weight").alias("avg_value")
            ) \
            .withColumn("dt", lit(date_str))

        # ====================== 4. 用户价值分群 ======================
        logging.info("用户价值分群...")
        user_value = order_detail \
            .groupBy("user_id") \
            .agg(
                sum("order_amount").alias("total_pay"),
                count("order_id").alias("order_freq"),
                max("order_time").alias("last_order_time")
            ) \
            .withColumn("user_value", 
                when((col("total_pay") > 5000) & (col("order_freq") > 20), "高价值用户")
                .when((col("total_pay") > 1000) & (col("order_freq") > 5), "中价值用户")
                .otherwise("低价值用户")
            ) \
            .withColumn("dt", lit(date_str))

        # ====================== 5. 写 DWS 层 ======================
        logging.info("写入 DWS 层...")
        dws_store.write.mode("append").saveAsTable("dws.dws_trade_store_1d")
        dws_behavior.write.mode("append").saveAsTable("dws.dws_user_behavior_1d")
        user_value.write.mode("append").saveAsTable("dws.dws_user_value_1d")

        # ====================== 6. 质量检查 ======================
        store_count = dws_store.count()
        behavior_count = dws_behavior.count()
        user_value_count = user_value.count()

        logging.info(f"✅ 处理完成:")
        logging.info(f"  门店汇总: {store_count} 条")
        logging.info(f"  行为汇总: {behavior_count} 条")
        logging.info(f"  用户价值: {user_value_count} 条")

        # 抽样检查
        logging.info("📊 结果抽样:")
        dws_store.select("city", "user_cnt", "order_cnt", "pay_amount").show(5, False)

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
