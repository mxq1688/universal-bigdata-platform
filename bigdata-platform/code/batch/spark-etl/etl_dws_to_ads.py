#!/usr/bin/env python3
"""
数据仓库分层 ETL - DWS → ADS
从 DWS 汇总层产出 ADS 应用报表层
ADS 层直接供业务看板、BI 报表、Agent 查询使用
"""
import sys
import datetime
import logging
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

    if not date_str:
        date_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 处理日期: {date_str}")

    spark = get_spark_session("DWS_TO_ADS")

    try:
        # ====================== 1. 读取 DWS 层数据 ======================
        logging.info("读取 DWS 层数据...")

        dws_store = spark.read.table("dws.dws_trade_store_1d") \
            .filter(col("dt") == date_str)
        dws_behavior = spark.read.table("dws.dws_user_behavior_1d") \
            .filter(col("dt") == date_str)
        dws_user_value = spark.read.table("dws.dws_user_value_1d") \
            .filter(col("dt") == date_str)

        # ====================== 2. ADS: 每日经营概览 ======================
        logging.info("计算每日经营概览...")

        ads_daily_overview = dws_store.agg(
            sum("user_cnt").alias("total_users"),
            sum("order_cnt").alias("total_orders"),
            sum("pay_amount").alias("total_gmv"),
            avg("avg_order_price").alias("avg_order_price"),
            sum("refund_cnt").alias("total_refunds"),
            avg("refund_rate").alias("avg_refund_rate"),
        ).withColumn("dt", lit(date_str)) \
         .withColumn("load_time", current_timestamp())

        # ====================== 3. ADS: 城市维度排行榜 ======================
        logging.info("计算城市维度排行...")

        ads_city_rank = dws_store.select(
            col("city"),
            col("user_cnt"),
            col("order_cnt"),
            col("pay_amount"),
            col("avg_order_price"),
            col("refund_rate"),
            rank().over(
                Window.orderBy(col("pay_amount").desc())
            ).alias("gmv_rank"),
            rank().over(
                Window.orderBy(col("user_cnt").desc())
            ).alias("user_rank"),
            lit(date_str).alias("dt"),
        )

        # ====================== 4. ADS: 用户价值分布 ======================
        logging.info("计算用户价值分布...")

        ads_user_value_dist = dws_user_value.groupBy("user_value").agg(
            count("user_id").alias("user_cnt"),
            sum("total_pay").alias("total_pay"),
            avg("total_pay").alias("avg_pay"),
            avg("order_freq").alias("avg_order_freq"),
        ).withColumn("user_pct",
            round(col("user_cnt") / sum("user_cnt").over(Window.partitionBy()), 4)
        ).withColumn("pay_pct",
            round(col("total_pay") / sum("total_pay").over(Window.partitionBy()), 4)
        ).withColumn("dt", lit(date_str))

        # ====================== 5. ADS: 品类行为漏斗 ======================
        logging.info("计算品类行为漏斗...")

        ads_category_funnel = dws_behavior.groupBy("category").pivot(
            "action", ["view", "cart", "order", "pay"]
        ).agg(
            sum("action_cnt")
        ).na.fill(0) \
         .withColumn("view_to_cart_rate",
            when(col("view") > 0, round(col("cart") / col("view"), 4)).otherwise(0)) \
         .withColumn("cart_to_order_rate",
            when(col("cart") > 0, round(col("`order`") / col("cart"), 4)).otherwise(0)) \
         .withColumn("order_to_pay_rate",
            when(col("`order`") > 0, round(col("pay") / col("`order`"), 4)).otherwise(0)) \
         .withColumn("overall_rate",
            when(col("view") > 0, round(col("pay") / col("view"), 4)).otherwise(0)) \
         .withColumn("dt", lit(date_str))

        # ====================== 6. ADS: DAU / 新增用户 / 留存 ======================
        logging.info("计算 DAU 指标...")

        dwd_behavior = spark.read.table("dwd.dwd_user_behavior_detail") \
            .filter(col("dt") == date_str)

        ads_dau = dwd_behavior.agg(
            countDistinct("user_id").alias("dau"),
            countDistinct("session_id").alias("session_cnt"),
            count("*").alias("total_events"),
        ).withColumn("avg_events_per_user",
            round(col("total_events") / col("dau"), 2)
        ).withColumn("dt", lit(date_str)) \
         .withColumn("load_time", current_timestamp())

        # ====================== 7. 写入 ADS 层 ======================
        logging.info("写入 ADS 层...")

        ads_daily_overview.write.mode("overwrite") \
            .saveAsTable("ads.ads_daily_overview")

        ads_city_rank.write.mode("overwrite") \
            .partitionBy("dt") \
            .saveAsTable("ads.ads_city_rank")

        ads_user_value_dist.write.mode("overwrite") \
            .partitionBy("dt") \
            .saveAsTable("ads.ads_user_value_distribution")

        ads_category_funnel.write.mode("overwrite") \
            .partitionBy("dt") \
            .saveAsTable("ads.ads_category_funnel")

        ads_dau.write.mode("overwrite") \
            .saveAsTable("ads.ads_dau")

        # ====================== 8. 质量检查 ======================
        counts = {
            "每日经营概览": ads_daily_overview.count(),
            "城市排行": ads_city_rank.count(),
            "用户价值分布": ads_user_value_dist.count(),
            "品类漏斗": ads_category_funnel.count(),
            "DAU": ads_dau.count(),
        }

        logging.info("✅ ADS 层处理完成:")
        for name, cnt in counts.items():
            logging.info("  %s: %s 条", name, cnt)

        if any(cnt == 0 for cnt in counts.values()):
            logging.warning("⚠️ 部分 ADS 表数据为空，请检查 DWS 层数据")

    except Exception as e:
        logging.error("❌ ADS ETL 失败: %s", e)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        date = sys.argv[1] if len(sys.argv) > 1 else None
        main(date_str=date)
    except Exception as e:
        print(f"运行失败: {e}")
        sys.exit(1)
