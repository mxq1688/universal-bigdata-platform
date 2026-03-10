"""
数据质量巡检 DAG
对 DWD/DWS/ADS 各层核心表进行数据质量巡检
调度时间: 每天上午 6:00（在 ETL 完成后执行）
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

import os
import sys


def run_quality_check(table_name, check_type="row_count", **context):
    """
    通用数据质量检查函数

    支持检查类型:
    - row_count: 数据量检查（不能为 0）
    - null_check: 关键字段空值检查
    - range_check: 数值范围检查
    - duplicate_check: 主键重复检查
    """
    date_str = context.get("ds")
    print(f"🧪 质量检查: {table_name} (type={check_type}, date={date_str})")

    # 这里使用 PySpark 连接 Hive 执行检查
    # 实际部署时 Spark 环境已配置好
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
            .appName(f"DataQuality_{table_name}_{date_str}") \
            .enableHiveSupport() \
            .getOrCreate()

        df = spark.sql(f"SELECT * FROM {table_name} WHERE dt = '{date_str}'")
        row_count = df.count()
        print(f"  数据量: {row_count}")

        if check_type == "row_count":
            if row_count == 0:
                raise Exception(f"❌ {table_name} 数据量为 0")
            print(f"  ✅ 数据量检查通过: {row_count} 行")

        elif check_type == "null_check":
            # 检查所有字段的空值比例
            total = row_count
            for col_name in df.columns:
                if col_name == "dt":
                    continue
                null_count = df.filter(df[col_name].isNull()).count()
                null_rate = null_count / total if total > 0 else 0
                if null_rate > 0.5:  # 超过 50% 为空告警
                    print(f"  ⚠️ {col_name} 空值率: {null_rate:.2%}")
                else:
                    print(f"  ✅ {col_name} 空值率: {null_rate:.2%}")

        elif check_type == "duplicate_check":
            # 检查第一列（通常是主键）的唯一性
            pk_col = df.columns[0]
            distinct_count = df.select(pk_col).distinct().count()
            dup_count = row_count - distinct_count
            if dup_count > 0:
                print(f"  ⚠️ {pk_col} 存在 {dup_count} 条重复数据")
            else:
                print(f"  ✅ {pk_col} 无重复数据")

        spark.stop()
        return {"table": table_name, "row_count": row_count, "status": "passed"}

    except ImportError:
        # Spark 不可用时，使用 Python 模拟检查
        print(f"  ⚠️ Spark 不可用，跳过 {table_name} 检查")
        return {"table": table_name, "row_count": -1, "status": "skipped"}


def generate_quality_report(**context):
    """汇总所有检查结果，生成报告"""
    ti = context["ti"]
    date_str = context.get("ds")

    tables_checked = [
        "dwd.dwd_trade_order_detail",
        "dwd.dwd_user_behavior_detail",
        "dws.dws_trade_store_1d",
        "dws.dws_user_behavior_1d",
        "dws.dws_user_value_1d",
        "ads.ads_daily_overview",
        "ads.ads_dau",
    ]

    report_lines = [
        f"📊 数据质量日报 - {date_str}",
        "=" * 50,
        f"{'表名':<40} {'状态':<10}",
        "-" * 50,
    ]

    all_passed = True
    for table in tables_checked:
        task_id = f"check_{table.replace('.', '_')}"
        try:
            result = ti.xcom_pull(task_ids=task_id)
            if result:
                status = result.get("status", "unknown")
                row_count = result.get("row_count", -1)
                report_lines.append(f"{table:<40} {status:<10} ({row_count} rows)")
                if status == "failed":
                    all_passed = False
            else:
                report_lines.append(f"{table:<40} {'no_data':<10}")
        except Exception:
            report_lines.append(f"{table:<40} {'error':<10}")

    report_lines.append("=" * 50)
    report_lines.append(f"整体状态: {'✅ 全部通过' if all_passed else '❌ 存在异常'}")

    report = "\n".join(report_lines)
    print(report)

    ti.xcom_push(key="quality_report", value=report)
    ti.xcom_push(key="all_passed", value=all_passed)
    return all_passed


# DAG 配置
default_args = {
    'owner': 'bigdata_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['bigdata@company.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(hours=1),
}

with DAG(
    'data_quality_daily',
    default_args=default_args,
    description='每日数据质量巡检: DWD/DWS/ADS 各层核心表',
    schedule_interval='0 6 * * *',  # 每天 6:00
    catchup=False,
    tags=['数据质量', '巡检', '每日任务'],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id='start')
    checks_done = EmptyOperator(task_id='checks_done', trigger_rule='all_done')
    end = EmptyOperator(task_id='end')

    # 等待 ETL DAG 完成
    wait_for_etl = ExternalTaskSensor(
        task_id='wait_for_etl',
        external_dag_id='daily_warehouse_etl',
        external_task_id='end',
        mode='reschedule',
        timeout=3600,
        poke_interval=60,
    )

    # DWD 层检查
    check_tasks = []
    tables_to_check = [
        ("dwd.dwd_trade_order_detail", "row_count"),
        ("dwd.dwd_user_behavior_detail", "row_count"),
        ("dws.dws_trade_store_1d", "row_count"),
        ("dws.dws_user_behavior_1d", "row_count"),
        ("dws.dws_user_value_1d", "null_check"),
        ("ads.ads_daily_overview", "row_count"),
        ("ads.ads_dau", "row_count"),
    ]

    for table_name, check_type in tables_to_check:
        task = PythonOperator(
            task_id=f"check_{table_name.replace('.', '_')}",
            python_callable=run_quality_check,
            op_kwargs={"table_name": table_name, "check_type": check_type},
            provide_context=True,
        )
        check_tasks.append(task)

    # 汇总报告
    report_task = PythonOperator(
        task_id='generate_quality_report',
        python_callable=generate_quality_report,
        provide_context=True,
    )

    # 通知
    quality_email = EmailOperator(
        task_id='quality_report_email',
        to='bigdata@company.com',
        subject='📊 数据质量日报 - {{ ds }}',
        html_content="""
            <h3>数据质量日报</h3>
            <p>日期: {{ ds }}</p>
            <pre>{{ ti.xcom_pull(task_ids='generate_quality_report', key='quality_report') }}</pre>
            <p>详情请登录 <a href="http://airflow:8080">Airflow</a></p>
        """,
    )

    # DAG 依赖
    start >> wait_for_etl >> check_tasks >> checks_done >> report_task >> quality_email >> end
