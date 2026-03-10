"""
数据仓库每日 ETL 调度 DAG
执行链路: MySQL同步 → ODS → DWD → DWS → ADS → 数据质量 → 通知
调度时间: 每天凌晨 2:00
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago


def quality_check(**context):
    """数据质量检查（调用 quality/ 模块）"""
    import subprocess
    import sys

    date = context.get("ds")
    print(f"🧪 对日期 {date} 进行数据质量检查...")

    try:
        result = subprocess.run(
            [sys.executable, "/opt/bigdata/quality/validate_order_data.py", date],
            capture_output=True, text=True, timeout=300
        )

        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise Exception(f"数据质量检查失败: 返回码 {result.returncode}")

        print(f"✅ 数据质量检查通过")
        return True

    except subprocess.TimeoutExpired:
        raise Exception("数据质量检查超时 (5min)")
    except Exception as e:
        print(f"❌ 数据质量检查失败: {e}")
        raise


# DAG 配置
default_args = {
    'owner': 'bigdata_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['bigdata@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=3),
}

with DAG(
    'daily_warehouse_etl',
    default_args=default_args,
    description='每日数仓分层 ETL: MySQL同步 → ODS → DWD → DWS → ADS → 质量检查 → 通知',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['数仓', 'ETL', '每日任务'],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id='start')
    sync_done = EmptyOperator(task_id='sync_done')
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    # ====================== 1. 数据同步 (并行) ======================
    sync_mysql_to_ods = BashOperator(
        task_id='sync_mysql_to_ods',
        bash_command='cd /opt/bigdata/sync && python sync_mysql_to_ods.py {{ ds }}',
        retries=1,
    )

    sync_kafka_to_ods = BashOperator(
        task_id='sync_kafka_to_ods',
        bash_command='cd /opt/bigdata/sync && python sync_kafka_to_ods.py {{ ds }}',
        retries=1,
    )

    # ====================== 2. ODS → DWD ======================
    ods_to_dwd = BashOperator(
        task_id='etl_ods_to_dwd',
        bash_command='cd /opt/bigdata/batch/spark-etl && spark-submit --master yarn --deploy-mode client etl_ods_to_dwd.py {{ ds }}',
    )

    # ====================== 3. DWD → DWS ======================
    dwd_to_dws = BashOperator(
        task_id='etl_dwd_to_dws',
        bash_command='cd /opt/bigdata/batch/spark-etl && spark-submit --master yarn --deploy-mode client etl_dwd_to_dws.py {{ ds }}',
    )

    # ====================== 4. DWS → ADS ======================
    dws_to_ads = BashOperator(
        task_id='etl_dws_to_ads',
        bash_command='cd /opt/bigdata/batch/spark-etl && spark-submit --master yarn --deploy-mode client etl_dws_to_ads.py {{ ds }}',
    )

    # ====================== 5. 数据质量检查 ======================
    quality_check_task = PythonOperator(
        task_id='quality_check',
        python_callable=quality_check,
        provide_context=True,
        retries=1,
    )

    # ====================== 6. 结果通知 ======================
    success_email = EmailOperator(
        task_id='success_notification',
        to='bigdata@company.com',
        subject='✅ 每日数仓 ETL 完成 - {{ ds }}',
        html_content="""
            <h3>每日数仓 ETL 执行结果</h3>
            <p>任务日期: {{ ds }}</p>
            <p>状态: <strong style="color:green">全部成功 ✅</strong></p>
            <p>链路: MySQL同步 → ODS → DWD → DWS → ADS → 质量检查</p>
            <p>查看: <a href="http://airflow:8080">Airflow</a> |
                     <a href="http://spark-master:8080">Spark</a> |
                     <a href="http://superset:8088">Superset</a></p>
        """,
        trigger_rule='all_success',
    )

    failure_email = EmailOperator(
        task_id='failure_notification',
        to='bigdata@company.com',
        subject='❌ 每日数仓 ETL 失败 - {{ ds }}',
        html_content="""
            <h3>每日数仓 ETL 执行结果</h3>
            <p>任务日期: {{ ds }}</p>
            <p>状态: <strong style="color:red">执行失败 ❌</strong></p>
            <p>请登录 <a href="http://airflow:8080">Airflow UI</a> 查看错误详情。</p>
        """,
        trigger_rule='one_failed',
    )

    # ====================== DAG 依赖 ======================
    # sync 并行 → DWD → DWS → ADS → 质量检查 → 通知
    start >> [sync_mysql_to_ods, sync_kafka_to_ods] >> sync_done
    sync_done >> ods_to_dwd >> dwd_to_dws >> dws_to_ads >> quality_check_task
    quality_check_task >> success_email >> end
    quality_check_task >> failure_email >> end
