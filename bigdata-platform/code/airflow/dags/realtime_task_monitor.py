"""
实时任务监控 DAG
定时检查 Flink 实时任务运行状态，异常时自动告警
调度时间: 每 10 分钟
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

import os


def check_flink_jobs(**context):
    """检查 Flink 实时任务状态"""
    import requests

    flink_rest_url = os.getenv("FLINK_REST_URL", "http://flink-jobmanager:8081")
    expected_jobs = [
        "实时用户行为分析 (5分钟窗口)",
    ]

    print(f"🔍 检查 Flink 任务状态: {flink_rest_url}")

    try:
        resp = requests.get(f"{flink_rest_url}/jobs/overview", timeout=10)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])

        running_jobs = {j["name"]: j for j in jobs if j["state"] == "RUNNING"}
        failed_jobs = [j for j in jobs if j["state"] in ("FAILED", "CANCELED")]

        print(f"运行中任务: {len(running_jobs)}")
        print(f"失败任务: {len(failed_jobs)}")

        # 检查预期任务是否在运行
        missing_jobs = []
        for expected in expected_jobs:
            if expected not in running_jobs:
                missing_jobs.append(expected)
                print(f"❌ 任务未运行: {expected}")
            else:
                job = running_jobs[expected]
                print(f"✅ {expected} 运行中 (uptime={job.get('duration', 'N/A')}ms)")

        if missing_jobs or failed_jobs:
            context["ti"].xcom_push(key="alert_needed", value=True)
            context["ti"].xcom_push(key="missing_jobs", value=missing_jobs)
            context["ti"].xcom_push(key="failed_jobs", value=[j["name"] for j in failed_jobs])
            raise Exception(f"Flink 任务异常: 缺失={missing_jobs}, 失败={[j['name'] for j in failed_jobs]}")

        print("✅ 所有 Flink 实时任务正常")
        return True

    except requests.exceptions.ConnectionError:
        raise Exception(f"无法连接 Flink REST API: {flink_rest_url}")
    except requests.exceptions.Timeout:
        raise Exception(f"Flink REST API 超时: {flink_rest_url}")


def check_kafka_lag(**context):
    """检查 Kafka 消费延迟"""
    import subprocess

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    group_id = "flink-user-behavior-group"
    max_lag_threshold = int(os.getenv("KAFKA_MAX_LAG", "10000"))

    print(f"🔍 检查 Kafka 消费延迟: group={group_id}")

    try:
        cmd = [
            "kafka-consumer-groups.sh",
            "--bootstrap-server", bootstrap_servers,
            "--group", group_id,
            "--describe",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            print(f"⚠️ kafka-consumer-groups.sh 执行失败: {result.stderr}")
            return True  # 命令不可用时不阻塞

        total_lag = 0
        for line in result.stdout.strip().split("\n")[1:]:  # 跳过 header
            parts = line.split()
            if len(parts) >= 6:
                try:
                    lag = int(parts[5])
                    total_lag += lag
                except (ValueError, IndexError):
                    pass

        print(f"总消费延迟: {total_lag} 条")

        if total_lag > max_lag_threshold:
            raise Exception(f"Kafka 消费延迟过高: {total_lag} > {max_lag_threshold}")

        print(f"✅ Kafka 消费延迟正常 ({total_lag} < {max_lag_threshold})")
        return True

    except FileNotFoundError:
        print("⚠️ kafka-consumer-groups.sh 不在 PATH 中，跳过 Kafka 延迟检查")
        return True


# DAG 配置
default_args = {
    'owner': 'bigdata_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['bigdata@company.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=5),
}

with DAG(
    'realtime_task_monitor',
    default_args=default_args,
    description='实时任务监控: 检查 Flink 任务状态 + Kafka 消费延迟',
    schedule_interval='*/10 * * * *',  # 每 10 分钟
    catchup=False,
    tags=['实时', '监控', 'Flink', 'Kafka'],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id='start')

    check_flink = PythonOperator(
        task_id='check_flink_jobs',
        python_callable=check_flink_jobs,
        provide_context=True,
    )

    check_kafka = PythonOperator(
        task_id='check_kafka_lag',
        python_callable=check_kafka_lag,
        provide_context=True,
    )

    alert_email = EmailOperator(
        task_id='alert_notification',
        to='bigdata@company.com',
        subject='🚨 实时任务异常告警 - {{ ds }}',
        html_content="""
            <h3>🚨 实时任务监控告警</h3>
            <p>时间: {{ ts }}</p>
            <p>请立即登录检查:</p>
            <ul>
                <li><a href="http://flink-jobmanager:8081">Flink Dashboard</a></li>
                <li><a href="http://airflow:8080">Airflow UI</a></li>
            </ul>
        """,
        trigger_rule='one_failed',
    )

    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    start >> [check_flink, check_kafka]
    check_flink >> alert_email >> end
    check_kafka >> alert_email >> end
    check_flink >> end
    check_kafka >> end
