#!/usr/bin/env python3
"""
大数据平台 监控告警工具
支持: Flink 任务监控 / Airflow DAG 状态 / 数仓延迟检查 / 磁盘空间检查

用法:
    python monitor.py flink          # 检查 Flink 作业状态
    python monitor.py airflow        # 检查 Airflow DAG 状态
    python monitor.py warehouse      # 检查数仓数据延迟
    python monitor.py disk           # 检查磁盘/HDFS 使用率
    python monitor.py all            # 全部检查
"""
import os
import sys
import json
import logging
import argparse
import subprocess
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('Monitor')


# ====================== 告警通道 ======================

def send_alert(title: str, content: str, level: str = "warning"):
    """发送告警 (支持 Webhook / Email)"""
    webhook_url = os.getenv("ALERT_WEBHOOK_URL")
    if webhook_url:
        _send_webhook(webhook_url, title, content, level)

    email = os.getenv("ALERT_EMAIL")
    if email:
        _send_email(email, title, content)

    logger.info(f"告警已发送: [{level}] {title}")


def _send_webhook(url: str, title: str, content: str, level: str):
    """Webhook 告警 (企业微信/钉钉/飞书通用)"""
    colors = {"critical": "#FF0000", "warning": "#FF8C00", "info": "#1E90FF"}
    color = colors.get(level, "#333333")

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": title,
            "text": f"### <font color='{color}'>{title}</font>\n"
                    f"> **时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"> **级别**: {level.upper()}\n\n"
                    f"{content}"
        }
    }

    try:
        data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        req = Request(url, data=data, headers={'Content-Type': 'application/json'})
        with urlopen(req, timeout=10) as resp:
            logger.debug(f"Webhook 响应: {resp.read().decode()}")
    except Exception as e:
        logger.error(f"Webhook 发送失败: {e}")


def _send_email(to: str, title: str, content: str):
    """邮件告警 (简易 sendmail)"""
    try:
        import smtplib
        from email.mime.text import MIMEText

        smtp_host = os.getenv("SMTP_HOST", "localhost")
        smtp_port = int(os.getenv("SMTP_PORT", "25"))
        from_addr = os.getenv("SMTP_FROM", "bigdata-monitor@company.com")

        msg = MIMEText(content, "plain", "utf-8")
        msg["Subject"] = title
        msg["From"] = from_addr
        msg["To"] = to

        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.send_message(msg)

    except Exception as e:
        logger.warning(f"邮件发送失败 (非致命): {e}")


# ====================== Flink 监控 ======================

def check_flink():
    """检查 Flink JobManager 中所有作业的运行状态"""
    flink_url = os.getenv("FLINK_JOBMANAGER", "http://flink-jobmanager:8081")
    logger.info(f"🔍 检查 Flink 作业状态 ({flink_url})...")

    try:
        with urlopen(f"{flink_url}/jobs/overview", timeout=10) as resp:
            data = json.loads(resp.read())
    except URLError as e:
        send_alert("🔴 Flink JobManager 不可达", f"无法连接 {flink_url}: {e}", "critical")
        return False

    jobs = data.get("jobs", [])
    if not jobs:
        logger.warning("⚠️ 没有运行中的 Flink 作业")
        return True

    all_ok = True
    for job in jobs:
        jid = job["jid"]
        name = job.get("name", "未知")
        state = job.get("state", "UNKNOWN")

        if state == "RUNNING":
            logger.info(f"  ✅ {name} ({jid[:8]}): RUNNING")
        elif state in ("FAILING", "FAILED", "RESTARTING"):
            logger.error(f"  ❌ {name} ({jid[:8]}): {state}")
            send_alert(
                f"🔴 Flink 作业异常: {name}",
                f"- 作业 ID: {jid}\n- 状态: {state}\n- 请检查 {flink_url}/#/job/{jid}",
                "critical"
            )
            all_ok = False
        elif state == "CANCELED":
            logger.warning(f"  ⚠️ {name} ({jid[:8]}): CANCELED")
        else:
            logger.info(f"  ℹ️ {name} ({jid[:8]}): {state}")

    return all_ok


# ====================== Airflow 监控 ======================

def check_airflow():
    """检查 Airflow DAG 最近执行状态"""
    airflow_url = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
    logger.info(f"🔍 检查 Airflow DAG 状态 ({airflow_url})...")

    critical_dags = ["daily_warehouse_etl", "data_quality_daily"]

    all_ok = True
    for dag_id in critical_dags:
        try:
            url = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns?order_by=-execution_date&limit=1"
            req = Request(url)
            req.add_header("Authorization", f"Basic {os.getenv('AIRFLOW_AUTH', '')}")
            req.add_header("Content-Type", "application/json")

            with urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())

            runs = data.get("dag_runs", [])
            if not runs:
                logger.warning(f"  ⚠️ {dag_id}: 无执行记录")
                continue

            latest = runs[0]
            state = latest.get("state", "unknown")
            exec_date = latest.get("execution_date", "")

            if state == "success":
                logger.info(f"  ✅ {dag_id}: {state} ({exec_date[:10]})")
            elif state == "failed":
                logger.error(f"  ❌ {dag_id}: {state} ({exec_date[:10]})")
                send_alert(
                    f"🔴 Airflow DAG 失败: {dag_id}",
                    f"- 执行日期: {exec_date}\n- 状态: {state}\n- 查看: {airflow_url}/dags/{dag_id}",
                    "critical"
                )
                all_ok = False
            elif state == "running":
                logger.info(f"  🔄 {dag_id}: 运行中 ({exec_date[:10]})")
            else:
                logger.warning(f"  ⚠️ {dag_id}: {state} ({exec_date[:10]})")

        except URLError as e:
            logger.error(f"  ❌ {dag_id}: 无法连接 Airflow API: {e}")
            all_ok = False

    return all_ok


# ====================== 数仓延迟检查 ======================

def check_warehouse():
    """检查数仓各层数据是否按时到达"""
    logger.info("🔍 检查数仓数据延迟...")

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")

    checks = [
        ("dwd.dwd_trade_order_detail", "dt", yesterday, "DWD 订单明细"),
        ("dwd.dwd_user_behavior_detail", "dt", yesterday, "DWD 用户行为"),
        ("dws.dws_trade_store_1d", "dt", yesterday, "DWS 门店汇总"),
        ("dws.dws_user_behavior_1d", "dt", yesterday, "DWS 行为汇总"),
        ("ads.ads_daily_overview", "dt", yesterday, "ADS 经营概览"),
    ]

    try:
        import pymysql
        conn = pymysql.connect(
            host=os.getenv("DORIS_FE_HOST", "doris-fe"),
            port=int(os.getenv("DORIS_QUERY_PORT", "9030")),
            user=os.getenv("DORIS_USER", "root"),
            password=os.getenv("DORIS_PASSWORD", ""),
        )
    except ImportError:
        logger.warning("pymysql 未安装，跳过数仓延迟检查")
        return True
    except Exception as e:
        logger.error(f"连接 Doris 失败: {e}")
        return False

    all_ok = True
    try:
        with conn.cursor() as cursor:
            for table, dt_col, expected_dt, desc in checks:
                try:
                    sql = f"SELECT COUNT(*) FROM {table} WHERE {dt_col} = '{expected_dt}'"
                    cursor.execute(sql)
                    count = cursor.fetchone()[0]

                    if count > 0:
                        logger.info(f"  ✅ {desc}: {count} 条 (dt={expected_dt})")
                    else:
                        logger.error(f"  ❌ {desc}: 数据为空 (dt={expected_dt})")
                        send_alert(
                            f"🟡 数仓延迟: {desc}",
                            f"- 表: {table}\n- 期望日期: {expected_dt}\n- 当前: 0 条数据",
                            "warning"
                        )
                        all_ok = False
                except Exception as e:
                    logger.warning(f"  ⚠️ {desc}: 查询失败 ({e})")

    finally:
        conn.close()

    return all_ok


# ====================== 磁盘空间检查 ======================

def check_disk():
    """检查本地磁盘和 HDFS 使用率"""
    logger.info("🔍 检查存储空间...")

    all_ok = True

    # 本地磁盘
    try:
        result = subprocess.run(
            ["df", "-h", "/", "/data"],
            capture_output=True, text=True, timeout=10
        )
        lines = result.stdout.strip().split("\n")
        for line in lines[1:]:
            parts = line.split()
            if len(parts) >= 5:
                usage = parts[4].replace("%", "")
                mount = parts[-1]
                try:
                    pct = int(usage)
                    if pct >= 90:
                        logger.error(f"  ❌ 磁盘 {mount}: {pct}% (≥90%)")
                        send_alert(
                            f"🔴 磁盘空间不足: {mount}",
                            f"- 使用率: {pct}%\n- 请立即清理",
                            "critical"
                        )
                        all_ok = False
                    elif pct >= 80:
                        logger.warning(f"  ⚠️ 磁盘 {mount}: {pct}% (≥80%)")
                    else:
                        logger.info(f"  ✅ 磁盘 {mount}: {pct}%")
                except ValueError:
                    pass
    except Exception as e:
        logger.warning(f"  磁盘检查失败: {e}")

    # HDFS
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-df", "-h", "/"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            logger.info(f"  HDFS:\n{result.stdout}")
        else:
            logger.warning(f"  HDFS 检查跳过 (hdfs 命令不可用)")
    except FileNotFoundError:
        logger.info("  HDFS 检查跳过 (hdfs 命令不在 PATH)")

    return all_ok


# ====================== 汇总报告 ======================

def generate_report(results: dict) -> str:
    """生成监控报告"""
    lines = [
        f"# 🏥 大数据平台健康报告",
        f"**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ]

    all_ok = all(results.values())
    status = "🟢 全部正常" if all_ok else "🔴 存在异常"
    lines.append(f"**总体状态**: {status}")
    lines.append("")

    for module, ok in results.items():
        icon = "✅" if ok else "❌"
        lines.append(f"- {icon} {module}")

    return "\n".join(lines)


# ====================== CLI ======================

def main():
    parser = argparse.ArgumentParser(description="大数据平台监控告警")
    parser.add_argument("target", nargs="?", default="all",
                        choices=["flink", "airflow", "warehouse", "disk", "all"],
                        help="监控目标 (默认 all)")
    parser.add_argument("--silent", action="store_true", help="静默模式 (不发送告警)")

    args = parser.parse_args()

    if args.silent:
        os.environ.pop("ALERT_WEBHOOK_URL", None)
        os.environ.pop("ALERT_EMAIL", None)

    logger.info("=" * 50)
    logger.info("🏥 大数据平台健康检查")
    logger.info("=" * 50)

    checkers = {
        "Flink 实时任务": check_flink,
        "Airflow 调度": check_airflow,
        "数仓数据延迟": check_warehouse,
        "存储空间": check_disk,
    }

    if args.target != "all":
        target_map = {
            "flink": "Flink 实时任务",
            "airflow": "Airflow 调度",
            "warehouse": "数仓数据延迟",
            "disk": "存储空间",
        }
        key = target_map[args.target]
        checkers = {key: checkers[key]}

    results = {}
    for name, checker in checkers.items():
        try:
            results[name] = checker()
        except Exception as e:
            logger.error(f"{name} 检查异常: {e}")
            results[name] = False

    report = generate_report(results)
    logger.info("\n" + report)

    if not all(results.values()):
        logger.error("❌ 存在异常，请排查")
        sys.exit(1)
    else:
        logger.info("✅ 全部检查通过")


if __name__ == "__main__":
    main()
