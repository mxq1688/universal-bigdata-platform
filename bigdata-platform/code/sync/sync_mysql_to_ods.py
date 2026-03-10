#!/usr/bin/env python3
"""
MySQL → HDFS (ODS 层) 同步脚本
使用 DataX 将 MySQL 业务数据全量/增量同步到 HDFS ODS 层
"""
import os
import sys
import json
import subprocess
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('SyncMySQLToODS')


def render_template(template_path, params):
    """渲染 DataX JSON 模板，替换 ${KEY} 占位符"""
    with open(template_path, 'r', encoding='utf-8') as f:
        content = f.read()

    for key, value in params.items():
        content = content.replace(f'${{{key}}}', str(value))

    return content


def sync_table(template_path, table_name, date_str, datax_home='/opt/datax'):
    """同步单张表"""
    if not os.path.exists(template_path):
        logger.error("模板文件不存在: %s", template_path)
        return False

    params = {
        'RUN_DATE': date_str,
        'MYSQL_HOST': os.getenv('MYSQL_HOST', 'localhost'),
        'MYSQL_PORT': os.getenv('MYSQL_PORT', '3306'),
        'MYSQL_DB': os.getenv('MYSQL_DB', 'bigdata'),
        'MYSQL_USER': os.getenv('MYSQL_USER', 'root'),
        'MYSQL_PASSWORD': os.getenv('MYSQL_PASSWORD', ''),
        'HDFS_URL': os.getenv('HDFS_URL', 'hdfs://localhost:9000'),
    }

    rendered_json = render_template(template_path, params)

    # 写临时配置文件
    tmp_dir = '/tmp/datax_output'
    os.makedirs(tmp_dir, exist_ok=True)
    temp_config = os.path.join(tmp_dir, f'temp_{table_name}_{date_str}.json')

    with open(temp_config, 'w', encoding='utf-8') as f:
        f.write(rendered_json)

    try:
        logger.info("🚀 开始同步 %s，日期: %s", table_name, date_str)

        cmd = [sys.executable, f'{datax_home}/bin/datax.py', temp_config]
        logger.info("命令: %s", ' '.join(cmd))

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

        if result.returncode == 0:
            logger.info("✅ %s 同步成功", table_name)
            return True
        else:
            logger.error("❌ %s 同步失败: %s", table_name, result.stderr[-500:] if result.stderr else "无错误输出")
            return False

    except subprocess.TimeoutExpired:
        logger.error("❌ %s 同步超时 (1h)", table_name)
        return False
    except Exception as e:
        logger.error("❌ %s 执行异常: %s", table_name, e)
        return False
    finally:
        if os.path.exists(temp_config):
            os.remove(temp_config)


def create_hive_partition(table_name, date_str):
    """创建 Hive ODS 表分区"""
    hive_sql = f"ALTER TABLE ods_mysql.ods_mysql_{table_name} ADD IF NOT EXISTS PARTITION(dt='{date_str}');"

    logger.info("📥 创建 Hive 分区: ods_mysql.ods_mysql_%s/dt=%s", table_name, date_str)
    cmd = f"hive -e \"{hive_sql}\""

    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            logger.info("✅ Hive 分区创建成功")
        else:
            logger.warning("⚠️ Hive 分区创建失败 (可能已存在): %s", result.stderr[:200] if result.stderr else "")
    except Exception as e:
        logger.warning("⚠️ Hive 分区创建异常: %s", e)


def sync_mysql_to_hdfs(date_str=None, datax_home='/opt/datax'):
    """同步所有 MySQL 业务表到 HDFS ODS 层"""
    if not date_str:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    base_dir = os.path.dirname(os.path.abspath(__file__))

    tables = [
        {'name': 'orders', 'template': 'datax-jobs/mysql2hdfs.json'},
        {'name': 'users', 'template': 'datax-jobs/mysql2hdfs_users.json'},
        {'name': 'products', 'template': 'datax-jobs/mysql2hdfs_products.json'},
    ]

    logger.info("=" * 60)
    logger.info("开始每日 MySQL → HDFS 同步, 日期: %s", date_str)
    logger.info("同步表: %s", [t['name'] for t in tables])
    logger.info("=" * 60)

    success_count = 0
    fail_count = 0

    for table in tables:
        template_path = os.path.join(base_dir, table['template'])
        ok = sync_table(template_path, table['name'], date_str, datax_home)

        if ok:
            create_hive_partition(table['name'], date_str)
            success_count += 1
        else:
            fail_count += 1

    logger.info("=" * 60)
    logger.info("同步完成: 成功=%s, 失败=%s, 总计=%s", success_count, fail_count, len(tables))
    logger.info("=" * 60)

    return fail_count == 0


def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    datax_home = sys.argv[2] if len(sys.argv) > 2 else '/opt/datax'
    success = sync_mysql_to_hdfs(date_str=date_str, datax_home=datax_home)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
