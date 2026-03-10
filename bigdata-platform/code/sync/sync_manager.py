#!/usr/bin/env python3
"""
数据同步任务管理工具
支持 DataX、Sqoop、自定义同步任务
"""

import os
import sys
import json
import time
import argparse
import subprocess
import logging
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('sync.log')
    ]
)
logger = logging.getLogger('SyncManager')


class DataXJob:
    """DataX 任务管理类"""

    def __init__(self, datax_home='/opt/datax', env_file='.env'):
        self.datax_home = datax_home
        self.env = self._load_env(env_file)

    def _load_env(self, env_file):
        """加载环境变量"""
        env = {}
        if os.path.exists(env_file):
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env[key.strip()] = value.strip()
        return env

    def _render_template(self, template_path, params):
        """渲染 DataX JSON 模板"""
        with open(template_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 替换占位符 ${KEY}
        for key, value in params.items():
            content = content.replace(f'${{{key}}}', str(value))

        # 临时文件
        temp_file = f'/tmp/datax_temp_{int(time.time())}.json'
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(content)

        return temp_file

    def run_job(self, template_path, run_date=None, env=None):
        """运行 DataX 任务"""
        if not os.path.exists(template_path):
            logger.error(f"模板文件不存在: {template_path}")
            return False

        # 参数准备
        params = self.env.copy()
        params.update(env or {})
        params['RUN_DATE'] = run_date or datetime.now().strftime('%Y-%m-%d')
        params['CURRENT_DATE'] = datetime.now().strftime('%Y-%m-%d')
        params['CURRENT_TIME'] = datetime.now().strftime('%H:%M:%S')

        logger.info(f"运行 DataX 任务: {template_path} date={params['RUN_DATE']}")

        # 渲染模板
        temp_file = self._render_template(template_path, params)

        try:
            # 执行 DataX 命令
            cmd = [
                sys.executable,
                f'{self.datax_home}/bin/datax.py',
                temp_file,
                '-p',
                ' '.join([f'-D{k}={v}' for k, v in params.items()[:10]])  # 只传前10个参数避免过长
            ]

            logger.info(f"执行命令: {' '.join(cmd)}")

            # 执行并实时输出日志
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                encoding='utf-8'
            )

            # 实时输出
            for line in proc.stdout:
                print(line.strip())
                logger.info(line.strip())

            proc.wait()

            if proc.returncode == 0:
                logger.info(f"任务成功完成: {template_path}")
                return True
            else:
                logger.error(f"任务失败，返回码: {proc.returncode}")
                return False

        except Exception as e:
            logger.error(f"任务执行异常: {e}", exc_info=True)
            return False
        finally:
            # 删除临时文件
            if os.path.exists(temp_file):
                os.remove(temp_file)


class SyncManager:
    """数据同步管理器"""

    def __init__(self, datax_home='/opt/datax'):
        self.datax = DataXJob(datax_home)
        self.job_configs = {
            'orders': {
                'template': 'datax-jobs/mysql2hdfs.json',
                'description': '订单表同步到 HDFS ODS 层',
                'deps': []
            },
            'users': {
                'template': 'datax-jobs/mysql2hdfs_users.json',
                'description': '用户表同步到 HDFS ODS 层',
                'deps': []
            },
            'products': {
                'template': 'datax-jobs/mysql2hdfs_products.json',
                'description': '商品表同步到 HDFS ODS 层',
                'deps': []
            },
            'hdfs2doris': {
                'template': 'datax-jobs/hdfs2doris.json',
                'description': 'HDFS ODS 层数据同步到 Doris',
                'deps': ['orders', 'users', 'products']
            }
        }

    def run_single_job(self, job_name, run_date=None):
        """运行单个同步任务"""
        if job_name not in self.job_configs:
            logger.error(f"任务不存在: {job_name}")
            return False

        job_config = self.job_configs[job_name]

        # 先运行依赖任务
        for dep in job_config['deps']:
            logger.info(f"运行依赖任务: {dep}")
            if not self.run_single_job(dep, run_date):
                logger.error(f"依赖任务失败: {dep}")
                return False

        # 运行当前任务
        return self.datax.run_job(job_config['template'], run_date)

    def run_all_jobs(self, run_date=None):
        """运行所有同步任务"""
        success = True
        for job_name in self.job_configs:
            if not self.run_single_job(job_name, run_date):
                success = False
                logger.error(f"任务失败: {job_name}")
        return success

    def run_daily_sync(self, run_date=None):
        """运行每日数据同步任务"""
        if run_date is None:
            # 默认昨天
            run_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        logger.info(f"开始每日数据同步: {run_date}")

        # 核心业务表同步
        jobs = ['orders', 'users', 'products', 'hdfs2doris']
        success = True

        for job_name in jobs:
            if not self.run_single_job(job_name, run_date):
                success = False
                logger.error(f"每日同步任务失败: {job_name}")

        if success:
            logger.info("每日数据同步全部成功!")
        else:
            logger.error("每日数据同步有任务失败!")

        return success

    def show_jobs(self):
        """显示所有任务列表"""
        print("=" * 80)
        print(f"{'任务名称':<15} {'描述':<40} {'依赖'}")
        print("=" * 80)
        for job_name, config in self.job_configs.items():
            deps = ','.join(config['deps']) or '无'
            print(f"{job_name:<15} {config['description']:<40} {deps}")
        print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description='数据同步任务管理工具')
    parser.add_argument('action', choices=['run', 'all', 'daily', 'list'], help='操作类型')
    parser.add_argument('--job', help='任务名称')
    parser.add_argument('--date', help='运行日期 (YYYY-MM-DD)')
    parser.add_argument('--datax-home', default='/opt/datax', help='DataX 安装路径')

    args = parser.parse_args()

    manager = SyncManager(args.datax_home)

    if args.action == 'list':
        manager.show_jobs()

    elif args.action == 'run':
        if not args.job:
            print("请指定任务名称: --job <job_name>")
            sys.exit(1)
        success = manager.run_single_job(args.job, args.date)
        sys.exit(0 if success else 1)

    elif args.action == 'all':
        success = manager.run_all_jobs(args.date)
        sys.exit(0 if success else 1)

    elif args.action == 'daily':
        success = manager.run_daily_sync(args.date)
        sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
