"""
自定义 Airflow Operator: DataX 任务执行器
"""
import subprocess
import sys
import os
import time
import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)


class DataXOperator(BaseOperator):
    """
    DataX 任务执行 Operator

    用法:
        DataXOperator(
            task_id='sync_orders',
            template_path='/opt/bigdata/sync/datax-jobs/mysql2hdfs.json',
            params={'RUN_DATE': '{{ ds }}'},
        )
    """

    template_fields = ('template_path', 'params')
    ui_color = '#1E90FF'
    ui_fgcolor = '#FFFFFF'

    @apply_defaults
    def __init__(self, template_path, params=None, datax_home='/opt/datax',
                 timeout=3600, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.template_path = template_path
        self.params = params or {}
        self.datax_home = datax_home
        self.timeout = timeout

    def _render_template_file(self, template_path, params):
        """渲染 DataX JSON 模板"""
        with open(template_path, 'r', encoding='utf-8') as f:
            content = f.read()

        for key, value in params.items():
            content = content.replace(f'${{{key}}}', str(value))

        temp_file = f'/tmp/datax_{self.task_id}_{int(time.time())}.json'
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(content)

        return temp_file

    def execute(self, context):
        """执行 DataX 任务"""
        if not os.path.exists(self.template_path):
            raise FileNotFoundError(f"DataX 模板不存在: {self.template_path}")

        # 合并 Airflow 模板变量
        all_params = dict(self.params)
        all_params.setdefault('RUN_DATE', context.get('ds', ''))

        # 从环境变量补充
        env_keys = ['MYSQL_HOST', 'MYSQL_PORT', 'MYSQL_DB', 'MYSQL_USER',
                     'MYSQL_PASSWORD', 'HDFS_URL', 'DORIS_FE_HOST',
                     'DORIS_HTTP_PORT', 'DORIS_QUERY_PORT', 'DORIS_DB',
                     'DORIS_USER', 'DORIS_PASSWORD']
        for key in env_keys:
            if key not in all_params and os.getenv(key):
                all_params[key] = os.getenv(key)

        temp_file = self._render_template_file(self.template_path, all_params)

        try:
            self.log.info("执行 DataX: %s", self.template_path)
            self.log.info("参数: RUN_DATE=%s", all_params.get('RUN_DATE'))

            cmd = [sys.executable, f'{self.datax_home}/bin/datax.py', temp_file]
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=self.timeout
            )

            self.log.info(result.stdout[-2000:] if result.stdout else "")

            if result.returncode != 0:
                error_msg = result.stderr[-1000:] if result.stderr else "未知错误"
                raise Exception(f"DataX 任务失败: {error_msg}")

            self.log.info("✅ DataX 任务成功: %s", self.template_path)

        except subprocess.TimeoutExpired:
            raise Exception(f"DataX 任务超时 ({self.timeout}s): {self.template_path}")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)


class SparkSubmitOperator(BaseOperator):
    """
    Spark 任务提交 Operator

    用法:
        SparkSubmitOperator(
            task_id='etl_ods_to_dwd',
            script_path='/opt/bigdata/batch/spark-etl/etl_ods_to_dwd.py',
            args=['{{ ds }}'],
        )
    """

    template_fields = ('script_path', 'args')
    ui_color = '#FF8C00'
    ui_fgcolor = '#FFFFFF'

    @apply_defaults
    def __init__(self, script_path, args=None, master='yarn',
                 deploy_mode='client', timeout=7200, *a, **kwargs):
        super().__init__(*a, **kwargs)
        self.script_path = script_path
        self.args = args or []
        self.master = master
        self.deploy_mode = deploy_mode
        self.timeout = timeout

    def execute(self, context):
        """提交 Spark 任务"""
        if not os.path.exists(self.script_path):
            raise FileNotFoundError(f"Spark 脚本不存在: {self.script_path}")

        cmd = [
            'spark-submit',
            '--master', self.master,
            '--deploy-mode', self.deploy_mode,
            self.script_path,
        ] + list(self.args)

        self.log.info("提交 Spark: %s", ' '.join(cmd))

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=self.timeout
            )

            self.log.info(result.stdout[-2000:] if result.stdout else "")

            if result.returncode != 0:
                error_msg = result.stderr[-1000:] if result.stderr else "未知错误"
                raise Exception(f"Spark 任务失败: {error_msg}")

            self.log.info("✅ Spark 任务成功: %s", self.script_path)

        except subprocess.TimeoutExpired:
            raise Exception(f"Spark 任务超时 ({self.timeout}s): {self.script_path}")
