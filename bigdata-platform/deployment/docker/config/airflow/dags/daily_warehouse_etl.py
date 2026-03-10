"""
示例 DAG: 每日数仓 ETL (ODS → DWD → DWS → ADS)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG('daily_warehouse_etl', default_args={'owner':'bigdata','retries':2,'retry_delay':timedelta(minutes=5)},
         description='每日数仓 ETL', schedule_interval='0 2 * * *', start_date=datetime(2026,1,1),
         catchup=False, tags=['数仓','ETL']) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    ods_orders = BashOperator(task_id='ods_sync_orders', bash_command='echo "同步订单到 ODS"')
    ods_users = BashOperator(task_id='ods_sync_users', bash_command='echo "同步用户到 ODS"')
    dwd = BashOperator(task_id='dwd_trade_order', bash_command='echo "DWD 清洗加工"')
    dws = BashOperator(task_id='dws_trade_store_1d', bash_command='echo "DWS 汇总统计"')
    ads = BashOperator(task_id='ads_daily_revenue', bash_command='echo "ADS 报表产出"')
    check = BashOperator(task_id='quality_check', bash_command='echo "数据质量检查通过"')

    start >> [ods_orders, ods_users] >> dwd >> dws >> ads >> check >> end
