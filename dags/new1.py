"""
DAG для загрузки CSV данных в Bronze слой через Spark
Использует BashOperator для запуска Python-скриптов
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='load_raw_to_bronze_working',
    default_args=default_args,
    description='Загрузка CSV в Bronze слой через Spark (рабочая версия)',
    schedule=None,
    start_date=datetime(2025, 12, 26),
    catchup=False,
    tags=['medallion', 'bronze', 'spark', 'working'],
) as dag:

    load_orders = BashOperator(
        task_id='load_orders_to_bronze',
        bash_command='python3 /opt/airflow/dags/scripts/load_csv_to_bronze.py s3a://lakehouse/raw/order_classification.csv raw_orders iceberg bronze 100000',
    )

    load_products = BashOperator(
        task_id='load_products_to_bronze',
        bash_command='python3 /opt/airflow/dags/scripts/load_csv_to_bronze.py s3a://lakehouse/raw/product_info.csv raw_product_info iceberg bronze 100000',
    )

    load_orders >> load_products