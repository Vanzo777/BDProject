from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='load_raw_to_bronze_simple_parquet',
    default_args=default_args,
    description='Простая загрузка CSV в Bronze как Parquet (без Iceberg)',
    schedule=None,
    start_date=datetime(2025, 12, 26),
    catchup=False,
    tags=['medallion', 'bronze', 'simple'],
) as dag:

    load_data = BashOperator(
        task_id='load_csv_to_parquet',
        bash_command='cd /opt/airflow && python3 dags/scripts/csv_to_parquet_bronze.py',
        env={
            'PYSPARK_PYTHON': 'python3',
            'PYSPARK_DRIVER_PYTHON': 'python3'
        }
    )