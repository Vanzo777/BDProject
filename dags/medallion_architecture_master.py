"""
Мастер-DAG для Medallion Architecture: Bronze → Silver → Gold
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='medallion_architecture_master',
    default_args=default_args,
    description='Мастер-DAG для управления Medallion архитектурой',
    schedule='@daily',
    start_date=datetime(2025, 12, 26),
    catchup=False,
    tags=['medallion', 'master'],
) as dag:

    trigger_bronze = TriggerDagRunOperator(
        task_id='trigger_load_raw_to_bronze',
        trigger_dag_id='load_raw_to_bronze',
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_bronze_to_silver',
        trigger_dag_id='bronze_to_silver_hft_processing',
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_silver_to_gold',
        trigger_dag_id='silver_to_gold_hft_analytics',
        wait_for_completion=True,
        poke_interval=30,
    )

    # Зависимости
    trigger_bronze >> trigger_silver >> trigger_gold