from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Аргументы по умолчанию
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

# Определение DAG
with DAG(
    dag_id="test_spark_bash_cluster",
    default_args=default_args,
    schedule=None,         # Запуск только вручную
    catchup=False,
    tags=["test", "spark", "bash"],
) as dag:

    # Задача запуска Spark через BashOperator
    # Это самый надежный способ, так как мы явно контролируем все аргументы
    # и избегаем проблем с парсингом URL в SparkSubmitOperator
    run_spark_pi = BashOperator(
        task_id="run_spark_pi",
        bash_command= "spark-submit pwd"
    )


run_spark_pi
