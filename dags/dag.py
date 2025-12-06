from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="test_spark_dag",
    default_args=default_args,
    schedule=None,          # вместо schedule_interval
    catchup=False,
    tags=["test", "spark"],
) as dag:

    run_spark = SparkSubmitOperator(
        task_id="run_spark_pi",
        application="/opt/spark/work-dir/test_spark_pi.py",  # твой PySpark-скрипт
        conn_id="spark_default",                             # коннект в Airflow
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.app.name": "airflow_test_pi",
        },
    )

run_spark