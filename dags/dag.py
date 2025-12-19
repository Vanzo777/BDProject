from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="test_spark_bash_cluster",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["test", "spark"],
) as dag:

    run_spark_cluster = BashOperator(
        task_id="run_spark_cluster",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --driver-memory 512m \
          --executor-memory 512m \
          --executor-cores 1 \
          --total-executor-cores 2 \
          --conf spark.driver.host=bdproject-airflow-worker-1 \
          --conf spark.driver.bindAddress=0.0.0.0 \
          /opt/airflow/dags/test_spark_pi.py
        """
    )

    run_spark_cluster
