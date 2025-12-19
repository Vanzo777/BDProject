from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="iceberg_example",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["iceberg", "minio"],
) as dag:

    create_iceberg_table = BashOperator(
        task_id="create_iceberg_table",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=bdproject-airflow-worker-1 \
          --conf spark.driver.bindAddress=0.0.0.0 \
          --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
          --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
          --conf spark.sql.catalog.iceberg.type=hadoop \
          --conf spark.sql.catalog.iceberg.warehouse=s3a://warehouse/ \
          --conf spark.hadoop.fs.s3a.endpoint=http://minio-server:9000 \
          --conf spark.hadoop.fs.s3a.access.key=minioadmin \
          --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
          --conf spark.hadoop.fs.s3a.path.style.access=true \
          --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
          /opt/airflow/dags/scripts/create_iceberg_table.py
        """
    )

    create_iceberg_table
