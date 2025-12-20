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
    tags=["iceberg", "minio", "spark", "hive-metastore"],
) as dag:

    create_iceberg_table = BashOperator(
        task_id="create_iceberg_table",
        bash_command=r"""
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.hadoop:hadoop-aws:3.4.1 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=bdproject-airflow-worker-1 \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hive \
  --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.iceberg.warehouse=s3a://warehouse/iceberg/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio-server:9000 \
  --conf spark.hadoop.fs.s3a.access.key=miniominio \
  --conf spark.hadoop.fs.s3a.secret.key=miniominio \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1 \
  /opt/airflow/dags/scripts/create_iceberg_table.py
""",
    )

    create_iceberg_table
