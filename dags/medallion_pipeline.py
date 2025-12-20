from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from dags.scripts.load_gold_to_postgres import create_postgres_load_tasks

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 20),
    'retries': 1,
}

with DAG(
    'medallion_pipeline',
    default_args=default_args,
    description='Bronze → Silver → Gold pipeline for products and orders',
    schedule=None,  # Изменено с schedule_interval на schedule
    catchup=False,
    tags=['iceberg', 'medallion', 'etl'],
) as dag:

    # Task 1: Загрузка CSV в Bronze слой (Iceberg таблицы)
    bronze_load = BashOperator(
        task_id='bronze_load_csv',
        bash_command="""
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
  /opt/airflow/dags/scripts/bronze_load_csv.py
        """,
    )

    # Task 2: Трансформация Bronze → Silver
    silver_transform = BashOperator(
        task_id='silver_transform',
        bash_command="""
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
  /opt/airflow/dags/scripts/silver_transform.py
        """,
    )

    # Task 3: Создание витрины Silver → Gold
    gold_datamart = BashOperator(
        task_id='gold_datamart',
        bash_command="""
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
  /opt/airflow/dags/scripts/gold_datamart.py
        """,
    )

    # Определяем зависимости: Bronze → Silver → Gold
    bronze_load >> silver_transform >> gold_datamart >> create_postgres_load_tasks(dag)

