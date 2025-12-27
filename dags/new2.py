"""
DAG для загрузки CSV в Bronze через spark-submit
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Общие Spark конфигурации
spark_conf = {
    'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
    'spark.sql.catalog.iceberg.type': 'hadoop',
    'spark.sql.catalog.iceberg.warehouse': 's3a://lakehouse/',
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': 'minioadmin',
    'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
    'spark.sql.catalog.iceberg.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
    'spark.sql.defaultCatalog': 'iceberg',
}

with DAG(
    dag_id='load_raw_to_bronze_sparksubmit',
    default_args=default_args,
    description='Загрузка CSV в Bronze через SparkSubmitOperator',
    schedule=None,
    start_date=datetime(2025, 12, 26),
    catchup=False,
    tags=['medallion', 'bronze', 'spark', 'sparksubmit'],
) as dag:

    load_orders = SparkSubmitOperator(
        task_id='load_orders_sparksubmit',
        application='/opt/airflow/dags/scripts/load_csv_to_bronze_simple.py',
        conn_id='spark_default',
        name='load_raw_orders',
        deploy_mode='client',
        application_args=[
            's3a://lakehouse/raw/order_classification.csv',
            'raw_orders',
            'iceberg',
            'bronze',
            '100000'
        ],
        packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4',
        conf=spark_conf,
        verbose=True,
        driver_memory='2g',
        executor_memory='2g',
    )

    load_products = SparkSubmitOperator(
        task_id='load_products_sparksubmit',
        conn_id='spark_default',
        application='/opt/airflow/dags/scripts/load_csv_to_bronze_simple.py',
        name='load_raw_product_info',
        deploy_mode='client',
        application_args=[
            's3a://lakehouse/raw/product_info.csv',
            'raw_product_info',
            'iceberg',
            'bronze',
            '100000'
        ],
        packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4',
        conf=spark_conf,
        verbose=True,
        driver_memory='2g',
        executor_memory='2g',
    )

    load_orders >> load_products