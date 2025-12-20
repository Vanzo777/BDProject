import logging
import time
from airflow.decorators import task
from pyspark.sql import SparkSession
import boto3
from botocore.config import Config

@task
def read_minio_to_bronze(**context):
    logging.info("🚀 [BRONZE] Starting raw data ingestion from MinIO...")
    
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Имитация тяжелого чтения (3 минуты)
    logging.info("📥 Reading raw orders from s3a://landingzone/orders/")
    time.sleep(45)  # Симуляция сканирования файлов
    
    # Генерация фейковых данных (1M строк)
    logging.info("🔄 Generating 1M raw order records...")
    raw_df = spark.range(1000000).select(
        (spark.range(1, 1000).selectExpr("id as order_id", 
                                        "rand()*10000 as amount", 
                                        "rand()*30 as sale_date_days",
                                        "rand()*1000 as customer_id",
                                        "rand()*500 as product_id").collect() * 1000)
    )
    
    time.sleep(120)  # "Тяжелая" обработка
    bronze_path = f"s3a://bronze/orders/run_date={context['ds']}/"
    logging.info(f"💾 Writing Bronze layer to {bronze_path}")
    
    raw_df.coalesce(4).write \
        .mode("overwrite") \
        .parquet(bronze_path)
    
    bronze_df = spark.read.parquet(bronze_path)
    logging.info(f"✅ [BRONZE] Bronze layer ready: {bronze_df.count():,} rows")
    
    spark.stop()
    return "s3a://bronze/orders/"  # Возвращаем путь для следующего слоя