import logging
import time
from airflow.decorators import task
from pyspark.sql import SparkSession

@task
def bronze_to_silver(bronze_path: str):
    logging.info(f"🔧 [SILVER] Transforming Bronze → Silver from {bronze_path}")
    
    spark = SparkSession.builder.appName("SilverLayer").getOrCreate()
    
    # Читаем Bronze (30 сек)
    time.sleep(30)
    bronze_df = spark.read.parquet(bronze_path)
    
    logging.info("🧹 Silver transformations: dedup, schema enforcement, joins...")
    time.sleep(90)  # "Комплексная" обработка
    
    # Имитация сложных трансформаций
    silver_df = bronze_df.dropDuplicates(['order_id']) \
        .withColumn("sale_date", col("sale_date_days").cast("date")) \
        .filter(col("amount") > 0)
    
    silver_path = f"s3a://silver/orders_clean/run_date={{context['ds']}}/"
    silver_df.coalesce(8).write.mode("overwrite").parquet(silver_path)
    
    logging.info(f"✅ [SILVER] Silver layer: {silver_df.count():,} clean rows")
    spark.stop()
    return "s3a://silver/orders_clean/"