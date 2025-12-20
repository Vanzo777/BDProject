import logging
import time
from airflow.decorators import task
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

@task
def silver_to_gold(silver_path: str, table_type: str):
    logging.info(f"🏆 [GOLD] Building {table_type} mart from {silver_path}")
    
    spark = SparkSession.builder.appName(f"Gold_{table_type}").getOrCreate()
    time.sleep(45)  # "Агрегация"
    
    silver_df = spark.read.parquet(silver_path)
    
    if table_type == 'sales':
        gold_df = silver_df.groupBy("sale_date").agg(
            sum("amount").alias("total_amount"),
            count("order_id").alias("order_count"),
            avg("amount").alias("avg_order_value")
        )
        gold_path = f"s3a://gold/sales_summary/"
        
    elif table_type == 'customers':
        gold_df = silver_df.groupBy("customer_id").agg(
            first("customer_name").alias("customer_name"),
            sum("amount").alias("lifetime_value"),
            min("sale_date").alias("first_order_date"),
            max("sale_date").alias("last_order_date")
        )
        gold_path = f"s3a://gold/customer_stats/"
        
    else:  # products
        gold_df = silver_df.groupBy("product_id").agg(
            sum("quantity").alias("total_quantity_sold"),
            sum("amount").alias("total_revenue")
        )
        gold_path = f"s3a://gold/product_performance/"
    
    time.sleep(60)  # "Бизнес-логика"
    gold_df.coalesce(1).write.mode("overwrite").parquet(gold_path)
    
    logging.info(f"✅ [GOLD {table_type.upper()}] Ready: {gold_df.count()} records")
    spark.stop()
    return gold_path