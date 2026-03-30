import logging
import time
from airflow.decorators import task
import psycopg2
from pyspark.sql import SparkSession

@task
def load_gold_to_postgres(gold_paths):
    logging.info("📊 [POSTGRES] Loading Gold tables to warehouse...")
    
    spark = SparkSession.builder.appName("PostgresLoad").getOrCreate()
    
    for i, path in enumerate(gold_paths):
        time.sleep(30)  # "Медленная" передача
        
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(f"gold_table_{i}")
        
        # PostgreSQL connection (твоя connection ID)
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/warehouse") \
            .option("dbtable", ["sales_summary", "customer_stats", "product_performance"][i]) \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
            
        logging.info(f"✅ Loaded table {i+1} to PostgreSQL")
    
    spark.stop()
    logging.info("🎉 Pipeline COMPLETED! All 3 marts loaded.")