from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, current_date  # Добавил current_date
import os

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("Bronze Layer - CSV to MinIO Raw") \
    .getOrCreate()

print("=== BRONZE LAYER: Loading CSV files to MinIO (Raw Zone) ===")

# Путь к сырым данным в MinIO
BRONZE_PATH = "s3a://warehouse/bronze/raw/"

# ========== PRODUCTS TABLE ==========
print("Loading products.csv to MinIO...")

df_products = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/airflow/data/products.csv")

# Добавляем технические метаданные
df_products = df_products \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("products.csv")) \
    .withColumn("load_date", current_date())

# Сохраняем в MinIO как Parquet (Bronze Raw)
df_products.write \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .parquet(f"{BRONZE_PATH}products/")

print(f"✓ Loaded {df_products.count()} products to {BRONZE_PATH}products/")

# ========== ORDERS TABLE ==========
print("Loading orders.csv to MinIO...")

df_orders = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/airflow/data/orders.csv")

df_orders = df_orders \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("orders.csv")) \
    .withColumn("load_date", current_date())

# Сохраняем в MinIO как Parquet (Bronze Raw)
df_orders.write \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .parquet(f"{BRONZE_PATH}orders/")

print(f"✓ Loaded {df_orders.count()} orders to {BRONZE_PATH}orders/")

# ========== CREATE ICEBERG TABLES FROM BRONZE ==========
print("Creating Iceberg tables from Bronze Parquet files...")

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

# Products в Iceberg
df_products_from_s3 = spark.read.parquet(f"{BRONZE_PATH}products/")

# Создаем таблицу динамически на основе схемы DataFrame
df_products_from_s3.writeTo("iceberg.bronze.products") \
    .using("iceberg") \
    .partitionBy("product_family") \
    .createOrReplace()

print(f"✓ Created iceberg.bronze.products with {df_products_from_s3.count()} records")

# Orders в Iceberg
df_orders_from_s3 = spark.read.parquet(f"{BRONZE_PATH}orders/")

df_orders_from_s3.writeTo("iceberg.bronze.orders") \
    .using("iceberg") \
    .partitionBy("load_date") \
    .createOrReplace()

print(f"✓ Created iceberg.bronze.orders with {df_orders_from_s3.count()} records")

spark.stop()
print("=== BRONZE LAYER COMPLETE ===")
