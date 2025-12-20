from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import *

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("Bronze Layer - CSV Load") \
    .getOrCreate()

print("=== BRONZE LAYER: Loading CSV files to Iceberg ===")

# Создаем namespace bronze если не существует
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

# ========== PRODUCTS TABLE ==========
print("Loading products.csv...")

# Схема для products (основные поля)
products_schema = StructType([
    StructField("all_volume", LongType(), True),
    StructField("ctag_volume", LongType(), True),
    StructField("etag_volume", LongType(), True),
    StructField("modify_count", IntegerType(), True),
    StructField("occured_at_cross", LongType(), True),
    StructField("product_family", StringType(), True),
    StructField("product_info_expiration_date", StringType(), True),
    StructField("product_info_financial_product", StringType(), True),
    StructField("product_info_long_name", StringType(), True),
    StructField("product_info_number_of_decimal_in_price", IntegerType(), True),
    StructField("product_info_number_of_decimals_in_strike_price", IntegerType(), True),
    StructField("product_info_number_of_legs", IntegerType(), True),
    StructField("product_info_order_book_id", LongType(), True),
    StructField("product_info_put_or_call", StringType(), True),
    StructField("product_info_strike_price", DoubleType(), True),
    StructField("product_info_symbol", StringType(), True),
    StructField("product_info_timestamp", StringType(), True),
    StructField("product_info_underlying_order_book_id", LongType(), True),
    StructField("ptag_volume", LongType(), True),
    StructField("taker_count", IntegerType(), True),
    StructField("unique_id_count", IntegerType(), True),
])

# Читаем CSV с inferSchema для всех остальных полей (ticks, mtag)
df_products = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/airflow/data/products.csv")

# Добавляем технические поля
df_products = df_products \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("products.csv"))

# Создаем таблицу products в bronze
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.bronze.products (
        all_volume BIGINT,
        ctag_volume BIGINT,
        etag_volume BIGINT,
        modify_count INT,
        occured_at_cross BIGINT,
        product_family STRING,
        product_info_expiration_date STRING,
        product_info_financial_product STRING,
        product_info_long_name STRING,
        product_info_number_of_decimal_in_price INT,
        product_info_number_of_decimals_in_strike_price INT,
        product_info_number_of_legs INT,
        product_info_order_book_id BIGINT,
        product_info_put_or_call STRING,
        product_info_strike_price DOUBLE,
        product_info_symbol STRING,
        product_info_timestamp STRING,
        product_info_underlying_order_book_id BIGINT,
        ptag_volume BIGINT,
        taker_count INT,
        unique_id_count INT,
        load_timestamp TIMESTAMP,
        source_file STRING
    )
    USING iceberg
    PARTITIONED BY (product_family)
""")

# Загружаем данные
df_products.writeTo("iceberg.bronze.products").append()

print(f"✓ Loaded {df_products.count()} products to bronze.products")

# ========== ORDERS TABLE ==========
print("Loading orders.csv...")

orders_schema = StructType([
    StructField("created_on", StringType(), True),
    StructField("created_with_qty", IntegerType(), True),
    StructField("deleted", StringType(), True),
    StructField("distance_from_bbo_avg", DoubleType(), True),
    StructField("distance_from_bbo_max", IntegerType(), True),
    StructField("distance_from_bbo_min", IntegerType(), True),
    StructField("existed_for", LongType(), True),
    StructField("filled_on_start", IntegerType(), True),
    StructField("fully_executed", StringType(), True),
    StructField("min_reaction_time", LongType(), True),
    StructField("modify_count", IntegerType(), True),
    StructField("order_book_id", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("partially_executed", StringType(), True),
    StructField("price_dif_from_bbo_avg", DoubleType(), True),
    StructField("price_dif_from_bbo_max", IntegerType(), True),
    StructField("price_dif_from_bbo_min", IntegerType(), True),
    StructField("priority_count_avg", DoubleType(), True),
    StructField("priority_count_max", IntegerType(), True),
    StructField("priority_count_min", IntegerType(), True),
    StructField("qty_on_end", IntegerType(), True),
    StructField("side", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("price_level_change_avg", DoubleType(), True),
    StructField("price_level_change_max", DoubleType(), True),
    StructField("price_level_change_min", DoubleType(), True),
    StructField("tick_count_price_level_change_avg", DoubleType(), True),
    StructField("tick_count_price_level_change_max", DoubleType(), True),
    StructField("tick_count_price_level_change_min", DoubleType(), True),
    StructField("time_passed_since_last_event_avg", DoubleType(), True),
    StructField("time_passed_since_last_event_max", DoubleType(), True),
    StructField("time_passed_since_last_event_min", DoubleType(), True),
])

df_orders = spark.read \
    .option("header", "true") \
    .schema(orders_schema) \
    .csv("/opt/airflow/data/orders.csv")

# Добавляем технические поля
df_orders = df_orders \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("orders.csv"))

# Создаем таблицу orders в bronze
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.bronze.orders (
        created_on STRING,
        created_with_qty INT,
        deleted STRING,
        distance_from_bbo_avg DOUBLE,
        distance_from_bbo_max INT,
        distance_from_bbo_min INT,
        existed_for BIGINT,
        filled_on_start INT,
        fully_executed STRING,
        min_reaction_time BIGINT,
        modify_count INT,
        order_book_id BIGINT,
        order_id BIGINT,
        partially_executed STRING,
        price_dif_from_bbo_avg DOUBLE,
        price_dif_from_bbo_max INT,
        price_dif_from_bbo_min INT,
        priority_count_avg DOUBLE,
        priority_count_max INT,
        priority_count_min INT,
        qty_on_end INT,
        side STRING,
        timestamp STRING,
        price_level_change_avg DOUBLE,
        price_level_change_max DOUBLE,
        price_level_change_min DOUBLE,
        tick_count_price_level_change_avg DOUBLE,
        tick_count_price_level_change_max DOUBLE,
        tick_count_price_level_change_min DOUBLE,
        time_passed_since_last_event_avg DOUBLE,
        time_passed_since_last_event_max DOUBLE,
        time_passed_since_last_event_min DOUBLE,
        load_timestamp TIMESTAMP,
        source_file STRING
    )
    USING iceberg
    PARTITIONED BY (days(timestamp))
""")

df_orders.writeTo("iceberg.bronze.orders").append()

print(f"✓ Loaded {df_orders.count()} orders to bronze.orders")

spark.stop()
print("=== BRONZE LAYER COMPLETE ===")
