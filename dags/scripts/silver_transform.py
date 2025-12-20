from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Silver Layer - Data Normalization") \
    .getOrCreate()

print("=== SILVER LAYER: Normalizing and cleaning data ===")

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

# ========== PRODUCTS SILVER ==========
print("Transforming bronze.products → silver.products...")

df_products_bronze = spark.table("iceberg.bronze.products")

df_products_silver = df_products_bronze \
    .withColumn("expiration_date", 
                when(col("product_info_expiration_date").isNotNull(),
                     to_date(col("product_info_expiration_date"), "yyyyMMdd"))
                .otherwise(None)) \
    .withColumn("product_timestamp", 
                when(col("product_info_timestamp").isNotNull(),
                     to_timestamp(col("product_info_timestamp")))
                .otherwise(None)) \
    .withColumn("is_option", when(col("product_info_financial_product") == "Option", lit(True)).otherwise(lit(False))) \
    .withColumn("is_call", when(col("product_info_put_or_call") == "Call", lit(True)).otherwise(lit(False))) \
    .withColumn("is_put", when(col("product_info_put_or_call") == "Put", lit(True)).otherwise(lit(False))) \
    .withColumn("total_volume", 
                coalesce(col("all_volume"), lit(0)) + 
                coalesce(col("ctag_volume"), lit(0)) + 
                coalesce(col("etag_volume"), lit(0)) + 
                coalesce(col("ptag_volume"), lit(0))) \
    .withColumn("has_activity", when(col("total_volume") > 0, lit(True)).otherwise(lit(False))) \
    .filter(col("product_info_order_book_id").isNotNull()) \
    .select(
        col("product_info_order_book_id").alias("order_book_id"),
        col("product_info_symbol").alias("symbol"),
        col("product_family"),
        col("product_info_financial_product").alias("financial_product"),
        col("is_option"),
        col("is_call"),
        col("is_put"),
        col("product_info_strike_price").alias("strike_price"),
        col("expiration_date"),
        col("product_timestamp"),
        col("product_info_underlying_order_book_id").alias("underlying_order_book_id"),
        col("all_volume"),
        col("ctag_volume"),
        col("etag_volume"),
        col("ptag_volume"),
        col("total_volume"),
        col("has_activity"),
        col("taker_count"),
        col("unique_id_count"),
        col("modify_count"),
        current_timestamp().alias("processed_timestamp")
    )

spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.silver.products (
        order_book_id BIGINT,
        symbol STRING,
        product_family STRING,
        financial_product STRING,
        is_option BOOLEAN,
        is_call BOOLEAN,
        is_put BOOLEAN,
        strike_price DOUBLE,
        expiration_date DATE,
        product_timestamp TIMESTAMP,
        underlying_order_book_id BIGINT,
        all_volume BIGINT,
        ctag_volume BIGINT,
        etag_volume BIGINT,
        ptag_volume BIGINT,
        total_volume BIGINT,
        has_activity BOOLEAN,
        taker_count INT,
        unique_id_count INT,
        modify_count INT,
        processed_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (product_family)
""")

df_products_silver.writeTo("iceberg.silver.products").overwrite()

print(f"✓ Transformed {df_products_silver.count()} products to silver.products")

# ========== ORDERS SILVER ==========
print("Transforming bronze.orders → silver.orders...")

df_orders_bronze = spark.table("iceberg.bronze.orders")

df_orders_silver = df_orders_bronze \
    .withColumn("created_timestamp", 
                when(col("created_on").isNotNull(), to_timestamp(col("created_on")))
                .otherwise(None)) \
    .withColumn("order_timestamp", 
                when(col("timestamp").isNotNull(), to_timestamp(col("timestamp")))
                .otherwise(None)) \
    .withColumn("is_deleted", when(col("deleted") == "true", lit(True)).otherwise(lit(False))) \
    .withColumn("is_fully_executed", when(col("fully_executed") == "true", lit(True)).otherwise(lit(False))) \
    .withColumn("is_partially_executed", when(col("partially_executed") == "true", lit(True)).otherwise(lit(False))) \
    .withColumn("is_buy", when(col("side") == "Buy", lit(True)).otherwise(lit(False))) \
    .withColumn("is_sell", when(col("side") == "Sell", lit(True)).otherwise(lit(False))) \
    .withColumn("executed_qty", col("created_with_qty") - col("qty_on_end")) \
    .withColumn("fill_rate", 
                when(col("created_with_qty") > 0,
                     (col("created_with_qty") - col("qty_on_end")) / col("created_with_qty"))
                .otherwise(lit(0.0))) \
    .withColumn("existed_for_seconds", col("existed_for") / 1e9) \
    .withColumn("reaction_time_seconds", col("min_reaction_time") / 1e9) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("order_timestamp").isNotNull()) \
    .select(
        col("order_id"),
        col("order_book_id"),
        col("created_timestamp"),
        col("order_timestamp"),
        col("side"),
        col("is_buy"),
        col("is_sell"),
        col("created_with_qty"),
        col("qty_on_end"),
        col("executed_qty"),
        col("fill_rate"),
        col("is_deleted"),
        col("is_fully_executed"),
        col("is_partially_executed"),
        col("existed_for_seconds"),
        col("reaction_time_seconds"),
        col("modify_count"),
        col("distance_from_bbo_avg"),
        col("distance_from_bbo_max"),
        col("distance_from_bbo_min"),
        col("price_dif_from_bbo_avg"),
        col("priority_count_avg"),
        current_timestamp().alias("processed_timestamp")
    )

spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.silver.orders (
        order_id BIGINT,
        order_book_id BIGINT,
        created_timestamp TIMESTAMP,
        order_timestamp TIMESTAMP,
        side STRING,
        is_buy BOOLEAN,
        is_sell BOOLEAN,
        created_with_qty INT,
        qty_on_end INT,
        executed_qty INT,
        fill_rate DOUBLE,
        is_deleted BOOLEAN,
        is_fully_executed BOOLEAN,
        is_partially_executed BOOLEAN,
        existed_for_seconds DOUBLE,
        reaction_time_seconds DOUBLE,
        modify_count INT,
        distance_from_bbo_avg DOUBLE,
        distance_from_bbo_max INT,
        distance_from_bbo_min INT,
        price_dif_from_bbo_avg DOUBLE,
        priority_count_avg DOUBLE,
        processed_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(order_timestamp))
""")

df_orders_silver.writeTo("iceberg.silver.orders").overwrite()

print(f"✓ Transformed {df_orders_silver.count()} orders to silver.orders")

spark.stop()
print("=== SILVER LAYER COMPLETE ===")
