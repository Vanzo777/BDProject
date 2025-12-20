from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder
    .appName("Gold Layer - Analytics Datamart")
    .getOrCreate()
)

print("=== GOLD LAYER: Creating analytics datamart ===")

# Namespace gold
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

# Aliases to avoid ambiguous columns after join
o = spark.table("iceberg.silver.orders").alias("o")
p = spark.table("iceberg.silver.products").alias("p")

print("Building product_trading_metrics datamart...")

# JOIN orders with products
df_joined = o.join(
    p,
    F.col("o.order_book_id") == F.col("p.order_book_id"),
    "inner"
)

# Product-level aggregation
df_gold = (
    df_joined
    .groupBy(
        F.col("p.product_family"),
        F.col("p.symbol"),
        F.col("p.financial_product"),
        F.col("p.strike_price"),
        F.col("p.expiration_date")
    )
    .agg(
        # Orders metrics
        F.count(F.col("o.order_id")).alias("total_orders"),
        F.count_distinct(F.col("o.order_id")).alias("unique_orders"),
        F.sum(F.col("o.created_with_qty")).alias("total_qty_created"),
        F.sum(F.col("o.executed_qty")).alias("total_qty_executed"),
        F.avg(F.col("o.fill_rate")).alias("avg_fill_rate"),

        # Side/execution flags
        F.sum(F.when(F.col("o.is_buy"), 1).otherwise(0)).alias("buy_orders_count"),
        F.sum(F.when(F.col("o.is_sell"), 1).otherwise(0)).alias("sell_orders_count"),
        F.sum(F.when(F.col("o.is_fully_executed"), 1).otherwise(0)).alias("fully_executed_count"),
        F.sum(F.when(F.col("o.is_partially_executed"), 1).otherwise(0)).alias("partially_executed_count"),
        F.sum(F.when(F.col("o.is_deleted"), 1).otherwise(0)).alias("deleted_orders_count"),

        # Time metrics
        F.avg(F.col("o.existed_for_seconds")).alias("avg_order_lifetime_sec"),
        F.max(F.col("o.existed_for_seconds")).alias("max_order_lifetime_sec"),
        F.avg(F.col("o.reaction_time_seconds")).alias("avg_reaction_time_sec"),

        # Modifications (explicitly from orders to avoid ambiguity)
        F.sum(F.col("o.modify_count")).alias("total_modifications"),
        F.avg(F.col("o.modify_count")).alias("avg_modifications_per_order"),

        # Price deviation metrics
        F.avg(F.col("o.price_dif_from_bbo_avg")).alias("avg_price_diff_from_bbo"),
        F.avg(F.col("o.distance_from_bbo_avg")).alias("avg_distance_from_bbo"),

        # Product attributes (explicitly from products)
        F.max(F.col("p.total_volume")).alias("product_total_volume"),
        F.max(F.col("p.taker_count")).alias("product_taker_count"),
        F.max(F.col("p.has_activity").cast("int")).cast("boolean").alias("product_has_activity"),

        # Technical fields
        F.max(F.col("o.processed_timestamp")).alias("last_processed_at")
    )
    # KPI (safe division)
    .withColumn(
        "execution_rate",
        F.when(F.col("total_orders") > 0,
               F.col("fully_executed_count") / F.col("total_orders"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "buy_sell_ratio",
        F.when(F.col("sell_orders_count") > 0,
               F.col("buy_orders_count") / F.col("sell_orders_count"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "cancellation_rate",
        F.when(F.col("total_orders") > 0,
               F.col("deleted_orders_count") / F.col("total_orders"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn("created_timestamp", F.current_timestamp())
)

# Create gold table
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.gold.product_trading_metrics (
        product_family STRING,
        symbol STRING,
        financial_product STRING,
        strike_price DOUBLE,
        expiration_date DATE,
        total_orders BIGINT,
        unique_orders BIGINT,
        total_qty_created BIGINT,
        total_qty_executed BIGINT,
        avg_fill_rate DOUBLE,
        buy_orders_count BIGINT,
        sell_orders_count BIGINT,
        fully_executed_count BIGINT,
        partially_executed_count BIGINT,
        deleted_orders_count BIGINT,
        avg_order_lifetime_sec DOUBLE,
        max_order_lifetime_sec DOUBLE,
        avg_reaction_time_sec DOUBLE,
        total_modifications BIGINT,
        avg_modifications_per_order DOUBLE,
        avg_price_diff_from_bbo DOUBLE,
        avg_distance_from_bbo DOUBLE,
        product_total_volume BIGINT,
        product_taker_count INT,
        product_has_activity BOOLEAN,
        execution_rate DOUBLE,
        buy_sell_ratio DOUBLE,
        cancellation_rate DOUBLE,
        last_processed_at TIMESTAMP,
        created_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (product_family, months(expiration_date))
""")

# Write: dynamic partition overwrite (Iceberg-friendly)
df_gold.writeTo("iceberg.gold.product_trading_metrics") \
    .using("iceberg") \
    .overwritePartitions()

print(f"✓ Created datamart with {df_gold.count()} product metrics")


# ================== Daily trading summary ==================
print("Building daily_trading_summary datamart...")

df_daily = (
    df_joined
    .withColumn("trade_date", F.to_date(F.col("o.order_timestamp")))
    .groupBy(
        F.col("trade_date"),
        F.col("p.product_family")
    )
    .agg(
        F.count(F.col("o.order_id")).alias("daily_orders"),
        F.sum(F.col("o.executed_qty")).alias("daily_volume"),
        F.avg(F.col("o.fill_rate")).alias("daily_avg_fill_rate"),
        F.count_distinct(F.col("o.order_book_id")).alias("active_products")
    )
    .withColumn("created_timestamp", F.current_timestamp())
)

spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.gold.daily_trading_summary (
        trade_date DATE,
        product_family STRING,
        daily_orders BIGINT,
        daily_volume BIGINT,
        daily_avg_fill_rate DOUBLE,
        active_products BIGINT,
        created_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (months(trade_date))
""")

df_daily.writeTo("iceberg.gold.daily_trading_summary") \
    .using("iceberg") \
    .overwritePartitions()

print(f"✓ Created daily summary with {df_daily.count()} records")

spark.stop()
print("=== GOLD LAYER COMPLETE ===")
