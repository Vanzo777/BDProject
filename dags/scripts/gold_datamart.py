from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Gold Layer - Analytics Datamart") \
    .getOrCreate()

print("=== GOLD LAYER: Creating analytics datamart ===")

# Создаем namespace gold
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

# Загружаем silver таблицы
df_products = spark.table("iceberg.silver.products")
df_orders = spark.table("iceberg.silver.orders")

print("Building product_trading_metrics datamart...")

# JOIN orders с products для создания витрины
df_joined = df_orders.join(
    df_products,
    df_orders.order_book_id == df_products.order_book_id,
    "inner"
)

# Агрегируем метрики по продуктам
df_gold = df_joined.groupBy(
    col("product_family"),
    col("symbol"),
    col("financial_product"),
    col("strike_price"),
    col("expiration_date")
).agg(
    # Метрики ордеров
    count("order_id").alias("total_orders"),
    countDistinct("order_id").alias("unique_orders"),
    sum("created_with_qty").alias("total_qty_created"),
    sum("executed_qty").alias("total_qty_executed"),
    avg("fill_rate").alias("avg_fill_rate"),
    
    # Метрики по типам
    sum(when(col("is_buy"), 1).otherwise(0)).alias("buy_orders_count"),
    sum(when(col("is_sell"), 1).otherwise(0)).alias("sell_orders_count"),
    sum(when(col("is_fully_executed"), 1).otherwise(0)).alias("fully_executed_count"),
    sum(when(col("is_partially_executed"), 1).otherwise(0)).alias("partially_executed_count"),
    sum(when(col("is_deleted"), 1).otherwise(0)).alias("deleted_orders_count"),
    
    # Временные метрики
    avg("existed_for_seconds").alias("avg_order_lifetime_sec"),
    max("existed_for_seconds").alias("max_order_lifetime_sec"),
    avg("reaction_time_seconds").alias("avg_reaction_time_sec"),
    
    # Метрики модификаций
    sum("modify_count").alias("total_modifications"),
    avg("modify_count").alias("avg_modifications_per_order"),
    
    # Метрики ценовых отклонений
    avg("price_dif_from_bbo_avg").alias("avg_price_diff_from_bbo"),
    avg("distance_from_bbo_avg").alias("avg_distance_from_bbo"),
    
    # Метрики активности продукта
    first("total_volume").alias("product_total_volume"),
    first("taker_count").alias("product_taker_count"),
    first("has_activity").alias("product_has_activity"),
    
    # Технические поля
    max("processed_timestamp").alias("last_processed_at")
).withColumn(
    # Вычисляемые KPI
    "execution_rate", col("fully_executed_count") / col("total_orders")
).withColumn(
    "buy_sell_ratio", col("buy_orders_count") / col("sell_orders_count")
).withColumn(
    "cancellation_rate", col("deleted_orders_count") / col("total_orders")
).withColumn(
    "created_timestamp", current_timestamp()
)

# Создаем gold таблицу
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

df_gold.writeTo("iceberg.gold.product_trading_metrics").overwrite()

print(f"✓ Created datamart with {df_gold.count()} product metrics")

# Дополнительная витрина: Daily trading summary
print("Building daily_trading_summary datamart...")

df_daily = df_joined.withColumn(
    "trade_date", to_date(col("order_timestamp"))
).groupBy("trade_date", "product_family").agg(
    count("order_id").alias("daily_orders"),
    sum("executed_qty").alias("daily_volume"),
    avg("fill_rate").alias("daily_avg_fill_rate"),
    countDistinct("order_book_id").alias("active_products")
).withColumn("created_timestamp", current_timestamp())

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

df_daily.writeTo("iceberg.gold.daily_trading_summary").overwrite()

print(f"✓ Created daily summary with {df_daily.count()} records")

spark.stop()
print("=== GOLD LAYER COMPLETE ===")
