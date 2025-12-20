from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Iceberg Example").getOrCreate()

# Создаем таблицу с правильным трехуровневым именем: catalog.database.table
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.default.sales (
        id BIGINT,
        product STRING,
        amount DOUBLE,
        date DATE
    )
    USING iceberg
    PARTITIONED BY (date)
""")

# Вставляем данные
spark.sql("""
    INSERT INTO iceberg.default.sales VALUES
    (1, 'Laptop', 1200.0, DATE'2024-01-15'),
    (2, 'Mouse', 25.0, DATE'2024-01-16'),
    (3, 'Keyboard', 75.0, DATE'2024-01-16')
""")

# Показываем текущие данные
print("Current data:")
spark.table("iceberg.default.sales").show()

# Показываем снапшоты
print("Snapshots:")
snapshots_df = spark.sql("""
    SELECT snapshot_id, committed_at, operation
    FROM iceberg.default.sales.snapshots
    ORDER BY committed_at
""")
snapshots_df.show(truncate=False)

# Берем самый ранний snapshot_id (можно взять и последний)
first_snapshot_id = snapshots_df.first()["snapshot_id"]

# Time travel к первому снапшоту
print(f"Time travel to snapshot_id={first_snapshot_id}:")
spark.sql(f"SELECT * FROM iceberg.default.sales VERSION AS OF {first_snapshot_id}").show()

spark.stop()
