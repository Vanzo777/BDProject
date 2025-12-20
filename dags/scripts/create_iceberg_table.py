from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Iceberg Example").getOrCreate()

spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.sales (
        id BIGINT,
        product STRING,
        amount DOUBLE,
        date DATE
    )
    USING iceberg
    PARTITIONED BY (date)
""")

spark.sql("""
    INSERT INTO iceberg.sales VALUES
    (1, 'Laptop', 1200.0, DATE'2024-01-15'),
    (2, 'Mouse', 25.0, DATE'2024-01-16'),
    (3, 'Keyboard', 75.0, DATE'2024-01-16')
""")

print("Current data:")
spark.table("iceberg.sales").show()

print("Snapshots:")
snapshots_df = spark.sql("""
    SELECT snapshot_id, committed_at, operation
    FROM iceberg.sales.snapshots
    ORDER BY committed_at
""")
snapshots_df.show(truncate=False)

# берем самый ранний snapshot_id (можно взять и последний)
first_snapshot_id = snapshots_df.first()["snapshot_id"]

print(f"Time travel to snapshot_id={first_snapshot_id}:")
spark.sql(f"SELECT * FROM iceberg.sales VERSION AS OF {first_snapshot_id}").show()

spark.stop()
