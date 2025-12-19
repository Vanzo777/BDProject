from pyspark.sql import SparkSession

# Создайте Spark Session с Iceberg
spark = SparkSession.builder \
    .appName("Iceberg Example") \
    .getOrCreate()

# Создайте тестовую таблицу
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

# Вставьте данные
spark.sql("""
    INSERT INTO iceberg.sales VALUES
    (1, 'Laptop', 1200.0, DATE'2024-01-15'),
    (2, 'Mouse', 25.0, DATE'2024-01-16'),
    (3, 'Keyboard', 75.0, DATE'2024-01-16')
""")

# Прочитайте данные
df = spark.table("iceberg.sales")
df.show()

# Time Travel - данные на определенную версию
spark.sql("SELECT * FROM iceberg.sales VERSION AS OF 1").show()

spark.stop()
