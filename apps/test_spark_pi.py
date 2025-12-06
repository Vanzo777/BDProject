from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TestPi").getOrCreate()

    n = 100000
    rdd = spark.sparkContext.parallelize(range(1, n + 1))
    inside = (
        rdd.map(lambda i: ((i % 317) / 100.0) ** 2 + ((i % 271) / 100.0) ** 2 < 1)
           .filter(lambda x: x)
           .count()
    )

    pi = 4.0 * inside / n
    print(f"Estimated Pi = {pi}")

    spark.stop()
