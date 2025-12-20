import logging
from datetime import datetime

from pyspark.sql import SparkSession

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("test_spark_pi")


def estimate_pi(n: int) -> float:
    # Убрали .master(...), пусть берет из spark-submit аргументов
    spark = SparkSession.builder.appName("TestPi").getOrCreate()

    sc = spark.sparkContext

    log.info("Запускаем расчёт Pi, n=%d", n)

    rdd = sc.parallelize(range(1, n + 1))

    log.info("Создан RDD из %d элементов, partitions=%d", n, rdd.getNumPartitions())

    def is_inside_circle(i: int) -> bool:
        x = (i % 317) / 100.0
        y = (i % 271) / 100.0
        return x * x + y * y < 1.0

    inside = (
        rdd.map(lambda i: (i, is_inside_circle(i)))
           .filter(lambda pair: pair[1])
           .count()
    )

    log.info("Количество точек внутри круга: %d", inside)

    pi_value = 4.0 * inside / n
    log.info("Оценка Pi = %.6f", pi_value)

    spark.stop()
    log.info("SparkSession остановлен")

    return pi_value


if __name__ == "__main__":
    start = datetime.now()
    log.info("Старт job: %s", start.isoformat())

    n = 500_000
    pi_est = estimate_pi(n)

    end = datetime.now()
    log.info("Завершено. Итоговая оценка Pi = %.6f, длительность = %s", pi_est, end - start)
