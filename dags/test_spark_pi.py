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
    """
    Оценка числа Pi методом Монте-Карло
    """
    spark = SparkSession.builder \
        .appName("TestPi") \
        .getOrCreate()

    sc = spark.sparkContext

    log.info("Запускаем расчёт Pi с %d итерациями", n)

    def sample(_):
        """Генерация случайной точки и проверка попадания в круг"""
        import random
        x = random.random()
        y = random.random()
        return 1 if x * x + y * y < 1.0 else 0

    # Распараллеливаем вычисления
    count = sc.parallelize(range(n), numSlices=10) \
              .map(sample) \
              .reduce(lambda a, b: a + b)

    log.info("Количество точек внутри круга: %d из %d", count, n)

    pi_value = 4.0 * count / n
    log.info("Оценка Pi = %.6f", pi_value)

    spark.stop()
    log.info("SparkSession остановлен")

    return pi_value


if __name__ == "__main__":
    start = datetime.now()
    log.info("Старт job: %s", start.isoformat())

    n = 1_000_000
    pi_est = estimate_pi(n)

    end = datetime.now()
    log.info("Завершено. Итоговая оценка Pi = %.6f, длительность = %s", 
             pi_est, end - start)
    
    print(f"\n{'='*80}")
    print(f"Результат: Pi ≈ {pi_est:.6f}")
    print(f"Ошибка: {abs(pi_est - 3.141592653589793):.6f}")
    print(f"Время выполнения: {end - start}")
    print(f"{'='*80}\n")
