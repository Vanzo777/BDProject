import sys
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_orders_schema():
    """Определяет схему для таблицы raw_orders"""
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("order_book_id", StringType(), True),
        StructField("side", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("created_on", StringType(), True),
        StructField("initial_qty", StringType(), True),
        StructField("created_with_qty", StringType(), True),
        StructField("qty_on_end", StringType(), True),
        StructField("filled_on_start", StringType(), True),
        StructField("deleted", StringType(), True),
        StructField("fully_executed", StringType(), True),
        StructField("partially_executed", StringType(), True),
        StructField("existed_for", StringType(), True),
        StructField("min_reaction_time", StringType(), True),
        StructField("modify_count", StringType(), True),
        StructField("distance_from_bbo_avg", StringType(), True),
        StructField("distance_from_bbo_max", StringType(), True),
        StructField("distance_from_bbo_min", StringType(), True),
        StructField("price_dif_from_bbo_avg", StringType(), True),
        StructField("price_dif_from_bbo_max", StringType(), True),
        StructField("price_dif_from_bbo_min", StringType(), True),
        StructField("priority_count_avg", StringType(), True),
        StructField("priority_count_max", StringType(), True),
        StructField("priority_count_min", StringType(), True),
        StructField("price_level_change_avg", StringType(), True),
        StructField("price_level_change_max", StringType(), True),
        StructField("price_level_change_min", StringType(), True),
        StructField("tick_count_price_level_change_avg", StringType(), True),
        StructField("tick_count_price_level_change_max", StringType(), True),
        StructField("tick_count_price_level_change_min", StringType(), True),
        StructField("time_passed_since_last_event_avg", StringType(), True),
        StructField("time_passed_since_last_event_max", StringType(), True),
        StructField("time_passed_since_last_event_min", StringType(), True)
    ])


def get_product_info_schema():
    """Определяет схему для таблицы raw_product_info"""
    return StructType([
        StructField("product_info_order_book_id", StringType(), True),
        StructField("product_info_underlying_order_book_id", StringType(), True),
        StructField("product_family", StringType(), True),
        StructField("product_info_symbol", StringType(), True),
        StructField("product_info_long_name", StringType(), True),
        StructField("product_info_financial_product", StringType(), True),
        StructField("product_info_put_or_call", StringType(), True),
        StructField("product_info_strike_price", StringType(), True),
        StructField("product_info_number_of_decimal_in_price", StringType(), True),
        StructField("product_info_number_of_decimals_in_strike_price", StringType(), True),
        StructField("product_info_expiration_date", StringType(), True),
        StructField("product_info_timestamp", StringType(), True),
        StructField("product_info_number_of_legs", StringType(), True),
        StructField("taker_count", StringType(), True),
        StructField("unique_id_count", StringType(), True),
        StructField("modify_count", StringType(), True),
        StructField("ctag_volume", StringType(), True),
        StructField("etag_volume", StringType(), True),
        StructField("ptag_volume", StringType(), True),
        StructField("leg_volume", StringType(), True),
        StructField("occured_at_cross", StringType(), True),
        StructField("ticks_0_price_from", StringType(), True),
        StructField("ticks_0_price_to", StringType(), True),
        StructField("ticks_0_tick_size", StringType(), True),
        StructField("ticks_0_timestamp", StringType(), True),
        StructField("ticks_1_price_from", StringType(), True),
        StructField("ticks_1_price_to", StringType(), True),
        StructField("ticks_1_tick_size", StringType(), True),
        StructField("ticks_1_timestamp", StringType(), True),
        StructField("ticks_2_price_from", StringType(), True),
        StructField("ticks_2_price_to", StringType(), True),
        StructField("ticks_2_tick_size", StringType(), True),
        StructField("ticks_2_timestamp", StringType(), True)
    ])


def main():
    """Основная функция загрузки"""
    if len(sys.argv) != 6:
        logger.error("Неверное количество аргументов")
        print("Usage: python3 load_csv_to_bronze.py <csv_path> <table_name> <catalog> <schema> <batch_size>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    table_name = sys.argv[2]
    catalog = sys.argv[3]
    schema = sys.argv[4]
    batch_size = int(sys.argv[5])
    
    logger.info("=" * 80)
    logger.info("ЗАГРУЗКА CSV В BRONZE")
    logger.info("=" * 80)
    logger.info(f"CSV путь: {csv_path}")
    logger.info(f"Таблица: {catalog}.{schema}.{table_name}")
    logger.info(f"Размер батча: {batch_size} KB")
    logger.info("=" * 80)
    
    os.environ['HADOOP_CONF_DIR'] = '/dev/null'
    
    spark = None
    try:
        logger.info("Создание Spark Session с явным переопределением всех S3 параметров...")
        
        spark = SparkSession.builder \
            .appName(f"load_{table_name}") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", "s3a://lakehouse/") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "300000") \
            .config("spark.hadoop.fs.s3a.socket.send.buffer", "8192") \
            .config("spark.hadoop.fs.s3a.socket.recv.buffer", "8192") \
            .config("spark.hadoop.fs.s3a.paging.maximum", "1000") \
            .config("spark.hadoop.fs.s3a.threads.max", "256") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
            .config("spark.hadoop.fs.s3a.connection.maximum", "200") \
            .config("spark.hadoop.fs.s3a.attempts.maximum", "20") \
            .config("spark.hadoop.fs.s3a.retry.throttle.limit", "20") \
            .config("spark.hadoop.fs.s3a.retry.throttle.interval", "1000") \
            .config("spark.hadoop.fs.s3a.retry.interval", "500") \
            .config("spark.hadoop.fs.s3a.timeout.interval", "10000") \
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
            .config("spark.hadoop.fs.s3a.multipart.threshold", "104857600") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.committer.threads", "8") \
            .config("spark.hadoop.fs.s3a.max.total.tasks", "1000") \
            .config("spark.hadoop.fs.s3a.list.version", "2") \
            .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.defaultCatalog", "iceberg") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
            .getOrCreate()
        
        logger.info(f"Spark Session создан. Версия: {spark.version}")
        spark.sparkContext.setLogLevel("WARN")
        
        if table_name == "raw_orders":
            data_schema = get_orders_schema()
        elif table_name == "raw_product_info":
            data_schema = get_product_info_schema()
        else:
            raise ValueError(f"Неизвестная таблица: {table_name}")
        
        logger.info(f"Чтение CSV из {csv_path}...")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("mode", "PERMISSIVE") \
            .schema(data_schema) \
            .csv(csv_path)
        
        count = df.count()
        logger.info(f"Прочитано записей: {count}")
        
        if count == 0:
            logger.warning("CSV пустой")
            return
        
        logger.info("Первые 3 строки:")
        df.show(3, truncate=False)
        
        full_table = f"{catalog}.{schema}.{table_name}"
        
        try:
            existing_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}").collect()[0]['cnt']
            logger.info(f"Таблица {full_table} существует. Текущих записей: {existing_count}")
            table_exists = True
        except Exception as e:
            logger.info(f"Таблица {full_table} не существует, создание...")
            table_exists = False
            
            fields = ', '.join([f"{f.name} STRING" for f in data_schema.fields])
            create_sql = f"""
                CREATE TABLE {full_table} (
                    {fields}
                )
                USING iceberg
                TBLPROPERTIES (
                    'format-version'='2',
                    'write.parquet.compression-codec'='snappy'
                )
            """
            spark.sql(create_sql)
            logger.info(f"Таблица {full_table} создана")
        
        logger.info(f"Запись в {full_table}...")
        df.writeTo(full_table) \
            .option("write-format", "parquet") \
            .append()
        
        final = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}").collect()[0]['cnt']
        logger.info(f"Итого записей в таблице: {final}")
        logger.info(f"Добавлено записей: {final - (existing_count if table_exists else 0)}")
        logger.info("=" * 80)
        logger.info("УСПЕШНО ЗАВЕРШЕНО")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("КРИТИЧЕСКАЯ ОШИБКА")
        logger.error("=" * 80)
        logger.error(f"Ошибка: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            logger.info("Остановка Spark Session...")
            spark.stop()
            logger.info("Spark Session остановлен")


if __name__ == "__main__":
    main()