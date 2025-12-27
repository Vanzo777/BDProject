import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_orders_schema():
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


if __name__ == "__main__":
    csv_path, table_name, catalog, schema, batch_size = sys.argv[1:6]
    
    logger.info("=" * 80)
    logger.info(f"ЗАГРУЗКА {table_name}")
    logger.info("=" * 80)
    
    spark = SparkSession.builder.appName(f"load_{table_name}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        data_schema = get_orders_schema() if table_name == "raw_orders" else get_product_info_schema()
        
        logger.info(f"Чтение CSV: {csv_path}")
        df = spark.read.option("header", "true").option("inferSchema", "false").schema(data_schema).csv(csv_path)
        
        count = df.count()
        logger.info(f"Прочитано: {count} записей")
        
        if count == 0:
            logger.warning("CSV пустой!")
            sys.exit(0)
        
        df.show(3, truncate=False)
        
        full_table = f"{catalog}.{schema}.{table_name}"
        
        try:
            existing = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}").collect()[0]['cnt']
            logger.info(f"Таблица существует: {existing} записей")
        except:
            logger.info("Создание таблицы...")
            fields = ', '.join([f"{f.name} STRING" for f in data_schema.fields])
            spark.sql(f"CREATE TABLE {full_table} ({fields}) USING iceberg TBLPROPERTIES ('format-version'='2')")
        
        logger.info("Запись...")
        df.writeTo(full_table).append()
        
        final = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}").collect()[0]['cnt']
        logger.info(f"Итого: {final} записей")
        logger.info("=" * 80)
        logger.info("УСПЕШНО")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"ОШИБКА: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()