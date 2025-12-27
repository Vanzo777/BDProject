"""
DAG для обработки HFT данных из Bronze в Silver слой с использованием Trino.
Medallion Architecture: Bronze → Silver трансформация
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# Параметры подключения к Trino
TRINO_CONN_ID = 'trino_default'
CATALOG = 'iceberg'
BRONZE_SCHEMA = 'bronze'
SILVER_SCHEMA = 'silver'

# Флаг для пересоздания таблиц (установите True для DROP и пересоздания)
RECREATE_TABLES = True


# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


# Определение DAG
with DAG(
    dag_id='bronze_to_silver_hft_processing',
    default_args=default_args,
    description='ETL pipeline для обработки HFT trading данных из Bronze в Silver слой',
    schedule='@daily',
    start_date=datetime(2025, 12, 26),
    catchup=False,
    tags=['medallion', 'bronze-to-silver', 'hft', 'trino'],
) as dag:

    # Task 0: Создание Bronze схемы
    create_bronze_schema = SQLExecuteQueryOperator(
        task_id='create_bronze_schema',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}
            WITH (location = 's3a://lakehouse/bronze')
        """,
    )

    # Task 1: Создание Silver схемы
    create_silver_schema = SQLExecuteQueryOperator(
        task_id='create_silver_schema',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}
            WITH (location = 's3a://lakehouse/silver')
        """,
    )

    # Task 2a: Удаление таблицы orders если нужно пересоздать
    drop_silver_orders_table = SQLExecuteQueryOperator(
        task_id='drop_silver_orders_table',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {CATALOG}.{SILVER_SCHEMA}.orders
        """ if RECREATE_TABLES else "SELECT 1",
    )

    # Task 2b: Создание таблицы orders в Silver слое
    create_silver_orders_table = SQLExecuteQueryOperator(
        task_id='create_silver_orders_table',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.orders (
                -- Идентификаторы
                order_id BIGINT,
                order_book_id BIGINT,
                side VARCHAR,
                
                -- Временные метки
                timestamp TIMESTAMP(6),
                created_on TIMESTAMP(6),
                partition_date DATE,
                
                -- Количественные характеристики
                initial_qty DOUBLE,
                created_with_qty DOUBLE,
                qty_on_end DOUBLE,
                filled_on_start DOUBLE,
                
                -- Статусы
                deleted BOOLEAN,
                fully_executed BOOLEAN,
                partially_executed BOOLEAN,
                
                -- Метрики времени (в секундах)
                order_lifetime_seconds DOUBLE,
                min_reaction_time_ms DOUBLE,
                
                -- Метрики модификаций
                modify_count BIGINT,
                
                -- Расстояние от BBO (в тиках)
                distance_from_bbo_avg DOUBLE,
                distance_from_bbo_max DOUBLE,
                distance_from_bbo_min DOUBLE,
                
                -- Ценовая разница от BBO
                price_dif_from_bbo_avg DOUBLE,
                price_dif_from_bbo_max DOUBLE,
                price_dif_from_bbo_min DOUBLE,
                
                -- Приоритет в очереди
                priority_count_avg DOUBLE,
                priority_count_max DOUBLE,
                priority_count_min DOUBLE,
                
                -- Изменения ценовых уровней
                price_level_change_avg DOUBLE,
                price_level_change_max DOUBLE,
                price_level_change_min DOUBLE,
                
                -- Изменения в тиках
                tick_count_price_level_change_avg DOUBLE,
                tick_count_price_level_change_max DOUBLE,
                tick_count_price_level_change_min DOUBLE,
                
                -- Временные интервалы между событиями (в миллисекундах)
                time_passed_since_last_event_avg DOUBLE,
                time_passed_since_last_event_max DOUBLE,
                time_passed_since_last_event_min DOUBLE,
                
                -- Вычисляемые метрики
                execution_ratio DOUBLE,
                
                -- Метаданные обработки
                processed_at TIMESTAMP(6),
                data_quality_score DOUBLE,
                source_file VARCHAR
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['partition_date', 'order_book_id']
            )
        """,
    )

    # Task 3a: Удаление таблицы product_info если нужно пересоздать
    drop_silver_products_table = SQLExecuteQueryOperator(
        task_id='drop_silver_products_table',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {CATALOG}.{SILVER_SCHEMA}.product_info
        """ if RECREATE_TABLES else "SELECT 1",
    )

    # Task 3b: Создание таблицы product_info в Silver слое
    create_silver_products_table = SQLExecuteQueryOperator(
        task_id='create_silver_products_table',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.product_info (
                -- Идентификаторы продукта
                product_info_order_book_id BIGINT,
                product_info_underlying_order_book_id BIGINT,
                product_family VARCHAR,
                
                -- Информация о продукте
                product_info_symbol VARCHAR,
                product_info_long_name VARCHAR,
                product_info_financial_product VARCHAR,
                product_info_put_or_call VARCHAR,
                
                -- Ценовые параметры
                product_info_strike_price DOUBLE,
                product_info_number_of_decimal_in_price INT,
                product_info_number_of_decimals_in_strike_price INT,
                
                -- Даты
                product_info_expiration_date DATE,
                product_info_timestamp TIMESTAMP(6),
                
                -- Статистика
                product_info_number_of_legs INT,
                taker_count BIGINT,
                unique_id_count BIGINT,
                modify_count BIGINT,
                
                -- Объемы торговли
                ctag_volume DOUBLE,
                etag_volume DOUBLE,
                ptag_volume DOUBLE,
                leg_volume DOUBLE,
                occured_at_cross DOUBLE,
                
                -- Tick sizes (первые 3 уровня для упрощения)
                ticks_0_price_from DOUBLE,
                ticks_0_price_to DOUBLE,
                ticks_0_tick_size DOUBLE,
                ticks_0_timestamp TIMESTAMP(6),
                
                ticks_1_price_from DOUBLE,
                ticks_1_price_to DOUBLE,
                ticks_1_tick_size DOUBLE,
                ticks_1_timestamp TIMESTAMP(6),
                
                ticks_2_price_from DOUBLE,
                ticks_2_price_to DOUBLE,
                ticks_2_tick_size DOUBLE,
                ticks_2_timestamp TIMESTAMP(6),
                
                -- Метаданные обработки
                processed_at TIMESTAMP(6),
                data_quality_score DOUBLE,
                source_file VARCHAR
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['product_info_financial_product']
            )
        """,
    )

    # Task 4: Загрузка и трансформация данных orders из Bronze в Silver
    transform_orders_to_silver = SQLExecuteQueryOperator(
        task_id='transform_orders_to_silver',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {CATALOG}.{SILVER_SCHEMA}.orders
            SELECT DISTINCT
                -- Идентификаторы
                CAST(order_id AS BIGINT) as order_id,
                CAST(order_book_id AS BIGINT) as order_book_id,
                TRIM(side) as side,
                
                -- Временные метки
                CAST(timestamp AS TIMESTAMP(6)) as timestamp,
                CAST(created_on AS TIMESTAMP(6)) as created_on,
                CAST(DATE(CAST(timestamp AS TIMESTAMP(6))) AS DATE) as partition_date,
                
                -- Количественные характеристики
                CAST(initial_qty AS DOUBLE) as initial_qty,
                CAST(created_with_qty AS DOUBLE) as created_with_qty,
                CAST(qty_on_end AS DOUBLE) as qty_on_end,
                CAST(filled_on_start AS DOUBLE) as filled_on_start,
                
                -- Статусы (конвертация строк в boolean)
                CASE 
                    WHEN LOWER(TRIM(deleted)) IN ('true', '1', 'yes') THEN true
                    WHEN LOWER(TRIM(deleted)) IN ('false', '0', 'no') THEN false
                    ELSE NULL
                END as deleted,
                CASE 
                    WHEN LOWER(TRIM(fully_executed)) IN ('true', '1', 'yes') THEN true
                    WHEN LOWER(TRIM(fully_executed)) IN ('false', '0', 'no') THEN false
                    ELSE NULL
                END as fully_executed,
                CASE 
                    WHEN LOWER(TRIM(partially_executed)) IN ('true', '1', 'yes') THEN true
                    WHEN LOWER(TRIM(partially_executed)) IN ('false', '0', 'no') THEN false
                    ELSE NULL
                END as partially_executed,
                
                -- Метрики времени (конвертация из наносекунд)
                CAST(existed_for AS DOUBLE) / 1e9 as order_lifetime_seconds,
                CAST(min_reaction_time AS DOUBLE) / 1e6 as min_reaction_time_ms,
                
                -- Метрики модификаций
                CAST(modify_count AS BIGINT) as modify_count,
                
                -- Расстояние от BBO
                TRY_CAST(distance_from_bbo_avg AS DOUBLE) as distance_from_bbo_avg,
                CAST(distance_from_bbo_max AS DOUBLE) as distance_from_bbo_max,
                CAST(distance_from_bbo_min AS DOUBLE) as distance_from_bbo_min,
                
                -- Ценовая разница от BBO
                TRY_CAST(price_dif_from_bbo_avg AS DOUBLE) as price_dif_from_bbo_avg,
                CAST(price_dif_from_bbo_max AS DOUBLE) as price_dif_from_bbo_max,
                CAST(price_dif_from_bbo_min AS DOUBLE) as price_dif_from_bbo_min,
                
                -- Приоритет в очереди
                TRY_CAST(priority_count_avg AS DOUBLE) as priority_count_avg,
                CAST(priority_count_max AS DOUBLE) as priority_count_max,
                CAST(priority_count_min AS DOUBLE) as priority_count_min,
                
                -- Изменения ценовых уровней
                TRY_CAST(price_level_change_avg AS DOUBLE) as price_level_change_avg,
                CAST(price_level_change_max AS DOUBLE) as price_level_change_max,
                CAST(price_level_change_min AS DOUBLE) as price_level_change_min,
                
                -- Изменения в тиках
                TRY_CAST(tick_count_price_level_change_avg AS DOUBLE) as tick_count_price_level_change_avg,
                CAST(tick_count_price_level_change_max AS DOUBLE) as tick_count_price_level_change_max,
                CAST(tick_count_price_level_change_min AS DOUBLE) as tick_count_price_level_change_min,
                
                -- Временные интервалы (конвертация из наносекунд в миллисекунды)
                TRY_CAST(time_passed_since_last_event_avg AS DOUBLE) / 1e6 as time_passed_since_last_event_avg,
                CAST(time_passed_since_last_event_max AS DOUBLE) / 1e6 as time_passed_since_last_event_max,
                CAST(time_passed_since_last_event_min AS DOUBLE) / 1e6 as time_passed_since_last_event_min,
                
                -- Вычисляемая метрика: execution_ratio
                CASE 
                    WHEN CAST(initial_qty AS DOUBLE) > 0 
                    THEN (CAST(initial_qty AS DOUBLE) - CAST(qty_on_end AS DOUBLE)) / CAST(initial_qty AS DOUBLE)
                    ELSE 0.0
                END as execution_ratio,
                
                -- Метаданные обработки
                CURRENT_TIMESTAMP as processed_at,
                
                -- Вычисление data_quality_score (процент заполненных критических полей)
                (
                    CAST((order_id IS NOT NULL) AS INT) +
                    CAST((order_book_id IS NOT NULL) AS INT) +
                    CAST((side IS NOT NULL) AS INT) +
                    CAST((timestamp IS NOT NULL) AS INT) +
                    CAST((initial_qty IS NOT NULL) AS INT) +
                    CAST((fully_executed IS NOT NULL) AS INT)
                ) / 6.0 as data_quality_score,
                
                'bronze_raw_orders' as source_file
                
            FROM {CATALOG}.{BRONZE_SCHEMA}.raw_orders
            WHERE 
                -- Фильтрация невалидных записей
                order_id IS NOT NULL
                AND order_book_id IS NOT NULL
                AND side IS NOT NULL
                AND timestamp IS NOT NULL
                AND initial_qty IS NOT NULL
                AND CAST(initial_qty AS DOUBLE) >= 0
        """,
    )

    # Task 5: Загрузка и трансформация данных product_info из Bronze в Silver
    transform_products_to_silver = SQLExecuteQueryOperator(
        task_id='transform_products_to_silver',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {CATALOG}.{SILVER_SCHEMA}.product_info
            SELECT DISTINCT
                -- Идентификаторы
                CAST(product_info_order_book_id AS BIGINT) as product_info_order_book_id,
                CAST(product_info_underlying_order_book_id AS BIGINT) as product_info_underlying_order_book_id,
                TRIM(product_family) as product_family,
                
                -- Информация о продукте
                TRIM(product_info_symbol) as product_info_symbol,
                TRIM(product_info_long_name) as product_info_long_name,
                TRIM(product_info_financial_product) as product_info_financial_product,
                TRIM(product_info_put_or_call) as product_info_put_or_call,
                
                -- Ценовые параметры
                CAST(product_info_strike_price AS DOUBLE) as product_info_strike_price,
                CAST(product_info_number_of_decimal_in_price AS INT) as product_info_number_of_decimal_in_price,
                CAST(product_info_number_of_decimals_in_strike_price AS INT) as product_info_number_of_decimals_in_strike_price,
                
                -- Даты (парсинг из YYYYMMDD формата)
                CASE 
                    WHEN product_info_expiration_date IS NOT NULL 
                    THEN DATE_PARSE(CAST(product_info_expiration_date AS VARCHAR), '%Y%m%d')
                    ELSE NULL
                END as product_info_expiration_date,
                CAST(product_info_timestamp AS TIMESTAMP(6)) as product_info_timestamp,
                
                -- Статистика
                CAST(product_info_number_of_legs AS INT) as product_info_number_of_legs,
                CAST(taker_count AS BIGINT) as taker_count,
                CAST(unique_id_count AS BIGINT) as unique_id_count,
                CAST(modify_count AS BIGINT) as modify_count,
                
                -- Объемы торговли
                CAST(ctag_volume AS DOUBLE) as ctag_volume,
                CAST(etag_volume AS DOUBLE) as etag_volume,
                CAST(ptag_volume AS DOUBLE) as ptag_volume,
                CAST(leg_volume AS DOUBLE) as leg_volume,
                CAST(occured_at_cross AS DOUBLE) as occured_at_cross,
                
                -- Tick sizes (первые 3 уровня)
                CAST(ticks_0_price_from AS DOUBLE) as ticks_0_price_from,
                CAST(ticks_0_price_to AS DOUBLE) as ticks_0_price_to,
                CAST(ticks_0_tick_size AS DOUBLE) as ticks_0_tick_size,
                CAST(ticks_0_timestamp AS TIMESTAMP(6)) as ticks_0_timestamp,
                
                CAST(ticks_1_price_from AS DOUBLE) as ticks_1_price_from,
                CAST(ticks_1_price_to AS DOUBLE) as ticks_1_price_to,
                CAST(ticks_1_tick_size AS DOUBLE) as ticks_1_tick_size,
                CAST(ticks_1_timestamp AS TIMESTAMP(6)) as ticks_1_timestamp,
                
                CAST(ticks_2_price_from AS DOUBLE) as ticks_2_price_from,
                CAST(ticks_2_price_to AS DOUBLE) as ticks_2_price_to,
                CAST(ticks_2_tick_size AS DOUBLE) as ticks_2_tick_size,
                CAST(ticks_2_timestamp AS TIMESTAMP(6)) as ticks_2_timestamp,
                
                -- Метаданные обработки
                CURRENT_TIMESTAMP as processed_at,
                
                -- Data quality score
                (
                    CAST((product_info_order_book_id IS NOT NULL) AS INT) +
                    CAST((product_info_symbol IS NOT NULL) AS INT) +
                    CAST((product_info_financial_product IS NOT NULL) AS INT) +
                    CAST((product_info_timestamp IS NOT NULL) AS INT)
                ) / 4.0 as data_quality_score,
                
                'bronze_raw_product_info' as source_file
                
            FROM {CATALOG}.{BRONZE_SCHEMA}.raw_product_info
            WHERE 
                product_info_order_book_id IS NOT NULL
                AND product_info_symbol IS NOT NULL
        """,
    )

    # Task 6: Создание view для аналитики
    create_analytics_view = SQLExecuteQueryOperator(
        task_id='create_analytics_view',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE OR REPLACE VIEW {CATALOG}.{SILVER_SCHEMA}.orders_with_product_info AS
            SELECT 
                o.*,
                p.product_info_symbol,
                p.product_info_financial_product,
                p.product_info_put_or_call,
                p.product_family,
                p.ticks_0_tick_size
            FROM {CATALOG}.{SILVER_SCHEMA}.orders o
            LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.product_info p
                ON o.order_book_id = p.product_info_order_book_id
        """,
    )

    # Определение зависимостей задач
    create_bronze_schema >> create_silver_schema
    create_silver_schema >> [drop_silver_orders_table, drop_silver_products_table]
    drop_silver_orders_table >> create_silver_orders_table >> transform_orders_to_silver
    drop_silver_products_table >> create_silver_products_table >> transform_products_to_silver
    [transform_orders_to_silver, transform_products_to_silver] >> create_analytics_view
