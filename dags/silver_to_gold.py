"""
DAG для создания Gold слоя - Денормализованные Data Marts для AI Agent
Medallion Architecture: Silver → Gold трансформация
Минимальная агрегация, максимальная детализация
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

TRINO_CONN_ID = 'trino_default'
CATALOG = 'iceberg'
SILVER_SCHEMA = 'silver'
GOLD_SCHEMA = 'gold'
POSTGRES_CATALOG = 'mock_postgres'

RECREATE_TABLES = True

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='silver_to_gold_hft_analytics',
    default_args=default_args,
    description='Создание денормализованных полных Data Marts для AI-агента',
    schedule='@daily',
    start_date=datetime(2025, 12, 26),
    catchup=False,
    tags=['medallion', 'silver-to-gold', 'hft', 'analytics', 'postgres-sync'],
) as dag:

    # Task 0: Создание Gold схемы в Iceberg
    create_gold_schema = SQLExecuteQueryOperator(
        task_id='create_gold_schema',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}
            WITH (location = 's3a://lakehouse/gold')
        """,
    )

    # Task 0.1: Создание схемы в PostgreSQL
    create_postgres_schema = SQLExecuteQueryOperator(
        task_id='create_postgres_schema',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {POSTGRES_CATALOG}.gold
        """,
    )

    # ========================================
    # GOLD TABLE 1: Enriched Orders (полная детализация всех ордеров)
    # ========================================
    
    drop_enriched_orders = SQLExecuteQueryOperator(
        task_id='drop_enriched_orders',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {CATALOG}.{GOLD_SCHEMA}.enriched_orders
        """ if RECREATE_TABLES else "SELECT 1",
    )

    create_enriched_orders = SQLExecuteQueryOperator(
        task_id='create_enriched_orders',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.enriched_orders (
                -- Идентификаторы ордера
                order_id BIGINT,
                order_book_id BIGINT,
                side VARCHAR,
                
                -- Временные метки
                timestamp TIMESTAMP(6),
                created_on TIMESTAMP(6),
                partition_date DATE,
                
                -- Количественные характеристики ордера
                initial_qty DOUBLE,
                created_with_qty DOUBLE,
                qty_on_end DOUBLE,
                filled_on_start DOUBLE,
                
                -- Статусы исполнения ордера
                deleted BOOLEAN,
                fully_executed BOOLEAN,
                partially_executed BOOLEAN,
                
                -- Метрики времени жизни ордера
                order_lifetime_seconds DOUBLE,
                min_reaction_time_ms DOUBLE,
                
                -- Метрики модификаций ордера
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
                
                -- Временные интервалы между событиями (мс)
                time_passed_since_last_event_avg DOUBLE,
                time_passed_since_last_event_max DOUBLE,
                time_passed_since_last_event_min DOUBLE,
                
                -- Вычисляемая метрика исполнения
                execution_ratio DOUBLE,
                
                -- === ENRICHMENT: Product Information ===
                product_info_symbol VARCHAR,
                product_info_long_name VARCHAR,
                product_info_financial_product VARCHAR,
                product_info_put_or_call VARCHAR,
                product_family VARCHAR,
                product_info_strike_price DOUBLE,
                product_info_number_of_decimal_in_price INT,
                product_info_number_of_decimals_in_strike_price INT,
                product_info_expiration_date DATE,
                product_info_timestamp TIMESTAMP(6),
                product_info_number_of_legs INT,
                
                -- === ENRICHMENT: Product Trading Metrics ===
                product_taker_count BIGINT,
                product_unique_id_count BIGINT,
                product_modify_count BIGINT,
                
                -- Объемы торгов продукта
                product_ctag_volume DOUBLE,
                product_etag_volume DOUBLE,
                product_ptag_volume DOUBLE,
                product_leg_volume DOUBLE,
                product_occured_at_cross DOUBLE,
                
                -- Tick sizes для продукта (3 уровня)
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
                
                -- Метаданные качества
                processed_at TIMESTAMP(6),
                order_data_quality_score DOUBLE,
                product_data_quality_score DOUBLE,
                combined_data_quality_score DOUBLE,
                source_file VARCHAR
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['partition_date']
            )
        """,
    )

    populate_enriched_orders = SQLExecuteQueryOperator(
        task_id='populate_enriched_orders',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {CATALOG}.{GOLD_SCHEMA}.enriched_orders
            SELECT 
                -- Order base data
                o.order_id,
                o.order_book_id,
                o.side,
                o.timestamp,
                o.created_on,
                o.partition_date,
                o.initial_qty,
                o.created_with_qty,
                o.qty_on_end,
                o.filled_on_start,
                o.deleted,
                o.fully_executed,
                o.partially_executed,
                o.order_lifetime_seconds,
                o.min_reaction_time_ms,
                o.modify_count,
                o.distance_from_bbo_avg,
                o.distance_from_bbo_max,
                o.distance_from_bbo_min,
                o.price_dif_from_bbo_avg,
                o.price_dif_from_bbo_max,
                o.price_dif_from_bbo_min,
                o.priority_count_avg,
                o.priority_count_max,
                o.priority_count_min,
                o.price_level_change_avg,
                o.price_level_change_max,
                o.price_level_change_min,
                o.tick_count_price_level_change_avg,
                o.tick_count_price_level_change_max,
                o.tick_count_price_level_change_min,
                o.time_passed_since_last_event_avg,
                o.time_passed_since_last_event_max,
                o.time_passed_since_last_event_min,
                o.execution_ratio,
                
                -- Product enrichment (denormalized)
                p.product_info_symbol,
                p.product_info_long_name,
                p.product_info_financial_product,
                p.product_info_put_or_call,
                p.product_family,
                p.product_info_strike_price,
                p.product_info_number_of_decimal_in_price,
                p.product_info_number_of_decimals_in_strike_price,
                p.product_info_expiration_date,
                p.product_info_timestamp,
                p.product_info_number_of_legs,
                p.taker_count,
                p.unique_id_count,
                p.modify_count as product_modify_count,
                p.ctag_volume,
                p.etag_volume,
                p.ptag_volume,
                p.leg_volume,
                p.occured_at_cross,
                p.ticks_0_price_from,
                p.ticks_0_price_to,
                p.ticks_0_tick_size,
                p.ticks_0_timestamp,
                p.ticks_1_price_from,
                p.ticks_1_price_to,
                p.ticks_1_tick_size,
                p.ticks_1_timestamp,
                p.ticks_2_price_from,
                p.ticks_2_price_to,
                p.ticks_2_tick_size,
                p.ticks_2_timestamp,
                
                -- Metadata
                CURRENT_TIMESTAMP as processed_at,
                o.data_quality_score as order_data_quality_score,
                p.data_quality_score as product_data_quality_score,
                (o.data_quality_score + p.data_quality_score) / 2.0 as combined_data_quality_score,
                COALESCE(o.source_file, p.source_file) as source_file
                
            FROM {CATALOG}.{SILVER_SCHEMA}.orders o
            LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.product_info p 
                ON o.order_book_id = p.product_info_order_book_id
        """,
    )

    # PostgreSQL: Enriched Orders (БЕЗ PRIMARY KEY!)
    create_postgres_enriched_orders = SQLExecuteQueryOperator(
        task_id='create_postgres_enriched_orders',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {POSTGRES_CATALOG}.gold.enriched_orders;
            
            CREATE TABLE {POSTGRES_CATALOG}.gold.enriched_orders (
                order_id BIGINT,
                order_book_id BIGINT,
                side VARCHAR(10),
                timestamp TIMESTAMP,
                created_on TIMESTAMP,
                partition_date DATE,
                initial_qty DOUBLE,
                created_with_qty DOUBLE,
                qty_on_end DOUBLE,
                filled_on_start DOUBLE,
                deleted BOOLEAN,
                fully_executed BOOLEAN,
                partially_executed BOOLEAN,
                order_lifetime_seconds DOUBLE,
                min_reaction_time_ms DOUBLE,
                modify_count BIGINT,
                distance_from_bbo_avg DOUBLE,
                distance_from_bbo_max DOUBLE,
                distance_from_bbo_min DOUBLE,
                price_dif_from_bbo_avg DOUBLE,
                price_dif_from_bbo_max DOUBLE,
                price_dif_from_bbo_min DOUBLE,
                priority_count_avg DOUBLE,
                priority_count_max DOUBLE,
                priority_count_min DOUBLE,
                price_level_change_avg DOUBLE,
                price_level_change_max DOUBLE,
                price_level_change_min DOUBLE,
                tick_count_price_level_change_avg DOUBLE,
                tick_count_price_level_change_max DOUBLE,
                tick_count_price_level_change_min DOUBLE,
                time_passed_since_last_event_avg DOUBLE,
                time_passed_since_last_event_max DOUBLE,
                time_passed_since_last_event_min DOUBLE,
                execution_ratio DOUBLE,
                product_info_symbol VARCHAR(100),
                product_info_long_name VARCHAR(255),
                product_info_financial_product VARCHAR(50),
                product_info_put_or_call VARCHAR(10),
                product_family VARCHAR(100),
                product_info_strike_price DOUBLE,
                product_info_number_of_decimal_in_price INTEGER,
                product_info_number_of_decimals_in_strike_price INTEGER,
                product_info_expiration_date DATE,
                product_info_timestamp TIMESTAMP,
                product_info_number_of_legs INTEGER,
                product_taker_count BIGINT,
                product_unique_id_count BIGINT,
                product_modify_count BIGINT,
                product_ctag_volume DOUBLE,
                product_etag_volume DOUBLE,
                product_ptag_volume DOUBLE,
                product_leg_volume DOUBLE,
                product_occured_at_cross DOUBLE,
                ticks_0_price_from DOUBLE,
                ticks_0_price_to DOUBLE,
                ticks_0_tick_size DOUBLE,
                ticks_0_timestamp TIMESTAMP,
                ticks_1_price_from DOUBLE,
                ticks_1_price_to DOUBLE,
                ticks_1_tick_size DOUBLE,
                ticks_1_timestamp TIMESTAMP,
                ticks_2_price_from DOUBLE,
                ticks_2_price_to DOUBLE,
                ticks_2_tick_size DOUBLE,
                ticks_2_timestamp TIMESTAMP,
                processed_at TIMESTAMP,
                order_data_quality_score DOUBLE,
                product_data_quality_score DOUBLE,
                combined_data_quality_score DOUBLE,
                source_file VARCHAR(255)
            )
        """ if RECREATE_TABLES else f"""
            CREATE TABLE IF NOT EXISTS {POSTGRES_CATALOG}.gold.enriched_orders (
                order_id BIGINT,
                order_book_id BIGINT,
                side VARCHAR(10),
                timestamp TIMESTAMP,
                created_on TIMESTAMP,
                partition_date DATE,
                initial_qty DOUBLE,
                created_with_qty DOUBLE,
                qty_on_end DOUBLE,
                filled_on_start DOUBLE,
                deleted BOOLEAN,
                fully_executed BOOLEAN,
                partially_executed BOOLEAN,
                order_lifetime_seconds DOUBLE,
                min_reaction_time_ms DOUBLE,
                modify_count BIGINT,
                distance_from_bbo_avg DOUBLE,
                distance_from_bbo_max DOUBLE,
                distance_from_bbo_min DOUBLE,
                price_dif_from_bbo_avg DOUBLE,
                price_dif_from_bbo_max DOUBLE,
                price_dif_from_bbo_min DOUBLE,
                priority_count_avg DOUBLE,
                priority_count_max DOUBLE,
                priority_count_min DOUBLE,
                price_level_change_avg DOUBLE,
                price_level_change_max DOUBLE,
                price_level_change_min DOUBLE,
                tick_count_price_level_change_avg DOUBLE,
                tick_count_price_level_change_max DOUBLE,
                tick_count_price_level_change_min DOUBLE,
                time_passed_since_last_event_avg DOUBLE,
                time_passed_since_last_event_max DOUBLE,
                time_passed_since_last_event_min DOUBLE,
                execution_ratio DOUBLE,
                product_info_symbol VARCHAR(100),
                product_info_long_name VARCHAR(255),
                product_info_financial_product VARCHAR(50),
                product_info_put_or_call VARCHAR(10),
                product_family VARCHAR(100),
                product_info_strike_price DOUBLE,
                product_info_number_of_decimal_in_price INTEGER,
                product_info_number_of_decimals_in_strike_price INTEGER,
                product_info_expiration_date DATE,
                product_info_timestamp TIMESTAMP,
                product_info_number_of_legs INTEGER,
                product_taker_count BIGINT,
                product_unique_id_count BIGINT,
                product_modify_count BIGINT,
                product_ctag_volume DOUBLE,
                product_etag_volume DOUBLE,
                product_ptag_volume DOUBLE,
                product_leg_volume DOUBLE,
                product_occured_at_cross DOUBLE,
                ticks_0_price_from DOUBLE,
                ticks_0_price_to DOUBLE,
                ticks_0_tick_size DOUBLE,
                ticks_0_timestamp TIMESTAMP,
                ticks_1_price_from DOUBLE,
                ticks_1_price_to DOUBLE,
                ticks_1_tick_size DOUBLE,
                ticks_1_timestamp TIMESTAMP,
                ticks_2_price_from DOUBLE,
                ticks_2_price_to DOUBLE,
                ticks_2_tick_size DOUBLE,
                ticks_2_timestamp TIMESTAMP,
                processed_at TIMESTAMP,
                order_data_quality_score DOUBLE,
                product_data_quality_score DOUBLE,
                combined_data_quality_score DOUBLE,
                source_file VARCHAR(255)
            )
        """,
    )

    sync_enriched_orders_to_postgres = SQLExecuteQueryOperator(
        task_id='sync_enriched_orders_to_postgres',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {POSTGRES_CATALOG}.gold.enriched_orders
            SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.enriched_orders
        """,
    )

    # ========================================
    # GOLD TABLE 2: Product Catalog (справочник всех продуктов)
    # ========================================
    
    drop_product_catalog = SQLExecuteQueryOperator(
        task_id='drop_product_catalog',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {CATALOG}.{GOLD_SCHEMA}.product_catalog
        """ if RECREATE_TABLES else "SELECT 1",
    )

    create_product_catalog = SQLExecuteQueryOperator(
        task_id='create_product_catalog',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.product_catalog (
                product_info_order_book_id BIGINT,
                product_info_underlying_order_book_id BIGINT,
                product_family VARCHAR,
                product_info_symbol VARCHAR,
                product_info_long_name VARCHAR,
                product_info_financial_product VARCHAR,
                product_info_put_or_call VARCHAR,
                product_info_strike_price DOUBLE,
                product_info_number_of_decimal_in_price INT,
                product_info_number_of_decimals_in_strike_price INT,
                product_info_expiration_date DATE,
                product_info_timestamp TIMESTAMP(6),
                product_info_number_of_legs INT,
                taker_count BIGINT,
                unique_id_count BIGINT,
                modify_count BIGINT,
                ctag_volume DOUBLE,
                etag_volume DOUBLE,
                ptag_volume DOUBLE,
                leg_volume DOUBLE,
                occured_at_cross DOUBLE,
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

    populate_product_catalog = SQLExecuteQueryOperator(
        task_id='populate_product_catalog',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {CATALOG}.{GOLD_SCHEMA}.product_catalog
            SELECT 
                product_info_order_book_id,
                product_info_underlying_order_book_id,
                product_family,
                product_info_symbol,
                product_info_long_name,
                product_info_financial_product,
                product_info_put_or_call,
                product_info_strike_price,
                product_info_number_of_decimal_in_price,
                product_info_number_of_decimals_in_strike_price,
                product_info_expiration_date,
                product_info_timestamp,
                product_info_number_of_legs,
                taker_count,
                unique_id_count,
                modify_count,
                ctag_volume,
                etag_volume,
                ptag_volume,
                leg_volume,
                occured_at_cross,
                ticks_0_price_from,
                ticks_0_price_to,
                ticks_0_tick_size,
                ticks_0_timestamp,
                ticks_1_price_from,
                ticks_1_price_to,
                ticks_1_tick_size,
                ticks_1_timestamp,
                ticks_2_price_from,
                ticks_2_price_to,
                ticks_2_tick_size,
                ticks_2_timestamp,
                processed_at,
                data_quality_score,
                source_file
            FROM {CATALOG}.{SILVER_SCHEMA}.product_info
        """,
    )

    # PostgreSQL: Product Catalog (БЕЗ PRIMARY KEY!)
    create_postgres_product_catalog = SQLExecuteQueryOperator(
        task_id='create_postgres_product_catalog',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {POSTGRES_CATALOG}.gold.product_catalog;
            
            CREATE TABLE {POSTGRES_CATALOG}.gold.product_catalog (
                product_info_order_book_id BIGINT,
                product_info_underlying_order_book_id BIGINT,
                product_family VARCHAR(100),
                product_info_symbol VARCHAR(100),
                product_info_long_name VARCHAR(255),
                product_info_financial_product VARCHAR(50),
                product_info_put_or_call VARCHAR(10),
                product_info_strike_price DOUBLE,
                product_info_number_of_decimal_in_price INTEGER,
                product_info_number_of_decimals_in_strike_price INTEGER,
                product_info_expiration_date DATE,
                product_info_timestamp TIMESTAMP,
                product_info_number_of_legs INTEGER,
                taker_count BIGINT,
                unique_id_count BIGINT,
                modify_count BIGINT,
                ctag_volume DOUBLE,
                etag_volume DOUBLE,
                ptag_volume DOUBLE,
                leg_volume DOUBLE,
                occured_at_cross DOUBLE,
                ticks_0_price_from DOUBLE,
                ticks_0_price_to DOUBLE,
                ticks_0_tick_size DOUBLE,
                ticks_0_timestamp TIMESTAMP,
                ticks_1_price_from DOUBLE,
                ticks_1_price_to DOUBLE,
                ticks_1_tick_size DOUBLE,
                ticks_1_timestamp TIMESTAMP,
                ticks_2_price_from DOUBLE,
                ticks_2_price_to DOUBLE,
                ticks_2_tick_size DOUBLE,
                ticks_2_timestamp TIMESTAMP,
                processed_at TIMESTAMP,
                data_quality_score DOUBLE,
                source_file VARCHAR(255)
            )
        """ if RECREATE_TABLES else f"""
            CREATE TABLE IF NOT EXISTS {POSTGRES_CATALOG}.gold.product_catalog (
                product_info_order_book_id BIGINT,
                product_info_underlying_order_book_id BIGINT,
                product_family VARCHAR(100),
                product_info_symbol VARCHAR(100),
                product_info_long_name VARCHAR(255),
                product_info_financial_product VARCHAR(50),
                product_info_put_or_call VARCHAR(10),
                product_info_strike_price DOUBLE,
                product_info_number_of_decimal_in_price INTEGER,
                product_info_number_of_decimals_in_strike_price INTEGER,
                product_info_expiration_date DATE,
                product_info_timestamp TIMESTAMP,
                product_info_number_of_legs INTEGER,
                taker_count BIGINT,
                unique_id_count BIGINT,
                modify_count BIGINT,
                ctag_volume DOUBLE,
                etag_volume DOUBLE,
                ptag_volume DOUBLE,
                leg_volume DOUBLE,
                occured_at_cross DOUBLE,
                ticks_0_price_from DOUBLE,
                ticks_0_price_to DOUBLE,
                ticks_0_tick_size DOUBLE,
                ticks_0_timestamp TIMESTAMP,
                ticks_1_price_from DOUBLE,
                ticks_1_price_to DOUBLE,
                ticks_1_tick_size DOUBLE,
                ticks_1_timestamp TIMESTAMP,
                ticks_2_price_from DOUBLE,
                ticks_2_price_to DOUBLE,
                ticks_2_tick_size DOUBLE,
                ticks_2_timestamp TIMESTAMP,
                processed_at TIMESTAMP,
                data_quality_score DOUBLE,
                source_file VARCHAR(255)
            )
        """,
    )

    sync_product_catalog_to_postgres = SQLExecuteQueryOperator(
        task_id='sync_product_catalog_to_postgres',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {POSTGRES_CATALOG}.gold.product_catalog
            SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.product_catalog
        """,
    )

    # ========================================
    # Определение зависимостей
    # ========================================
    
    create_gold_schema >> drop_enriched_orders
    create_gold_schema >> drop_product_catalog
    
    create_postgres_schema >> create_postgres_enriched_orders
    create_postgres_schema >> create_postgres_product_catalog
    
    # Enriched Orders flow
    drop_enriched_orders >> create_enriched_orders >> populate_enriched_orders
    create_postgres_enriched_orders >> sync_enriched_orders_to_postgres
    populate_enriched_orders >> sync_enriched_orders_to_postgres
    
    # Product Catalog flow
    drop_product_catalog >> create_product_catalog >> populate_product_catalog
    create_postgres_product_catalog >> sync_product_catalog_to_postgres
    populate_product_catalog >> sync_product_catalog_to_postgres
