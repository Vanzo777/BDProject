"""
DAG для загрузки сырых HFT данных в Bronze слой через Polars
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

TRINO_CONN_ID = 'trino_default'
CATALOG = 'iceberg'
BRONZE_SCHEMA = 'bronze'

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='load_raw_to_bronze',
    default_args=default_args,
    description='Загрузка сырых CSV данных в Bronze слой через Polars',
    schedule='@daily',
    start_date=datetime(2025, 12, 26),
    catchup=False,
    tags=['medallion', 'bronze', 'ingestion', 'polars'],
) as dag:
    

    # ===== ШАГ 1: Конвертация CSV -> Parquet через Polars =====
    csv_to_parquet = BashOperator(
        task_id='csv_to_parquet',
        bash_command='python /opt/airflow/dags/scripts/csv_to_parquet_bronze.py',
    )

    # ===== ШАГ 2: Создание таблицы raw_orders =====
    create_raw_orders_table = SQLExecuteQueryOperator(
        task_id='create_raw_orders_table',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.raw_orders (
                order_id VARCHAR,
                order_book_id VARCHAR,
                side VARCHAR,
                timestamp VARCHAR,
                created_on VARCHAR,
                initial_qty VARCHAR,
                created_with_qty VARCHAR,
                qty_on_end VARCHAR,
                filled_on_start VARCHAR,
                deleted VARCHAR,
                fully_executed VARCHAR,
                partially_executed VARCHAR,
                existed_for VARCHAR,
                min_reaction_time VARCHAR,
                modify_count VARCHAR,
                distance_from_bbo_avg VARCHAR,
                distance_from_bbo_max VARCHAR,
                distance_from_bbo_min VARCHAR,
                price_dif_from_bbo_avg VARCHAR,
                price_dif_from_bbo_max VARCHAR,
                price_dif_from_bbo_min VARCHAR,
                priority_count_avg VARCHAR,
                priority_count_max VARCHAR,
                priority_count_min VARCHAR,
                price_level_change_avg VARCHAR,
                price_level_change_max VARCHAR,
                price_level_change_min VARCHAR,
                tick_count_price_level_change_avg VARCHAR,
                tick_count_price_level_change_max VARCHAR,
                tick_count_price_level_change_min VARCHAR,
                time_passed_since_last_event_avg VARCHAR,
                time_passed_since_last_event_max VARCHAR,
                time_passed_since_last_event_min VARCHAR
            )
            WITH (
                format = 'PARQUET',
                location = 's3a://lakehouse/bronze/order_classification/'
            )
        """,
    )

    # ===== ШАГ 3: Загрузка данных из Parquet в Iceberg =====
    load_orders_from_parquet = SQLExecuteQueryOperator(
        task_id='load_orders_from_parquet',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {CATALOG}.{BRONZE_SCHEMA}.raw_orders
            SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.raw_orders_external
        """,
    )

    # ===== ШАГ 4: Создание таблицы raw_product_info =====
    create_raw_product_info_table = SQLExecuteQueryOperator(
        task_id='create_raw_product_info_table',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.raw_product_info (
                product_info_order_book_id VARCHAR,
                product_info_underlying_order_book_id VARCHAR,
                product_family VARCHAR,
                product_info_symbol VARCHAR,
                product_info_long_name VARCHAR,
                product_info_financial_product VARCHAR,
                product_info_put_or_call VARCHAR,
                product_info_strike_price VARCHAR,
                product_info_number_of_decimal_in_price VARCHAR,
                product_info_number_of_decimals_in_strike_price VARCHAR,
                product_info_expiration_date VARCHAR,
                product_info_timestamp VARCHAR,
                product_info_number_of_legs VARCHAR,
                taker_count VARCHAR,
                unique_id_count VARCHAR,
                modify_count VARCHAR,
                ctag_volume VARCHAR,
                etag_volume VARCHAR,
                ptag_volume VARCHAR,
                leg_volume VARCHAR,
                occured_at_cross VARCHAR,
                ticks_0_price_from VARCHAR,
                ticks_0_price_to VARCHAR,
                ticks_0_tick_size VARCHAR,
                ticks_0_timestamp VARCHAR,
                ticks_1_price_from VARCHAR,
                ticks_1_price_to VARCHAR,
                ticks_1_tick_size VARCHAR,
                ticks_1_timestamp VARCHAR,
                ticks_2_price_from VARCHAR,
                ticks_2_price_to VARCHAR,
                ticks_2_tick_size VARCHAR,
                ticks_2_timestamp VARCHAR
            )
            WITH (
                format = 'PARQUET',
                location = 's3a://lakehouse/bronze/product_info/'
            )
        """,
    )

    # ===== ШАГ 5: Загрузка product_info из Parquet в Iceberg =====
    load_products_from_parquet = SQLExecuteQueryOperator(
        task_id='load_products_from_parquet',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {CATALOG}.{BRONZE_SCHEMA}.raw_product_info
            SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.raw_product_info_external
        """,
    )

    # ===== ЗАВИСИМОСТИ =====
    # Сначала конвертируем CSV -> Parquet
    # Затем создаём таблицы и загружаем данные
    csv_to_parquet >> create_raw_orders_table >> load_orders_from_parquet
    csv_to_parquet >> create_raw_product_info_table >> load_products_from_parquet
