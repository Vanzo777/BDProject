"""
DAG для загрузки сырых HFT данных в Bronze слой
"""

from datetime import datetime, timedelta
from airflow import DAG
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
    description='Загрузка сырых данных в Bronze слой',
    schedule='@daily',
    start_date=datetime(2025, 12, 26),
    catchup=False,
    tags=['medallion', 'bronze', 'ingestion'],
) as dag:
    
    create_bronze_schema = SQLExecuteQueryOperator(
        task_id='create_bronze_schema',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}
            WITH (location = 's3a://lakehouse/bronze/')
        """,
    )

    # Создание таблицы raw_orders
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
                format = 'PARQUET'
            )
        """,
    )

    # Вставка тестовых данных в raw_orders
    insert_test_orders = SQLExecuteQueryOperator(
        task_id='insert_test_orders',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {CATALOG}.{BRONZE_SCHEMA}.raw_orders VALUES
            ('1001', '5001', 'BUY', '2025-12-26 10:00:00.000000', '2025-12-26 10:00:00.000000',
             '100', '100', '50', '0', 'false', 'false', 'true',
             '5000000000', '1000000', '2',
             '5.0', '10.0', '2.0',
             '0.5', '1.0', '0.1',
             '100', '200', '50',
             '1', '3', '0',
             '2', '5', '1',
             '500000000', '1000000000', '100000000'),
            ('1002', '5002', 'SELL', '2025-12-26 10:01:00.000000', '2025-12-26 10:01:00.000000',
             '200', '200', '100', '0', 'false', 'true', 'false',
             '3000000000', '2000000', '1',
             '3.0', '8.0', '1.0',
             '0.3', '0.8', '0.05',
             '150', '250', '75',
             '2', '4', '1',
             '3', '6', '2',
             '400000000', '800000000', '200000000')
        """,
    )

    # Создание таблицы raw_product_info
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
                format = 'PARQUET'
            )
        """,
    )

    # Вставка тестовых данных в raw_product_info
    insert_test_products = SQLExecuteQueryOperator(
        task_id='insert_test_products',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {CATALOG}.{BRONZE_SCHEMA}.raw_product_info VALUES
            ('5001', '4001', 'OPTIONS', 'SPY_CALL_450', 'SPY Call Option Strike 450', 'OPTION', 'CALL',
             '450.0', '2', '2', '20251231', '2025-12-26 10:00:00.000000', '1',
             '100', '50', '5',
             '1000.0', '2000.0', '1500.0', '500.0', '100.0',
             '0.0', '100.0', '0.01', '2025-12-26 10:00:00.000000',
             '100.0', '200.0', '0.05', '2025-12-26 10:00:00.000000',
             '200.0', '500.0', '0.10', '2025-12-26 10:00:00.000000'),
            ('5002', '4002', 'OPTIONS', 'SPY_PUT_440', 'SPY Put Option Strike 440', 'OPTION', 'PUT',
             '440.0', '2', '2', '20251231', '2025-12-26 10:01:00.000000', '1',
             '150', '75', '8',
             '1500.0', '2500.0', '2000.0', '800.0', '150.0',
             '0.0', '100.0', '0.01', '2025-12-26 10:01:00.000000',
             '100.0', '200.0', '0.05', '2025-12-26 10:01:00.000000',
             '200.0', '500.0', '0.10', '2025-12-26 10:01:00.000000')
        """,
    )

    # Зависимости
    create_bronze_schema >> [create_raw_orders_table, create_raw_product_info_table]
    create_raw_orders_table >> insert_test_orders
    create_raw_product_info_table >> insert_test_products
