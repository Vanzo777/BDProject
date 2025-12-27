"""
DAG для загрузки сырых HFT данных из CSV файлов в Bronze слой
Использует PythonOperator для загрузки CSV через Pandas
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication

TRINO_CONN_ID = 'trino_default'
CATALOG = 'iceberg'
BRONZE_SCHEMA = 'bronze'

# MinIO/S3 настройки
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BUCKET = 'lakehouse'

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def load_csv_to_bronze(table_name: str, csv_filename: str, **context):
    """Загружает CSV из MinIO в Iceberg Bronze таблицу через Pandas"""
    import io
    from minio import Minio
    
    # Подключение к MinIO
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    # Чтение CSV из MinIO
    csv_object = minio_client.get_object(BUCKET, f'raw/{csv_filename}')
    csv_data = csv_object.read()
    csv_object.close()
    
    # Парсинг CSV с Pandas
    df = pd.read_csv(io.BytesIO(csv_data))
    
    # Конвертация всех колонок в VARCHAR
    df = df.astype(str)
    
    print(f"Loaded {len(df)} rows from {csv_filename}")
    
    # Подключение к Trino
    conn = connect(
        host='trino',
        port=8080,
        user='trino',
        catalog=CATALOG,
        schema=BRONZE_SCHEMA,
    )
    cursor = conn.cursor()
    
    # Batch insert (по 1000 строк)
    batch_size = 1000
    total_rows = len(df)
    
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i+batch_size]
        
        # Формирование VALUES для INSERT
        values_list = []
        for _, row in batch.iterrows():
            values = ', '.join([f"'{str(val).replace("'", "''")}'" for val in row])
            values_list.append(f"({values})")
        
        values_str = ', '.join(values_list)
        
        insert_sql = f"""
            INSERT INTO {CATALOG}.{BRONZE_SCHEMA}.{table_name}
            VALUES {values_str}
        """
        
        cursor.execute(insert_sql)
        print(f"Inserted batch {i // batch_size + 1}: rows {i} to {min(i + batch_size, total_rows)}")
    
    cursor.close()
    conn.close()
    
    print(f"Successfully loaded {total_rows} rows into {CATALOG}.{BRONZE_SCHEMA}.{table_name}")

with DAG(
    dag_id='load_raw_to_bronze',
    default_args=default_args,
    description='Загрузка сырых данных из CSV в Bronze слой через Python',
    schedule='@daily',
    start_date=datetime(2025, 12, 26),
    catchup=False,
    tags=['medallion', 'bronze', 'ingestion'],
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

    # Task 1: Создание таблицы raw_orders
    create_raw_orders_table = SQLExecuteQueryOperator(
        task_id='create_raw_orders_table',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.raw_orders;
            
            CREATE TABLE {CATALOG}.{BRONZE_SCHEMA}.raw_orders (
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

    # Task 2: Загрузка orders через Python
    load_orders = PythonOperator(
        task_id='load_orders_from_csv',
        python_callable=load_csv_to_bronze,
        op_kwargs={
            'table_name': 'raw_orders',
            'csv_filename': 'order_classification.csv'
        },
    )

    # Task 3: Создание таблицы raw_product_info
    create_raw_product_info_table = SQLExecuteQueryOperator(
        task_id='create_raw_product_info_table',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.raw_product_info;
            
            CREATE TABLE {CATALOG}.{BRONZE_SCHEMA}.raw_product_info (
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

    # Task 4: Загрузка products через Python
    load_products = PythonOperator(
        task_id='load_products_from_csv',
        python_callable=load_csv_to_bronze,
        op_kwargs={
            'table_name': 'raw_product_info',
            'csv_filename': 'product_info.csv'
        },
    )

    # Зависимости
    create_bronze_schema >> [create_raw_orders_table, create_raw_product_info_table]
    create_raw_orders_table >> load_orders
    create_raw_product_info_table >> load_products
