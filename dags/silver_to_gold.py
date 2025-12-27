"""
DAG для создания Gold слоя - Data Marts для HFT Analytics
Medallion Architecture: Silver → Gold трансформация
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

TRINO_CONN_ID = 'trino_default'
CATALOG = 'iceberg'
SILVER_SCHEMA = 'silver'
GOLD_SCHEMA = 'gold'

# Флаг для пересоздания таблиц
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
    description='Создание Data Marts для HFT аналитики в Gold слое',
    schedule='@daily',
    start_date=datetime(2025, 12, 26),
    catchup=False,
    tags=['medallion', 'silver-to-gold', 'hft', 'analytics'],
) as dag:

    # Task 0: Создание Gold схемы
    create_gold_schema = SQLExecuteQueryOperator(
        task_id='create_gold_schema',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}
            WITH (location = 's3a://lakehouse/gold')
        """,
    )

    # ========================================
    # DATA MART 1: Order Speed & Execution Analytics
    # ========================================
    
    drop_speed_execution_dm = SQLExecuteQueryOperator(
        task_id='drop_speed_execution_dm',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {CATALOG}.{GOLD_SCHEMA}.order_speed_execution_stats
        """ if RECREATE_TABLES else "SELECT 1",
    )

    create_speed_execution_dm = SQLExecuteQueryOperator(
        task_id='create_speed_execution_dm',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.order_speed_execution_stats (
                speed_category VARCHAR,
                order_count BIGINT,
                avg_execution_ratio DOUBLE,
                execution_rate DOUBLE,
                deletion_rate DOUBLE,
                partially_executed_rate DOUBLE,
                avg_lifetime_sec DOUBLE,
                median_lifetime_sec DOUBLE,
                avg_reaction_time_ms DOUBLE,
                avg_modify_count DOUBLE,
                created_at TIMESTAMP(6)
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['speed_category']
            )
        """,
    )

    populate_speed_execution_dm = SQLExecuteQueryOperator(
        task_id='populate_speed_execution_dm',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {CATALOG}.{GOLD_SCHEMA}.order_speed_execution_stats
            WITH percentiles AS (
                SELECT
                    APPROX_PERCENTILE(min_reaction_time_ms, 0.25) as p25,
                    APPROX_PERCENTILE(min_reaction_time_ms, 0.50) as p50,
                    APPROX_PERCENTILE(min_reaction_time_ms, 0.75) as p75
                FROM {CATALOG}.{SILVER_SCHEMA}.orders
                WHERE min_reaction_time_ms IS NOT NULL
            )
            SELECT
                CASE 
                    WHEN o.min_reaction_time_ms < p.p25 THEN 'ultra_fast'
                    WHEN o.min_reaction_time_ms < p.p50 THEN 'fast'
                    WHEN o.min_reaction_time_ms < p.p75 THEN 'medium'
                    ELSE 'slow'
                END as speed_category,
                
                COUNT(*) as order_count,
                AVG(o.execution_ratio) as avg_execution_ratio,
                
                CAST(SUM(CASE WHEN o.fully_executed THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as execution_rate,
                CAST(SUM(CASE WHEN o.deleted THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as deletion_rate,
                CAST(SUM(CASE WHEN o.partially_executed THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as partially_executed_rate,
                
                AVG(o.order_lifetime_seconds) as avg_lifetime_sec,
                APPROX_PERCENTILE(o.order_lifetime_seconds, 0.5) as median_lifetime_sec,
                AVG(o.min_reaction_time_ms) as avg_reaction_time_ms,
                AVG(CAST(o.modify_count AS DOUBLE)) as avg_modify_count,
                
                CURRENT_TIMESTAMP as created_at
            FROM {CATALOG}.{SILVER_SCHEMA}.orders o
            CROSS JOIN percentiles p
            WHERE o.min_reaction_time_ms IS NOT NULL
            GROUP BY 
                CASE 
                    WHEN o.min_reaction_time_ms < p.p25 THEN 'ultra_fast'
                    WHEN o.min_reaction_time_ms < p.p50 THEN 'fast'
                    WHEN o.min_reaction_time_ms < p.p75 THEN 'medium'
                    ELSE 'slow'
                END
        """,
    )

    # ========================================
    # DATA MART 2: Product Performance Comparison
    # ========================================
    
    drop_product_metrics_dm = SQLExecuteQueryOperator(
        task_id='drop_product_metrics_dm',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {CATALOG}.{GOLD_SCHEMA}.product_execution_metrics
        """ if RECREATE_TABLES else "SELECT 1",
    )

    create_product_metrics_dm = SQLExecuteQueryOperator(
        task_id='create_product_metrics_dm',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.product_execution_metrics (
                product_info_financial_product VARCHAR,
                product_info_symbol VARCHAR,
                product_family VARCHAR,
                total_orders BIGINT,
                avg_execution_ratio DOUBLE,
                execution_rate DOUBLE,
                deletion_rate DOUBLE,
                avg_reaction_time_ms DOUBLE,
                avg_lifetime_sec DOUBLE,
                etag_volume DOUBLE,
                unique_id_count BIGINT,
                taker_count BIGINT,
                created_at TIMESTAMP(6)
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['product_info_financial_product']
            )
        """,
    )

    populate_product_metrics_dm = SQLExecuteQueryOperator(
        task_id='populate_product_metrics_dm',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {CATALOG}.{GOLD_SCHEMA}.product_execution_metrics
            SELECT
                p.product_info_financial_product,
                p.product_info_symbol,
                p.product_family,
                
                COUNT(DISTINCT o.order_id) as total_orders,
                AVG(o.execution_ratio) as avg_execution_ratio,
                
                CAST(SUM(CASE WHEN o.fully_executed THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) as execution_rate,
                CAST(SUM(CASE WHEN o.deleted THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) as deletion_rate,
                
                AVG(o.min_reaction_time_ms) as avg_reaction_time_ms,
                AVG(o.order_lifetime_seconds) as avg_lifetime_sec,
                
                MAX(p.etag_volume) as etag_volume,
                MAX(p.unique_id_count) as unique_id_count,
                MAX(p.taker_count) as taker_count,
                
                CURRENT_TIMESTAMP as created_at
                
            FROM {CATALOG}.{SILVER_SCHEMA}.orders o
            JOIN {CATALOG}.{SILVER_SCHEMA}.product_info p 
                ON o.order_book_id = p.product_info_order_book_id
            GROUP BY 
                p.product_info_financial_product,
                p.product_info_symbol,
                p.product_family
        """,
    )

    # ========================================
    # DATA MART 3: Volume vs Activity Correlation
    # ========================================
    
    drop_volume_activity_dm = SQLExecuteQueryOperator(
        task_id='drop_volume_activity_dm',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {CATALOG}.{GOLD_SCHEMA}.volume_activity_analysis
        """ if RECREATE_TABLES else "SELECT 1",
    )

    create_volume_activity_dm = SQLExecuteQueryOperator(
        task_id='create_volume_activity_dm',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.volume_activity_analysis (
                product_info_financial_product VARCHAR,
                product_family VARCHAR,
                total_volume DOUBLE,
                total_maker_orders BIGINT,
                avg_volume_per_product DOUBLE,
                avg_maker_orders_per_product DOUBLE,
                product_count BIGINT,
                volume_per_order_ratio DOUBLE,
                products_with_zero_execution BIGINT,
                products_with_execution BIGINT,
                zero_execution_rate DOUBLE,
                created_at TIMESTAMP(6)
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['product_info_financial_product']
            )
        """,
    )

    populate_volume_activity_dm = SQLExecuteQueryOperator(
        task_id='populate_volume_activity_dm',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {CATALOG}.{GOLD_SCHEMA}.volume_activity_analysis
            SELECT
                product_info_financial_product,
                product_family,
                
                SUM(etag_volume) as total_volume,
                SUM(unique_id_count) as total_maker_orders,
                AVG(etag_volume) as avg_volume_per_product,
                AVG(CAST(unique_id_count AS DOUBLE)) as avg_maker_orders_per_product,
                
                COUNT(DISTINCT product_info_order_book_id) as product_count,
                
                SUM(etag_volume) / NULLIF(SUM(unique_id_count), 0) as volume_per_order_ratio,
                
                SUM(CASE WHEN etag_volume = 0 THEN 1 ELSE 0 END) as products_with_zero_execution,
                SUM(CASE WHEN etag_volume > 0 THEN 1 ELSE 0 END) as products_with_execution,
                CAST(SUM(CASE WHEN etag_volume = 0 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as zero_execution_rate,
                
                CURRENT_TIMESTAMP as created_at
                
            FROM {CATALOG}.{SILVER_SCHEMA}.product_info
            GROUP BY product_info_financial_product, product_family
        """,
    )

    # ========================================
    # DATA MART 4: Options Strike & Expiry Analytics
    # ========================================
    
    drop_options_strike_dm = SQLExecuteQueryOperator(
        task_id='drop_options_strike_dm',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {CATALOG}.{GOLD_SCHEMA}.options_strike_expiry_stats
        """ if RECREATE_TABLES else "SELECT 1",
    )

    create_options_strike_dm = SQLExecuteQueryOperator(
        task_id='create_options_strike_dm',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.options_strike_expiry_stats (
                product_info_strike_price DOUBLE,
                product_info_expiration_date DATE,
                product_info_put_or_call VARCHAR,
                product_family VARCHAR,
                order_count BIGINT,
                maker_order_count BIGINT,
                avg_execution_ratio DOUBLE,
                execution_rate DOUBLE,
                etag_volume DOUBLE,
                zero_execution BOOLEAN,
                created_at TIMESTAMP(6)
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['product_info_put_or_call', 'product_info_expiration_date']
            )
        """,
    )

    populate_options_strike_dm = SQLExecuteQueryOperator(
        task_id='populate_options_strike_dm',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            INSERT INTO {CATALOG}.{GOLD_SCHEMA}.options_strike_expiry_stats
            SELECT
                p.product_info_strike_price,
                p.product_info_expiration_date,
                p.product_info_put_or_call,
                p.product_family,
                
                COUNT(DISTINCT o.order_id) as order_count,
                MAX(p.unique_id_count) as maker_order_count,
                
                AVG(o.execution_ratio) as avg_execution_ratio,
                CAST(SUM(CASE WHEN o.fully_executed THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) as execution_rate,
                
                MAX(p.etag_volume) as etag_volume,
                CASE WHEN MAX(p.etag_volume) = 0 THEN true ELSE false END as zero_execution,
                
                CURRENT_TIMESTAMP as created_at
                
            FROM {CATALOG}.{SILVER_SCHEMA}.product_info p
            LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.orders o 
                ON p.product_info_order_book_id = o.order_book_id
            WHERE p.product_info_financial_product = 'OPTION'
                AND p.product_family LIKE '%NK225%'
            GROUP BY 
                p.product_info_strike_price,
                p.product_info_expiration_date,
                p.product_info_put_or_call,
                p.product_family
        """,
    )

    # ========================================
    # VIEW: Unified Analytics Dashboard
    # ========================================
    
    create_unified_analytics_view = SQLExecuteQueryOperator(
        task_id='create_unified_analytics_view',
        conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.hft_analytics_dashboard AS
            SELECT
                'speed_execution' as metric_type,
                speed_category as dimension_1,
                NULL as dimension_2,
                order_count,
                execution_rate,
                avg_lifetime_sec as metric_value_1,
                avg_reaction_time_ms as metric_value_2
            FROM {CATALOG}.{GOLD_SCHEMA}.order_speed_execution_stats
            
            UNION ALL
            
            SELECT
                'product_performance' as metric_type,
                product_info_financial_product as dimension_1,
                product_family as dimension_2,
                total_orders as order_count,
                execution_rate,
                avg_lifetime_sec as metric_value_1,
                etag_volume as metric_value_2
            FROM {CATALOG}.{GOLD_SCHEMA}.product_execution_metrics
        """,
    )

    # Определение зависимостей
    create_gold_schema >> [drop_speed_execution_dm, drop_product_metrics_dm, drop_volume_activity_dm, drop_options_strike_dm]
    
    drop_speed_execution_dm >> create_speed_execution_dm >> populate_speed_execution_dm
    drop_product_metrics_dm >> create_product_metrics_dm >> populate_product_metrics_dm
    drop_volume_activity_dm >> create_volume_activity_dm >> populate_volume_activity_dm
    drop_options_strike_dm >> create_options_strike_dm >> populate_options_strike_dm
    
    [populate_speed_execution_dm, populate_product_metrics_dm, populate_volume_activity_dm, populate_options_strike_dm] >> create_unified_analytics_view
