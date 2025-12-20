from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, taskgroup
from tasks.bronze_tasks import read_minio_to_bronze
from tasks.silver_tasks import bronze_to_silver  
from tasks.gold_tasks import silver_to_gold
from tasks.postgres_tasks import load_gold_to_postgres

@dag(
    dag_id='data_lakehouse_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 12, 20),
    catchup=False,
    tags=['medallion', 'minio', 'postgres', 'spark'],
    max_active_runs=1
)
def data_pipeline():
    
    @taskgroup(group_id='bronze_layer')
    def bronze_group():
        raw_data = read_minio_to_bronze()
        return raw_data
    
    @taskgroup(group_id='silver_layer')
    def silver_group(bronze_df):
        silver_data = bronze_to_silver(bronze_df)
        return silver_data
    
    @taskgroup(group_id='gold_layer')
    def gold_group(silver_df):
        sales_summary = silver_to_gold(silver_df, 'sales')
        customer_stats = silver_to_gold(silver_df, 'customers') 
        product_perf = silver_to_gold(silver_df, 'products')
        return [sales_summary, customer_stats, product_perf]
    
    @taskgroup(group_id='postgres_load')
    def postgres_group(gold_tables):
        load_gold_to_postgres(gold_tables)
    
    bronze_df = bronze_group()
    silver_df = silver_group(bronze_df)
    gold_tables = gold_group(silver_df)
    postgres_group(gold_tables)

data_pipeline()
