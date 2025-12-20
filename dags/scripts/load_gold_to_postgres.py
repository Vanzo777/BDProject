"""Load data from Iceberg Gold layer to PostgreSQL via Trino."""
from airflow.operators.bash import BashOperator


def create_postgres_load_tasks(dag):
    """
    Create tasks to load Gold layer tables into PostgreSQL via Trino.
    
    Returns a list of BashOperator tasks that execute Trino SQL commands
    to copy data from iceberg.gold.* tables to analytics_postgres.analytics.* tables.
    """
    
    # Table mapping: gold table -> postgres table
    tables_to_load = [
        {
            'source': 'iceberg.gold.product_trading_metrics',
            'target': 'analytics_postgres.analytics.product_trading_metrics',
            'task_id': 'load_product_trading_metrics'
        },
        {
            'source': 'iceberg.gold.daily_trading_summary',
            'target': 'analytics_postgres.analytics.daily_trading_summary',
            'task_id': 'load_daily_trading_summary'
        }
    ]
    
    load_tasks = []
    
    for table_config in tables_to_load:
        # Extract schema and table name from target
        target_parts = table_config['target'].split('.')
        catalog = target_parts[0]
        schema = target_parts[1]
        table = target_parts[2]
        
        task = BashOperator(
            task_id=table_config['task_id'],
            bash_command=f"""
                docker exec trino trino --execute "
                    -- Create schema if not exists
                    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};
                    
                    -- Drop existing table
                    DROP TABLE IF EXISTS {table_config['target']};
                    
                    -- Create table and load data from Gold layer
                    CREATE TABLE {table_config['target']}
                    AS SELECT * FROM {table_config['source']};
                    
                    -- Show row count
                    SELECT COUNT(*) as row_count FROM {table_config['target']};
                "
            """,
            dag=dag,
        )
        load_tasks.append(task)
    
    # Chain tasks if multiple tables (execute sequentially)
    if len(load_tasks) > 1:
        for i in range(len(load_tasks) - 1):
            load_tasks[i] >> load_tasks[i + 1]
    
    return load_tasks[0] if load_tasks else None
