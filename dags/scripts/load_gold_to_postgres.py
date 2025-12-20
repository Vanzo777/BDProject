from trino.dbapi import connect
from trino.auth import BasicAuthentication
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_table_to_postgres(source_schema: str, source_table: str, 
                          target_schema: str, target_table: str):
    """
    Загружает данные из Iceberg (gold) в PostgreSQL через Trino
    """
    conn = connect(
        host='trino',
        port=8080,
        user='trino',
        catalog='iceberg',
        schema=source_schema
    )
    
    cursor = conn.cursor()
    
    try:
        # 1. Создаем схему в PostgreSQL если не существует
        logger.info(f"Создание схемы {target_schema} в PostgreSQL")
        cursor.execute(f"""
            CREATE SCHEMA IF NOT EXISTS analytics_postgres.{target_schema}
        """)
        
        # 2. Удаляем старую таблицу если существует (для полной перезагрузки)
        logger.info(f"Удаление старой таблицы {target_schema}.{target_table}")
        cursor.execute(f"""
            DROP TABLE IF EXISTS analytics_postgres.{target_schema}.{target_table}
        """)
        
        # 3. Создаем таблицу и загружаем данные за один запрос (CREATE TABLE AS SELECT)
        logger.info(f"Загрузка данных из iceberg.{source_schema}.{source_table} в PostgreSQL")
        cursor.execute(f"""
            CREATE TABLE analytics_postgres.{target_schema}.{target_table}
            AS SELECT * FROM iceberg.{source_schema}.{source_table}
        """)
        
        # 4. Получаем количество загруженных строк
        cursor.execute(f"""
            SELECT COUNT(*) FROM analytics_postgres.{target_schema}.{target_table}
        """)
        row_count = cursor.fetchone()[0]
        
        logger.info(f"Успешно загружено {row_count} строк в {target_schema}.{target_table}")
        
        return row_count
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке таблицы: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Пример использования
    load_table_to_postgres(
        source_schema='gold_dm',
        source_table='your_table_name',
        target_schema='analytics',
        target_table='your_table_name'
    )
