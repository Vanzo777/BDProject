"""
Database module для SQL AI Agent.

Предоставляет абстракцию для работы с PostgreSQL:
- Подключение к БД
- Выполнение SQL запросов
- Получение схемы таблиц
- Обработка ошибок
"""

from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy.engine import Engine, Result
from sqlalchemy.exc import SQLAlchemyError, OperationalError, ProgrammingError
from sqlalchemy.pool import QueuePool
import logging
from contextlib import contextmanager

from agent.config import settings


# Настройка логирования
logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Ошибка подключения к базе данных"""
    pass


class DatabaseQueryError(Exception):
    """Ошибка выполнения SQL запроса"""
    pass


class Database:
    """
    Класс для работы с PostgreSQL базой данных.
    
    Предоставляет методы для:
    - Подключения к БД
    - Выполнения SQL запросов
    - Получения метаданных (схемы таблиц)
    - Безопасного управления соединениями
    """
    
    def __init__(
        self,
        connection_uri: Optional[str] = None,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        echo: bool = False
    ):
        """
        Инициализация подключения к базе данных.
        
        Args:
            connection_uri: URI подключения (если None, берётся из settings)
            pool_size: Размер пула соединений
            max_overflow: Максимальное количество дополнительных соединений
            pool_timeout: Таймаут ожидания соединения из пула
            echo: Логировать ли SQL запросы SQLAlchemy
        """
        self.connection_uri = connection_uri or settings.get_database_uri()
        self.schema = settings.DB_SCHEMA
        
        # Создаём engine с пулом соединений
        self.engine = create_engine(
            self.connection_uri,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            echo=echo or (settings.LOG_LEVEL == "DEBUG"),
            connect_args={
                "options": f"-c search_path={self.schema}",
                "connect_timeout": 10
            }
        )
        
        self.metadata = MetaData(schema=self.schema)
        
        logger.info(f"Database initialized: {settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}")
    
    def test_connection(self) -> bool:
        """
        Проверяет подключение к базе данных.
        
        Returns:
            True если подключение успешно, False иначе
        
        Raises:
            DatabaseConnectionError: Если не удалось подключиться
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("✅ Database connection successful")
            return True
        
        except OperationalError as e:
            error_msg = f"Failed to connect to database: {str(e)}"
            logger.error(f"❌ {error_msg}")
            raise DatabaseConnectionError(error_msg) from e
        
        except Exception as e:
            error_msg = f"Unexpected error during connection test: {str(e)}"
            logger.error(f"❌ {error_msg}")
            raise DatabaseConnectionError(error_msg) from e
    
    @contextmanager
    def get_connection(self):
        """
        Context manager для получения соединения с БД.
        
        Yields:
            SQLAlchemy Connection объект
        
        Example:
            with db.get_connection() as conn:
                result = conn.execute(text("SELECT * FROM table"))
        """
        connection = self.engine.connect()
        
        try:
            yield connection
            connection.commit()
        
        except Exception:
            connection.rollback()
            raise
        
        finally:
            connection.close()
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        fetch_all: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Выполняет SQL запрос и возвращает результаты.
        
        Args:
            query: SQL запрос
            params: Параметры для запроса (для защиты от SQL injection)
            fetch_all: Получить все строки (True) или только первую (False)
        
        Returns:
            Список словарей с результатами запроса
        
        Raises:
            DatabaseQueryError: Если запрос завершился с ошибкой
        """
        if settings.ENABLE_SQL_LOGGING:
            logger.info(f"Executing query: {query[:200]}{'...' if len(query) > 200 else ''}")
        
        try:
            with self.get_connection() as conn:
                # Устанавливаем таймаут для запроса
                conn.execute(text(f"SET statement_timeout = {settings.SQL_EXECUTION_TIMEOUT * 1000}"))
                
                # Выполняем запрос
                result = conn.execute(text(query), params or {})
                
                # Если это SELECT запрос, возвращаем результаты
                if result.returns_rows:
                    if fetch_all:
                        rows = result.fetchall()
                    else:
                        row = result.fetchone()
                        rows = [row] if row else []
                    
                    # Конвертируем в список словарей
                    columns = result.keys()
                    results = [dict(zip(columns, row)) for row in rows]
                    
                    logger.info(f"Query returned {len(results)} rows")
                    return results
                
                # Для INSERT/UPDATE/DELETE возвращаем количество затронутых строк
                else:
                    rowcount = result.rowcount
                    logger.info(f"Query affected {rowcount} rows")
                    return [{"rowcount": rowcount}]
        
        except ProgrammingError as e:
            error_msg = f"SQL syntax error: {str(e)}"
            logger.error(f"❌ {error_msg}")
            raise DatabaseQueryError(error_msg) from e
        
        except OperationalError as e:
            error_msg = f"Database operational error: {str(e)}"
            logger.error(f"❌ {error_msg}")
            raise DatabaseQueryError(error_msg) from e
        
        except SQLAlchemyError as e:
            error_msg = f"Database error: {str(e)}"
            logger.error(f"❌ {error_msg}")
            raise DatabaseQueryError(error_msg) from e
        
        except Exception as e:
            error_msg = f"Unexpected error during query execution: {str(e)}"
            logger.error(f"❌ {error_msg}")
            raise DatabaseQueryError(error_msg) from e
    
    def get_table_names(self) -> List[str]:
        """
        Получает список всех таблиц в схеме.
        
        Returns:
            Список названий таблиц
        """
        inspector = inspect(self.engine)
        tables = inspector.get_table_names(schema=self.schema)
        
        logger.info(f"Found {len(tables)} tables in schema '{self.schema}'")
        return tables
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Получает схему таблицы (колонки и их типы).
        
        Args:
            table_name: Название таблицы
        
        Returns:
            Список словарей с информацией о колонках:
            [
                {
                    "name": "column_name",
                    "type": "VARCHAR(100)",
                    "nullable": True,
                    "primary_key": False,
                    "default": None
                },
                ...
            ]
        """
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name, schema=self.schema)
        pk_constraint = inspector.get_pk_constraint(table_name, schema=self.schema)
        pk_columns = pk_constraint.get("constrained_columns", []) if pk_constraint else []
        
        schema_info = []
        
        for column in columns:
            schema_info.append({
                "name": column["name"],
                "type": str(column["type"]),
                "nullable": column["nullable"],
                "primary_key": column["name"] in pk_columns,
                "default": column.get("default")
            })
        
        logger.info(f"Retrieved schema for table '{table_name}': {len(schema_info)} columns")
        return schema_info
    
    def get_all_tables_schemas(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Получает схемы всех таблиц в базе данных.
        
        Returns:
            Словарь: {table_name: [column_info, ...]}
        """
        tables = self.get_table_names()
        schemas = {}
        
        for table in tables:
            schemas[table] = self.get_table_schema(table)
        
        return schemas
    
    def get_table_sample(self, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Получает несколько примеров строк из таблицы.
        
        Args:
            table_name: Название таблицы
            limit: Количество строк для выборки
        
        Returns:
            Список словарей с данными
        """
        query = f"SELECT * FROM {self.schema}.{table_name} LIMIT {limit}"
        return self.execute_query(query)
    
    def format_query_error(self, query: str, error: Exception) -> str:
        """
        Форматирует ошибку выполнения запроса для передачи LLM.
        
        Args:
            query: SQL запрос который вызвал ошибку
            error: Объект исключения
        
        Returns:
            Форматированное сообщение об ошибке
        """
        error_msg = str(error)
        
        # Извлекаем полезную часть ошибки PostgreSQL
        if "HINT:" in error_msg:
            error_msg = error_msg.split("HINT:")[0]
        
        formatted = f"""
SQL Query Error:

Query:
{query}

Error:
{error_msg}

Please fix the SQL query to resolve this error.
"""
        
        return formatted.strip()
    
    def close(self):
        """Закрывает все соединения с базой данных"""
        self.engine.dispose()
        logger.info("Database connections closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def get_database_info_summary(self) -> str:
        """
        Возвращает краткую информацию о базе данных.
        
        Returns:
            Текстовое описание БД (таблицы и их структура)
        """
        tables_schemas = self.get_all_tables_schemas()
        
        summary = []
        summary.append(f"Database: {settings.DB_NAME}")
        summary.append(f"Schema: {self.schema}")
        summary.append(f"Tables: {len(tables_schemas)}\n")
        
        for table_name, columns in tables_schemas.items():
            summary.append(f"Table: {table_name}")
            
            for col in columns:
                pk_mark = " [PK]" if col["primary_key"] else ""
                null_mark = " [NULL]" if col["nullable"] else " [NOT NULL]"
                summary.append(f"  - {col['name']} ({col['type']}){pk_mark}{null_mark}")
            
            summary.append("")  # Пустая строка между таблицами
        
        return "\n".join(summary)


# Создаём глобальный экземпляр для использования в приложении
db = Database()


# Вспомогательные функции

def test_database_connection() -> bool:
    """
    Тестирует подключение к базе данных.
    
    Returns:
        True если подключение успешно
    """
    return db.test_connection()


def execute_sql(query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Быстрый способ выполнить SQL запрос.
    
    Args:
        query: SQL запрос
        params: Параметры запроса
    
    Returns:
        Результаты запроса
    """
    return db.execute_query(query, params)


def get_tables() -> List[str]:
    """Возвращает список таблиц в БД"""
    return db.get_table_names()


def get_schema(table_name: str) -> List[Dict[str, Any]]:
    """Возвращает схему таблицы"""
    return db.get_table_schema(table_name)


# Пример использования
if __name__ == "__main__":
    # Настраиваем логирование
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    print("=" * 60)
    print("Database Connection Test")
    print("=" * 60)
    
    try:
        # Тестируем подключение
        db.test_connection()
        print("✅ Connection successful!\n")
        
        # Получаем список таблиц
        tables = db.get_table_names()
        print(f"Found {len(tables)} tables:")
        for table in tables:
            print(f"  - {table}")
        
        print("\n" + "=" * 60)
        print("Database Schema Summary")
        print("=" * 60)
        
        # Выводим информацию о БД
        print(db.get_database_info_summary())
        
        # Пример запроса
        if tables:
            print("\n" + "=" * 60)
            print(f"Sample data from {tables[0]}")
            print("=" * 60)
            
            sample = db.get_table_sample(tables[0], limit=3)
            
            for i, row in enumerate(sample, 1):
                print(f"\nRow {i}:")
                for key, value in row.items():
                    print(f"  {key}: {value}")
    
    except DatabaseConnectionError as e:
        print(f"❌ Connection failed: {e}")
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    finally:
        db.close()
        print("\n✅ Database connections closed")