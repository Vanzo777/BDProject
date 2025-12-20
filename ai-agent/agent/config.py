"""
Configuration module для SQL AI Agent.

Загружает настройки из .env файла и предоставляет типизированный доступ к конфигурации.
Использует pydantic для валидации и удобного доступа к переменным окружения.
"""

from typing import Optional
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator


class Settings(BaseSettings):
    """
    Настройки приложения из .env файла.
    
    Все параметры автоматически загружаются из переменных окружения.
    Можно переопределить через .env файл в корне проекта.
    """
    
    # === DATABASE SETTINGS ===
    
    DB_HOST: str = Field(
        default="localhost",
        description="Хост PostgreSQL"
    )
    
    DB_PORT: int = Field(
        default=5432,
        description="Порт PostgreSQL"
    )
    
    DB_NAME: str = Field(
        default="mock_analytics",
        description="Название базы данных"
    )
    
    DB_USER: str = Field(
        default="user",
        description="Пользователь БД"
    )
    
    DB_PASSWORD: str = Field(
        default="123",
        description="Пароль БД"
    )
    
    DB_SCHEMA: str = Field(
        default="public",
        description="Схема БД для таблиц"
    )
    
    # === LLM SETTINGS ===
    
    OPENROUTER_API_KEY: str = Field(
        default="",
        description="API ключ OpenRouter"
    )
    
    OPENROUTER_BASE_URL: str = Field(
        default="https://openrouter.ai/api/v1",
        description="Base URL для OpenRouter API"
    )
    
    LLM_MODEL: str = Field(
        default="google/gemini-2.0-flash-exp:free",
        description="Модель LLM для использования"
    )
    
    LLM_TEMPERATURE: float = Field(
        default=0.0,
        ge=0.0,
        le=2.0,
        description="Temperature для LLM (0.0 = детерминированный, 2.0 = креативный)"
    )
    
    LLM_MAX_TOKENS: int = Field(
        default=2000,
        gt=0,
        description="Максимальное количество токенов в ответе LLM"
    )
    
    # === SEMANTIC LAYER SETTINGS ===
    
    SEMANTIC_LAYER_PATH: str = Field(
        default="config/semantic_layer.yaml",
        description="Путь к файлу semantic layer"
    )
    
    FEW_SHOT_EXAMPLES_LIMIT: int = Field(
        default=5,
        ge=0,
        le=20,
        description="Количество примеров для few-shot learning (0 = без примеров)"
    )
    
    # === APPLICATION SETTINGS ===
    
    LOG_LEVEL: str = Field(
        default="INFO",
        description="Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    
    ENABLE_SQL_LOGGING: bool = Field(
        default=True,
        description="Логировать ли SQL запросы"
    )
    
    MAX_RETRY_ATTEMPTS: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Максимальное количество попыток исправить SQL при ошибке"
    )
    
    SQL_EXECUTION_TIMEOUT: int = Field(
        default=30,
        gt=0,
        description="Таймаут выполнения SQL запроса в секундах"
    )
    
    # === UI SETTINGS ===
    
    STREAMLIT_PORT: int = Field(
        default=8501,
        description="Порт для Streamlit UI"
    )
    
    STREAMLIT_TITLE: str = Field(
        default="SQL AI Agent",
        description="Заголовок Streamlit приложения"
    )
    
    # Конфигурация для pydantic_settings
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"  # Игнорировать неизвестные переменные
    )
    
    @field_validator("LOG_LEVEL")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Валидация уровня логирования"""
        allowed_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        
        if v_upper not in allowed_levels:
            raise ValueError(f"LOG_LEVEL должен быть одним из: {', '.join(allowed_levels)}")
        
        return v_upper
    
    @field_validator("OPENROUTER_API_KEY")
    @classmethod
    def validate_api_key(cls, v: str) -> str:
        """Проверка наличия API ключа"""
        if not v or v.strip() == "":
            import warnings
            warnings.warn(
                "OPENROUTER_API_KEY не установлен! "
                "Установите API ключ в .env файле для работы с LLM.",
                UserWarning
            )
        
        return v
    
    @field_validator("SEMANTIC_LAYER_PATH")
    @classmethod
    def validate_semantic_layer_path(cls, v: str) -> str:
        """Проверка существования файла semantic layer"""
        path = Path(v)
        
        if not path.exists():
            import warnings
            warnings.warn(
                f"Файл semantic layer не найден: {v}\n"
                f"Убедитесь, что файл существует или будет создан.",
                UserWarning
            )
        
        return v
    
    def get_database_uri(self) -> str:
        """
        Формирует URI для подключения к PostgreSQL.
        
        Returns:
            Connection string в формате postgresql://user:password@host:port/database
        """
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )
    
    def get_database_uri_async(self) -> str:
        """
        Формирует асинхронный URI для подключения к PostgreSQL.
        
        Returns:
            Connection string в формате postgresql+asyncpg://...
        """
        return (
            f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )
    
    def get_llm_config(self) -> dict:
        """
        Возвращает конфигурацию для LLM в виде словаря.
        
        Returns:
            Словарь с настройками LLM
        """
        return {
            "model": self.LLM_MODEL,
            "temperature": self.LLM_TEMPERATURE,
            "max_tokens": self.LLM_MAX_TOKENS,
            "api_key": self.OPENROUTER_API_KEY,
            "base_url": self.OPENROUTER_BASE_URL
        }
    
    def is_development(self) -> bool:
        """
        Проверяет, запущено ли приложение в режиме разработки.
        
        Returns:
            True если используется localhost БД
        """
        return self.DB_HOST in ["localhost", "127.0.0.1", "0.0.0.0"]
    
    def validate_all(self) -> list[str]:
        """
        Проверяет все критичные настройки и возвращает список проблем.
        
        Returns:
            Список строк с описанием проблем (пустой список если всё ОК)
        """
        issues = []
        
        # Проверка API ключа
        if not self.OPENROUTER_API_KEY or self.OPENROUTER_API_KEY.strip() == "":
            issues.append("❌ OPENROUTER_API_KEY не установлен")
        
        # Проверка semantic layer
        if not Path(self.SEMANTIC_LAYER_PATH).exists():
            issues.append(f"❌ Файл semantic layer не найден: {self.SEMANTIC_LAYER_PATH}")
        
        # Проверка температуры
        if self.LLM_TEMPERATURE < 0 or self.LLM_TEMPERATURE > 2:
            issues.append(f"❌ LLM_TEMPERATURE должна быть от 0 до 2, текущее значение: {self.LLM_TEMPERATURE}")
        
        return issues
    
    def print_config_summary(self) -> None:
        """Выводит краткую сводку по текущей конфигурации"""
        print("=" * 60)
        print("SQL AI Agent - Configuration Summary")
        print("=" * 60)
        print(f"Database: {self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}")
        print(f"DB User: {self.DB_USER}")
        print(f"DB Schema: {self.DB_SCHEMA}")
        print(f"LLM Model: {self.LLM_MODEL}")
        print(f"LLM Temperature: {self.LLM_TEMPERATURE}")
        print(f"Semantic Layer: {self.SEMANTIC_LAYER_PATH}")
        print(f"Few-shot Examples: {self.FEW_SHOT_EXAMPLES_LIMIT}")
        print(f"Log Level: {self.LOG_LEVEL}")
        print(f"Development Mode: {self.is_development()}")
        
        issues = self.validate_all()
        
        if issues:
            print("\n⚠️  Configuration Issues:")
            for issue in issues:
                print(f"  {issue}")
        else:
            print("\n✅ Configuration is valid!")
        
        print("=" * 60)


# Создаём глобальный экземпляр настроек
# Он будет загружен при импорте модуля
settings = Settings()


# Вспомогательные функции для быстрого доступа

def get_db_uri() -> str:
    """Быстрый доступ к database URI"""
    return settings.get_database_uri()


def get_llm_config() -> dict:
    """Быстрый доступ к LLM конфигурации"""
    return settings.get_llm_config()


def is_dev_mode() -> bool:
    """Проверка режима разработки"""
    return settings.is_development()


# Пример использования
if __name__ == "__main__":
    # Выводим текущую конфигурацию
    settings.print_config_summary()
    
    # Проверяем валидность
    issues = settings.validate_all()
    
    if issues:
        print("\n❌ Найдены проблемы конфигурации:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("\n✅ Конфигурация валидна!")
    
    # Выводим connection string
    print(f"\nDatabase URI: {settings.get_database_uri()}")
    print(f"LLM Config: {settings.get_llm_config()}")