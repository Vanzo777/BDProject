"""
SQL Agent module для SQL AI Agent.

Интегрирует все компоненты для работы Text-to-SQL системы:
- Semantic Layer для контекста
- LLM для генерации SQL
- Database для выполнения запросов
- LangChain для агентской логики

Updated: December 2025 для LangChain 0.3.x
"""

from typing import List, Dict, Any, Optional, Tuple
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain_community.agent_toolkits import create_sql_agent, SQLDatabaseToolkit
from langchain.agents import AgentExecutor
import logging
import time

from agent.config import settings, get_db_uri
from agent.semantic_layer import SemanticLayer
from agent.database import db, DatabaseQueryError
from agent.llm import llm as custom_llm


# Настройка логирования
logger = logging.getLogger(__name__)


class SQLAgent:
    """
    SQL Agent для Text-to-SQL системы.
    
    Объединяет:
    - LangChain SQL Agent для генерации и выполнения SQL
    - Semantic Layer для контекста о данных
    - Retry логику для исправления ошибок
    - Кастомный LLM через OpenRouter
    """
    
    def __init__(
        self,
        semantic_layer_path: Optional[str] = None,
        use_few_shot: bool = True,
        verbose: bool = True
    ):
        """
        Инициализация SQL Agent.
        
        Args:
            semantic_layer_path: Путь к semantic_layer.yaml (если None, берётся из settings)
            use_few_shot: Использовать ли few-shot примеры
            verbose: Выводить ли детальные логи работы агента
        """
        self.verbose = verbose
        self.use_few_shot = use_few_shot
        
        # Загружаем semantic layer
        semantic_path = semantic_layer_path or settings.SEMANTIC_LAYER_PATH
        self.semantic_layer = SemanticLayer.from_yaml(semantic_path)
        
        logger.info(f"Semantic layer loaded: {self.semantic_layer}")
        
        # Проверяем подключение к БД
        try:
            db.test_connection()
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
        
        # Создаём LangChain SQLDatabase wrapper
        self.sql_database = SQLDatabase.from_uri(
            get_db_uri(),
            schema=settings.DB_SCHEMA,
            include_tables=self.semantic_layer.list_tables(),
            sample_rows_in_table_info=2
        )
        
        # Создаём LLM для LangChain (ИСПРАВЛЕНО: base_url вместо openai_api_base)
        self.llm = ChatOpenAI(
            model=settings.LLM_MODEL,
            temperature=settings.LLM_TEMPERATURE,
            max_tokens=settings.LLM_MAX_TOKENS,
            openai_api_key=settings.OPENROUTER_API_KEY,
            base_url=settings.OPENROUTER_BASE_URL,  # ИСПРАВЛЕНО!
            default_headers={
                "HTTP-Referer": "https://sql-ai-agent.local",
                "X-Title": "SQL AI Agent"
            }
        )
        
        # Создаём SQL toolkit
        self.toolkit = SQLDatabaseToolkit(
            db=self.sql_database,
            llm=self.llm
        )
        
        # Формируем system prompt с контекстом
        self.system_prompt = self._build_system_prompt()
        
        # Создаём агента
        self.agent_executor = self._create_agent()
        
        logger.info("SQL Agent initialized successfully")
    
    def _build_system_prompt(self) -> str:
        """
        Формирует system prompt для агента с полным контекстом.
        
        Returns:
            System prompt текст
        """
        prompt_parts = []
        
        # Базовый system prompt из semantic layer
        prompt_parts.append(self.semantic_layer.get_system_prompt())
        
        # Добавляем few-shot примеры если включены
        if self.use_few_shot:
            examples = self.semantic_layer.get_few_shot_examples(
                limit=settings.FEW_SHOT_EXAMPLES_LIMIT
            )
            if examples:
                prompt_parts.append("\n")
                prompt_parts.append(examples)
        
        # Дополнительные инструкции для агента
        prompt_parts.append("\n")
        prompt_parts.append("=== ДОПОЛНИТЕЛЬНЫЕ ИНСТРУКЦИИ ДЛЯ АГЕНТА ===\n")
        prompt_parts.append("1. Сначала изучи структуру таблиц с помощью инструмента sql_db_list_tables\n")
        prompt_parts.append("2. Затем посмотри схему нужных таблиц через sql_db_schema\n")
        prompt_parts.append("3. Генерируй SQL запрос внимательно, проверяя названия колонок и таблиц\n")
        prompt_parts.append("4. Выполни запрос через sql_db_query\n")
        prompt_parts.append("5. Если запрос вернул ошибку, исправь его и попробуй снова\n")
        prompt_parts.append("6. Верни результат пользователю в понятном виде на русском языке\n")
        prompt_parts.append("7. ВСЕГДА отвечай на русском языке, даже если вопрос на английском\n")
        
        return "\n".join(prompt_parts)
    
    def _create_agent(self) -> AgentExecutor:
        """
        Создаёт LangChain SQL Agent.
        
        Returns:
            AgentExecutor готовый к использованию
        """
        # ИСПРАВЛЕНО: используем agent_type="openai-tools" для LangChain 0.3.x
        agent_executor = create_sql_agent(
            llm=self.llm,
            toolkit=self.toolkit,
            verbose=self.verbose,
            agent_type="openai-tools",  # ИСПРАВЛЕНО! Вместо AgentType.ZERO_SHOT_REACT_DESCRIPTION
            max_iterations=settings.MAX_RETRY_ATTEMPTS + 2,
            max_execution_time=settings.SQL_EXECUTION_TIMEOUT * 2,
            handle_parsing_errors=True,
            agent_executor_kwargs={
                "return_intermediate_steps": True,
            },
            prefix=self.system_prompt
        )
        
        return agent_executor
    
    def ask(
        self,
        question: str,
        return_sql: bool = False,
        return_intermediate_steps: bool = False
    ) -> Dict[str, Any]:
        """
        Задаёт вопрос агенту и получает ответ.
        
        Args:
            question: Вопрос на естественном языке
            return_sql: Возвращать ли сгенерированный SQL
            return_intermediate_steps: Возвращать ли промежуточные шаги
        
        Returns:
            Словарь с ответом:
            {
                "answer": "текстовый ответ",
                "sql": "SELECT ...",  # если return_sql=True
                "intermediate_steps": [...],  # если return_intermediate_steps=True
                "execution_time": 1.23,
                "success": True
            }
        """
        logger.info(f"Question received: {question}")
        
        start_time = time.time()
        
        try:
            # Выполняем запрос через агента
            result = self.agent_executor.invoke(
                {"input": question}
            )
            
            execution_time = time.time() - start_time
            
            # Формируем ответ
            response = {
                "answer": result.get("output", "Не удалось получить ответ"),
                "execution_time": round(execution_time, 2),
                "success": True
            }
            
            # Добавляем промежуточные шаги если нужно
            if return_intermediate_steps:
                response["intermediate_steps"] = result.get("intermediate_steps", [])
            
            # Пытаемся извлечь SQL из промежуточных шагов
            if return_sql:
                sql = self._extract_sql_from_steps(result.get("intermediate_steps", []))
                response["sql"] = sql
            
            logger.info(f"Question answered successfully in {execution_time:.2f}s")
            
            return response
        
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = str(e)
            
            logger.error(f"Failed to answer question: {error_msg}")
            
            return {
                "answer": f"Ошибка при обработке вопроса: {error_msg}",
                "execution_time": round(execution_time, 2),
                "success": False,
                "error": error_msg
            }
    
    def _extract_sql_from_steps(self, intermediate_steps: List[Tuple]) -> Optional[str]:
        """
        Извлекает SQL запрос из промежуточных шагов агента.
        
        Args:
            intermediate_steps: Промежуточные шаги выполнения агента
        
        Returns:
            SQL запрос или None если не найден
        """
        if not intermediate_steps:
            return None
        
        # Проходим по всем шагам в обратном порядке (последние более актуальные)
        for step in reversed(intermediate_steps):
            if not isinstance(step, tuple) or len(step) < 2:
                continue
            
            action, observation = step
            
            # Проверяем разные форматы action
            tool_name = None
            tool_input = None
            
            if hasattr(action, 'tool'):
                tool_name = action.tool
                tool_input = action.tool_input
            elif isinstance(action, dict):
                tool_name = action.get('tool')
                tool_input = action.get('tool_input')
            
            # Ищем вызов sql_db_query
            if tool_name == 'sql_db_query':
                if isinstance(tool_input, dict):
                    sql = tool_input.get('query', '')
                elif isinstance(tool_input, str):
                    sql = tool_input
                else:
                    sql = str(tool_input)
                
                if sql:
                    return sql
        
        return None
    
    def ask_with_sql(self, question: str) -> Tuple[str, Optional[str], List[Dict[str, Any]]]:
        """
        Задаёт вопрос и возвращает: ответ, SQL и результаты запроса.
        
        Args:
            question: Вопрос на естественном языке
        
        Returns:
            Кортеж (answer, sql, results)
        """
        response = self.ask(question, return_sql=True, return_intermediate_steps=True)
        
        answer = response.get("answer", "")
        sql = response.get("sql")
        
        # Пытаемся извлечь результаты из промежуточных шагов
        results = []
        for step in response.get("intermediate_steps", []):
            if not isinstance(step, tuple) or len(step) < 2:
                continue
            
            action, observation = step
            
            tool_name = None
            if hasattr(action, 'tool'):
                tool_name = action.tool
            elif isinstance(action, dict):
                tool_name = action.get('tool')
            
            if tool_name == 'sql_db_query':
                # observation содержит результаты запроса в виде строки
                results = [{"raw_output": observation}]
                break
        
        return answer, sql, results
    
    def regenerate_sql(
        self,
        question: str,
        failed_sql: str,
        error_message: str
    ) -> str:
        """
        Регенерирует SQL на основе ошибки предыдущей попытки.
        
        Args:
            question: Исходный вопрос
            failed_sql: SQL который вызвал ошибку
            error_message: Сообщение об ошибке
        
        Returns:
            Новый SQL запрос
        """
        logger.info("Regenerating SQL after error")
        
        # Используем наш кастомный LLM для исправления
        fixed_sql = custom_llm.fix_sql_error(
            original_query=failed_sql,
            error_message=error_message,
            system_prompt=self.system_prompt,
            question=question
        )
        
        return fixed_sql
    
    def get_table_info(self, table_name: str) -> str:
        """
        Получает информацию о таблице.
        
        Args:
            table_name: Название таблицы
        
        Returns:
            Текстовое описание таблицы
        """
        return self.sql_database.get_table_info([table_name])
    
    def list_tables(self) -> List[str]:
        """Возвращает список доступных таблиц"""
        return self.semantic_layer.list_tables()
    
    def get_semantic_info(self, table_name: str) -> Optional[str]:
        """
        Возвращает бизнес-описание таблицы из semantic layer.
        
        Args:
            table_name: Название таблицы
        
        Returns:
            Описание таблицы или None
        """
        table_info = self.semantic_layer.get_table_info(table_name)
        
        if not table_info:
            return None
        
        info = []
        info.append(f"Таблица: {table_info.name}")
        info.append(f"Описание: {table_info.description}")
        info.append(f"Бизнес-контекст: {table_info.business_context}")
        info.append("\nКолонки:")
        
        for col_name, col in table_info.columns.items():
            col_desc = f"  - {col_name} ({col.type}): {col.description}"
            if col.business_meaning:
                col_desc += f" — {col.business_meaning}"
            info.append(col_desc)
        
        return "\n".join(info)
    
    def switch_model(self, model: str):
        """
        Переключает LLM модель.
        
        Args:
            model: Название модели (например, "deepseek/deepseek-chat")
        """
        logger.info(f"Switching model to: {model}")
        
        # Обновляем LLM (ИСПРАВЛЕНО: base_url)
        self.llm = ChatOpenAI(
            model=model,
            temperature=settings.LLM_TEMPERATURE,
            max_tokens=settings.LLM_MAX_TOKENS,
            openai_api_key=settings.OPENROUTER_API_KEY,
            base_url=settings.OPENROUTER_BASE_URL,  # ИСПРАВЛЕНО!
            default_headers={
                "HTTP-Referer": "https://sql-ai-agent.local",
                "X-Title": "SQL AI Agent"
            }
        )
        
        # Пересоздаём toolkit и агента
        self.toolkit = SQLDatabaseToolkit(db=self.sql_database, llm=self.llm)
        self.agent_executor = self._create_agent()
        
        logger.info(f"Model switched to: {model}")
    
    def __repr__(self) -> str:
        return (f"SQLAgent(model={settings.LLM_MODEL}, "
                f"tables={len(self.list_tables())}, "
                f"few_shot={self.use_few_shot})")


# ============================================================================
# Вспомогательные функции для быстрого доступа
# ============================================================================

_agent_instance: Optional[SQLAgent] = None


def get_agent(
    semantic_layer_path: Optional[str] = None,
    use_few_shot: bool = True,
    verbose: bool = False,
    force_new: bool = False
) -> SQLAgent:
    """
    Получает singleton instance SQL Agent.
    
    Args:
        semantic_layer_path: Путь к semantic_layer.yaml
        use_few_shot: Использовать few-shot примеры
        verbose: Детальные логи
        force_new: Создать новый instance (игнорировать кэш)
    
    Returns:
        SQLAgent instance
    """
    global _agent_instance
    
    if force_new or _agent_instance is None:
        _agent_instance = SQLAgent(
            semantic_layer_path=semantic_layer_path,
            use_few_shot=use_few_shot,
            verbose=verbose
        )
    
    return _agent_instance


def ask_question(question: str) -> str:
    """
    Простая функция для быстрого вопроса к агенту.
    
    Args:
        question: Вопрос на естественном языке
    
    Returns:
        Текстовый ответ
    """
    agent = get_agent()
    response = agent.ask(question)
    return response["answer"]


def ask_with_details(question: str) -> Dict[str, Any]:
    """
    Задаёт вопрос и возвращает детальную информацию.
    
    Args:
        question: Вопрос на естественном языке
    
    Returns:
        Словарь с answer, sql, execution_time и т.д.
    """
    agent = get_agent()
    return agent.ask(question, return_sql=True, return_intermediate_steps=False)


# ============================================================================
# CLI для тестирования
# ============================================================================

def main():
    """CLI для интерактивного тестирования агента"""
    import sys
    
    print("=" * 80)
    print("SQL AI AGENT - Interactive Mode")
    print("=" * 80)
    
    # Инициализируем агента
    try:
        agent = get_agent(verbose=True, use_few_shot=True)
        print(f"✅ Agent initialized: {agent}\n")
    except Exception as e:
        print(f"❌ Failed to initialize agent: {e}")
        sys.exit(1)
    
    # Интерактивный режим
    print("Задавайте вопросы (или 'exit' для выхода):\n")
    
    while True:
        try:
            question = input("Вопрос: ").strip()
            
            if not question:
                continue
            
            if question.lower() in ['exit', 'quit', 'q']:
                print("Пока!")
                break
            
            # Специальные команды
            if question.startswith('/'):
                if question == '/tables':
                    tables = agent.list_tables()
                    print(f"Доступные таблицы: {', '.join(tables)}\n")
                    continue
                elif question.startswith('/info '):
                    table = question.split(' ', 1)[1]
                    info = agent.get_semantic_info(table)
                    print(f"\n{info}\n")
                    continue
                elif question == '/help':
                    print("Команды:")
                    print("  /tables - список таблиц")
                    print("  /info <table> - информация о таблице")
                    print("  /help - эта справка")
                    print("  exit - выход\n")
                    continue
            
            # Задаём вопрос
            print("\n🤖 Обработка...\n")
            
            response = agent.ask(question, return_sql=True)
            
            print(f"Ответ: {response['answer']}\n")
            
            if response.get('sql'):
                print(f"SQL: {response['sql']}\n")
            
            print(f"Время: {response['execution_time']}s")
            print("-" * 80 + "\n")
        
        except KeyboardInterrupt:
            print("\n\nПрервано пользователем. Пока!")
            break
        except Exception as e:
            print(f"❌ Ошибка: {e}\n")


if __name__ == "__main__":
    main()