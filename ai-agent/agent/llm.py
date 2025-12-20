"""
LLM module для SQL AI Agent.

Предоставляет интерфейс для работы с языковыми моделями через OpenRouter API.
Использует OpenAI-совместимый клиент для единого интерфейса ко всем моделям.
"""

from typing import List, Dict, Any, Optional, Union
from openai import OpenAI
from openai.types.chat import ChatCompletion, ChatCompletionMessage
import logging
import time

from agent.config import settings


# Настройка логирования
logger = logging.getLogger(__name__)


class LLMError(Exception):
    """Ошибка при работе с LLM"""
    pass


class LLMRateLimitError(LLMError):
    """Ошибка превышения лимита запросов"""
    pass


class LLM:
    """
    Класс для работы с языковыми моделями через OpenRouter API.
    
    Поддерживает все модели доступные через OpenRouter:
    - Google Gemini (gemini-2.0-flash-exp:free, gemini-pro)
    - Deepseek (deepseek-chat)
    - OpenAI (gpt-4-turbo, gpt-3.5-turbo)
    - Anthropic Claude (claude-3.5-sonnet)
    - И многие другие
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        site_url: str = "https://sql-ai-agent.local",
        site_name: str = "SQL AI Agent"
    ):
        """
        Инициализация LLM клиента.
        
        Args:
            api_key: API ключ OpenRouter (если None, берётся из settings)
            base_url: Base URL для OpenRouter (если None, берётся из settings)
            model: Название модели (если None, берётся из settings)
            temperature: Temperature для генерации (если None, берётся из settings)
            max_tokens: Максимум токенов (если None, берётся из settings)
            site_url: URL вашего сайта для статистики OpenRouter
            site_name: Название сайта для статистики OpenRouter
        """
        self.api_key = api_key or settings.OPENROUTER_API_KEY
        self.base_url = base_url or settings.OPENROUTER_BASE_URL
        self.model = model or settings.LLM_MODEL
        self.temperature = temperature if temperature is not None else settings.LLM_TEMPERATURE
        self.max_tokens = max_tokens or settings.LLM_MAX_TOKENS
        self.site_url = site_url
        self.site_name = site_name
        
        # Проверяем наличие API ключа
        if not self.api_key or self.api_key.strip() == "":
            raise LLMError(
                "OPENROUTER_API_KEY не установлен! "
                "Установите API ключ в .env файле."
            )
        
        # Создаём OpenAI клиент с OpenRouter endpoint
        self.client = OpenAI(
            base_url=self.base_url,
            api_key=self.api_key,
        )
        
        logger.info(f"LLM initialized: model={self.model}, temperature={self.temperature}")
    
    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        model: Optional[str] = None,
        stream: bool = False
    ) -> Union[str, ChatCompletion]:
        """
        Отправляет запрос к LLM и получает ответ.
        
        Args:
            messages: Список сообщений в формате OpenAI:
                [
                    {"role": "system", "content": "You are a SQL expert"},
                    {"role": "user", "content": "Generate SQL for..."}
                ]
            temperature: Temperature для этого запроса (переопределяет дефолтную)
            max_tokens: Максимум токенов для этого запроса
            model: Модель для этого запроса (переопределяет дефолтную)
            stream: Использовать ли streaming (для real-time ответов)
        
        Returns:
            Текст ответа от LLM или полный ChatCompletion объект
        
        Raises:
            LLMError: При ошибке выполнения запроса
        """
        try:
            # Параметры запроса
            model_to_use = model or self.model
            temp_to_use = temperature if temperature is not None else self.temperature
            tokens_to_use = max_tokens or self.max_tokens
            
            if settings.LOG_LEVEL == "DEBUG":
                logger.debug(f"LLM request: model={model_to_use}, messages={len(messages)}")
            
            # Засекаем время
            start_time = time.time()
            
            # Выполняем запрос
            completion = self.client.chat.completions.create(
                extra_headers={
                    "HTTP-Referer": self.site_url,
                    "X-Title": self.site_name,
                },
                extra_body={},
                model=model_to_use,
                messages=messages,
                temperature=temp_to_use,
                max_tokens=tokens_to_use,
                stream=stream
            )
            
            elapsed_time = time.time() - start_time
            
            # Если streaming, возвращаем полный объект
            if stream:
                return completion
            
            # Извлекаем текст ответа
            response_text = completion.choices[0].message.content
            
            # Логируем статистику
            if hasattr(completion, 'usage') and completion.usage:
                logger.info(
                    f"LLM response received in {elapsed_time:.2f}s "
                    f"(tokens: {completion.usage.total_tokens})"
                )
            else:
                logger.info(f"LLM response received in {elapsed_time:.2f}s")
            
            if settings.LOG_LEVEL == "DEBUG":
                logger.debug(f"LLM response: {response_text[:200]}...")
            
            return response_text
        
        except Exception as e:
            error_msg = f"LLM request failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            
            # Проверяем специфичные ошибки
            if "rate limit" in str(e).lower():
                raise LLMRateLimitError(error_msg) from e
            
            raise LLMError(error_msg) from e
    
    def generate_sql(
        self,
        question: str,
        system_prompt: str,
        examples: Optional[str] = None,
        temperature: Optional[float] = None
    ) -> str:
        """
        Генерирует SQL запрос на основе вопроса на естественном языке.
        
        Args:
            question: Вопрос пользователя на естественном языке
            system_prompt: System prompt с описанием БД и инструкциями
            examples: Примеры запросов (few-shot learning)
            temperature: Temperature для генерации (если None, используется дефолтная)
        
        Returns:
            Сгенерированный SQL запрос
        """
        messages = [
            {"role": "system", "content": system_prompt}
        ]
        
        # Добавляем примеры если есть
        if examples:
            messages.append({"role": "system", "content": examples})
        
        # Добавляем вопрос пользователя
        messages.append({
            "role": "user",
            "content": f"Сгенерируй SQL запрос для вопроса: {question}"
        })
        
        response = self.chat(messages, temperature=temperature)
        
        # Извлекаем SQL из ответа (может быть обёрнут в ```
        sql = self._extract_sql_from_response(response)
        
        return sql
    
    def fix_sql_error(
        self,
        original_query: str,
        error_message: str,
        system_prompt: str,
        question: str
    ) -> str:
        """
        Пытается исправить SQL запрос на основе сообщения об ошибке.
        
        Args:
            original_query: Исходный SQL запрос с ошибкой
            error_message: Сообщение об ошибке от БД
            system_prompt: System prompt с описанием БД
            question: Исходный вопрос пользователя
        
        Returns:
            Исправленный SQL запрос
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Вопрос: {question}"},
            {"role": "assistant", "content": original_query},
            {
                "role": "user",
                "content": f"""
Этот SQL запрос вызвал ошибку:

{error_message}

Пожалуйста, исправь SQL запрос чтобы он работал корректно.
Верни ТОЛЬКО исправленный SQL запрос без объяснений.
"""
            }
        ]
        
        response = self.chat(messages, temperature=0.0)
        
        # Извлекаем исправленный SQL
        fixed_sql = self._extract_sql_from_response(response)
        
        logger.info("SQL query fixed by LLM")
        
        return fixed_sql
    
    def explain_results(
        self,
        question: str,
        sql_query: str,
        results: List[Dict[str, Any]],
        max_rows_to_show: int = 10
    ) -> str:
        """
        Генерирует текстовое объяснение результатов запроса.
        
        Args:
            question: Исходный вопрос пользователя
            sql_query: Выполненный SQL запрос
            results: Результаты запроса
            max_rows_to_show: Максимум строк для показа LLM
        
        Returns:
            Текстовое объяснение результатов
        """
        # Ограничиваем количество строк для экономии токенов
        results_to_show = results[:max_rows_to_show]
        
        messages = [
            {
                "role": "system",
                "content": "Ты — помощник по анализу данных. Объясняй результаты SQL запросов простым языком."
            },
            {
                "role": "user",
                "content": f"""
Вопрос пользователя: {question}

Выполненный SQL запрос:
{sql_query}

Результаты ({len(results)} строк всего, показаны первые {len(results_to_show)}):
{self._format_results_for_llm(results_to_show)}

Объясни результаты простым языком, ответь на исходный вопрос пользователя.
Если результатов много, дай краткую сводку. Будь конкретным и используй цифры из результатов.
"""
            }
        ]
        
        explanation = self.chat(messages, temperature=0.3)
        
        return explanation
    
    def _extract_sql_from_response(self, response: str) -> str:
        """
        Извлекает SQL запрос из ответа LLM.
        """
        # Убираем лишние пробелы
        response = response.strip()
        
        # ИСПРАВЛЕНИЕ: Добавлены закрывающие кавычки и оператор 'in'
        if "```" in response:
            # Извлекаем содержимое между ```sql и ```
            parts = response.split("```")
            for part in parts:
                if part.strip().lower().startswith("sql"):
                    sql = part[3:].strip()  # Убираем "sql" и пробелы
                    return sql
            
            # Если есть просто markdown блок (без указания языка)
            if len(parts) >= 2:
                return parts[1].strip()
        
        # Иначе возвращаем как есть
        return response

    
    def _format_results_for_llm(self, results: List[Dict[str, Any]]) -> str:
        """
        Форматирует результаты запроса для передачи LLM.
        
        Args:
            results: Результаты SQL запроса
        
        Returns:
            Форматированная строка с результатами
        """
        if not results:
            return "Нет результатов"
        
        # Форматируем как таблицу
        lines = []
        
        # Заголовки
        headers = list(results[0].keys())
        lines.append(" | ".join(headers))
        lines.append("-" * (len(" | ".join(headers))))
        
        # Строки
        for row in results:
            values = [str(row.get(h, "NULL")) for h in headers]
            lines.append(" | ".join(values))
        
        return "\n".join(lines)
    
    def switch_model(self, model: str):
        """
        Переключает модель LLM на другую.
        
        Args:
            model: Название модели (например, "deepseek/deepseek-chat")
        """
        self.model = model
        logger.info(f"LLM model switched to: {model}")
    
    def get_current_model(self) -> str:
        """Возвращает текущую модель"""
        return self.model
    
    def __repr__(self) -> str:
        return f"LLM(model={self.model}, temperature={self.temperature})"


# Создаём глобальный экземпляр
llm = LLM()


# Вспомогательные функции

def generate_sql(question: str, system_prompt: str, examples: Optional[str] = None) -> str:
    """
    Быстрый способ сгенерировать SQL.
    
    Args:
        question: Вопрос на естественном языке
        system_prompt: System prompt с контекстом БД
        examples: Примеры запросов
    
    Returns:
        SQL запрос
    """
    return llm.generate_sql(question, system_prompt, examples)


def fix_sql(query: str, error: str, system_prompt: str, question: str) -> str:
    """
    Быстрый способ исправить SQL.
    
    Args:
        query: SQL с ошибкой
        error: Сообщение об ошибке
        system_prompt: System prompt
        question: Исходный вопрос
    
    Returns:
        Исправленный SQL
    """
    return llm.fix_sql_error(query, error, system_prompt, question)


def explain_query_results(
    question: str,
    sql: str,
    results: List[Dict[str, Any]]
) -> str:
    """
    Быстрый способ объяснить результаты.
    
    Args:
        question: Вопрос пользователя
        sql: Выполненный SQL
        results: Результаты
    
    Returns:
        Текстовое объяснение
    """
    return llm.explain_results(question, sql, results)


# Пример использования
if __name__ == "__main__":
    # Настраиваем логирование
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    print("=" * 60)
    print("LLM Test")
    print("=" * 60)
    print(f"Model: {llm.get_current_model()}")
    print(f"Temperature: {llm.temperature}")
    print()
    
    try:
        # Простой тест
        response = llm.chat([
            {"role": "user", "content": "Напиши SQL запрос для получения всех записей из таблицы users"}
        ])
        
        print("Response:")
        print(response)
        print()
        print("✅ LLM is working!")
    
    except LLMError as e:
        print(f"❌ LLM Error: {e}")
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")