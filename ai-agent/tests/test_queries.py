"""
Тесты для SQL AI Agent.

Набор тестовых вопросов для проверки качества работы Text-to-SQL системы.
Можно использовать для:
- Ручного тестирования
- Автоматизированного тестирования
- Оценки качества разных LLM моделей
- Демонстрации возможностей системы
"""

import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple
import logging
from datetime import datetime
import json

# Добавляем корневую директорию в PYTHONPATH
root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(root_dir))

from agent.config import settings
from agent.sql_agent import get_agent, SQLAgent
from agent.database import db


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestQueries:
    """
    Класс для тестирования SQL AI Agent.
    
    Содержит набор тестовых вопросов разной сложности и методы для их выполнения.
    """
    
    # Простые вопросы (базовые агрегации)
    SIMPLE_QUERIES = [
        "Сколько всего записей в таблице sales_summary?",
        "Какая была максимальная выручка за один день?",
        "Сколько клиентов в базе данных?",
        "Какое среднее значение чека?",
        "Сколько товаров в категории Электроника?",
    ]
    
    # Вопросы с фильтрацией
    FILTER_QUERIES = [
        "Какая была выручка в декабре 2024?",
        "Сколько заказов было сделано в последние 7 дней?",
        "Какие товары имеют остаток меньше 20 единиц?",
        "Сколько VIP клиентов в базе?",
        "Какая была выручка за последние 30 дней?",
    ]
    
    # Вопросы с сортировкой и лимитами
    RANKING_QUERIES = [
        "Покажи топ-5 клиентов по сумме покупок",
        "Какие 3 товара принесли больше всего выручки?",
        "Топ-10 дней с максимальной выручкой",
        "5 самых дорогих товаров по средней цене",
        "3 клиента с самым высоким средним чеком",
    ]
    
    # Вопросы с группировкой
    GROUPING_QUERIES = [
        "Средний LTV клиентов по сегментам",
        "Количество товаров в каждой категории",
        "Выручка по месяцам за 2024 год",
        "Средний чек по дням недели",
        "Распределение клиентов по сегментам",
    ]
    
    # Вопросы с подсчётом и статистикой
    STATISTICS_QUERIES = [
        "Какой средний чек за последний месяц?",
        "Какова медианная выручка за день?",
        "Какой процент клиентов относится к VIP сегменту?",
        "Какова общая выручка за весь период?",
        "Сколько в среднем заказов делает один клиент?",
    ]
    
    # Сложные вопросы (множественные условия)
    COMPLEX_QUERIES = [
        "Какие товары из категории Электроника принесли больше 100000 рублей выручки?",
        "Клиенты с более чем 10 заказами и суммой покупок больше 50000",
        "Дни когда выручка была выше среднего и количество заказов больше 50",
        "Товары которые не продавались последние 30 дней",
        "Топ-3 товара по выручке в каждой категории",
    ]
    
    # Временные вопросы
    TEMPORAL_QUERIES = [
        "Динамика продаж за последние 30 дней",
        "Сравни выручку декабря 2024 с ноябрем 2024",
        "В какой день недели обычно больше всего продаж?",
        "Какой был самый успешный месяц по выручке?",
        "Как менялся средний чек по месяцам?",
    ]
    
    # Аналитические вопросы
    ANALYTICAL_QUERIES = [
        "Какие клиенты не делали покупок последние 30 дней?",
        "Какие товары скоро закончатся на складе?",
        "Какая доля выручки приходится на VIP клиентов?",
        "Какие категории товаров самые прибыльные?",
        "Какой процент клиентов совершил больше 5 покупок?",
    ]
    
    @classmethod
    def get_all_queries(cls) -> List[str]:
        """Возвращает все тестовые вопросы"""
        return (
            cls.SIMPLE_QUERIES +
            cls.FILTER_QUERIES +
            cls.RANKING_QUERIES +
            cls.GROUPING_QUERIES +
            cls.STATISTICS_QUERIES +
            cls.COMPLEX_QUERIES +
            cls.TEMPORAL_QUERIES +
            cls.ANALYTICAL_QUERIES
        )
    
    @classmethod
    def get_queries_by_difficulty(cls, difficulty: str) -> List[str]:
        """
        Возвращает вопросы определённой сложности.
        
        Args:
            difficulty: "easy", "medium", "hard"
        
        Returns:
            Список вопросов
        """
        if difficulty == "easy":
            return cls.SIMPLE_QUERIES + cls.FILTER_QUERIES
        elif difficulty == "medium":
            return cls.RANKING_QUERIES + cls.GROUPING_QUERIES + cls.STATISTICS_QUERIES
        elif difficulty == "hard":
            return cls.COMPLEX_QUERIES + cls.TEMPORAL_QUERIES + cls.ANALYTICAL_QUERIES
        else:
            return cls.get_all_queries()


class QueryTester:
    """
    Класс для запуска тестов и сбора статистики.
    """
    
    def __init__(self, agent: SQLAgent):
        """
        Инициализация тестера.
        
        Args:
            agent: Инициализированный SQL Agent
        """
        self.agent = agent
        self.results: List[Dict[str, Any]] = []
    
    def run_single_test(
        self,
        question: str,
        expected_result: Any = None,
        show_sql: bool = True,
        show_answer: bool = True
    ) -> Dict[str, Any]:
        """
        Запускает один тест.
        
        Args:
            question: Вопрос для теста
            expected_result: Ожидаемый результат (опционально)
            show_sql: Выводить ли SQL
            show_answer: Выводить ли ответ
        
        Returns:
            Результат теста
        """
        logger.info(f"Testing: {question}")
        
        try:
            response = self.agent.ask(
                question,
                return_sql=True,
                return_intermediate_steps=False
            )
            
            result = {
                "question": question,
                "answer": response.get("answer"),
                "sql": response.get("sql"),
                "execution_time": response.get("execution_time"),
                "success": response.get("success"),
                "timestamp": datetime.now().isoformat(),
                "model": settings.LLM_MODEL
            }
            
            # Вывод результатов
            if show_sql and result["sql"]:
                logger.info(f"SQL: {result['sql']}")
            
            if show_answer:
                logger.info(f"Answer: {result['answer']}")
            
            logger.info(f"Time: {result['execution_time']}s | Success: {result['success']}")
            
            # Проверка ожидаемого результата
            if expected_result is not None:
                result["expected"] = expected_result
                result["matches_expected"] = self._check_result(result["answer"], expected_result)
                logger.info(f"Expected match: {result['matches_expected']}")
            
            self.results.append(result)
            return result
        
        except Exception as e:
            logger.error(f"Test failed: {e}")
            
            result = {
                "question": question,
                "error": str(e),
                "success": False,
                "timestamp": datetime.now().isoformat(),
                "model": settings.LLM_MODEL
            }
            
            self.results.append(result)
            return result
    
    def run_test_suite(
        self,
        queries: List[str],
        show_progress: bool = True,
        stop_on_error: bool = False
    ) -> Dict[str, Any]:
        """
        Запускает набор тестов.
        
        Args:
            queries: Список вопросов для теста
            show_progress: Показывать прогресс
            stop_on_error: Останавливаться при ошибке
        
        Returns:
            Сводная статистика
        """
        logger.info(f"Running test suite: {len(queries)} queries")
        print("=" * 80)
        
        for i, query in enumerate(queries, 1):
            if show_progress:
                print(f"\n[{i}/{len(queries)}] Testing: {query}")
                print("-" * 80)
            
            result = self.run_single_test(query, show_sql=True, show_answer=True)
            
            if not result["success"] and stop_on_error:
                logger.error("Stopping due to error")
                break
            
            print("-" * 80)
        
        # Вычисляем статистику
        stats = self.get_statistics()
        self._print_statistics(stats)
        
        return stats
    
    def _check_result(self, answer: str, expected: Any) -> bool:
        """
        Проверяет соответствие ответа ожидаемому результату.
        
        Args:
            answer: Ответ агента
            expected: Ожидаемый результат
        
        Returns:
            True если совпадает
        """
        # Простая проверка на наличие ключевых слов/чисел
        answer_lower = str(answer).lower()
        expected_str = str(expected).lower()
        
        return expected_str in answer_lower
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Вычисляет статистику по результатам тестов.
        
        Returns:
            Словарь со статистикой
        """
        if not self.results:
            return {}
        
        total = len(self.results)
        successful = len([r for r in self.results if r.get("success", False)])
        failed = total - successful
        
        execution_times = [r["execution_time"] for r in self.results if "execution_time" in r]
        avg_time = sum(execution_times) / len(execution_times) if execution_times else 0
        
        stats = {
            "total_queries": total,
            "successful": successful,
            "failed": failed,
            "success_rate": (successful / total * 100) if total > 0 else 0,
            "avg_execution_time": round(avg_time, 2),
            "min_execution_time": round(min(execution_times), 2) if execution_times else 0,
            "max_execution_time": round(max(execution_times), 2) if execution_times else 0,
            "model": settings.LLM_MODEL,
            "timestamp": datetime.now().isoformat()
        }
        
        return stats
    
    def _print_statistics(self, stats: Dict[str, Any]):
        """Выводит статистику в консоль"""
        print("\n" + "=" * 80)
        print("TEST SUITE STATISTICS")
        print("=" * 80)
        print(f"Model: {stats['model']}")
        print(f"Total queries: {stats['total_queries']}")
        print(f"Successful: {stats['successful']}")
        print(f"Failed: {stats['failed']}")
        print(f"Success rate: {stats['success_rate']:.1f}%")
        print(f"Avg execution time: {stats['avg_execution_time']}s")
        print(f"Min execution time: {stats['min_execution_time']}s")
        print(f"Max execution time: {stats['max_execution_time']}s")
        print("=" * 80)
    
    def save_results(self, filepath: str = "test_results.json"):
        """
        Сохраняет результаты тестов в JSON файл.
        
        Args:
            filepath: Путь к файлу для сохранения
        """
        output = {
            "statistics": self.get_statistics(),
            "results": self.results
        }
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Results saved to {filepath}")
    
    def reset(self):
        """Очищает результаты тестов"""
        self.results = []


def run_quick_test():
    """Быстрый тест с несколькими вопросами"""
    print("=" * 80)
    print("QUICK TEST - SQL AI AGENT")
    print("=" * 80)
    
    # Проверяем подключение к БД
    try:
        db.test_connection()
        print("✅ Database connection OK\n")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    # Создаём агента
    try:
        agent = get_agent(verbose=False)
        print(f"✅ Agent initialized: {agent}\n")
    except Exception as e:
        print(f"❌ Agent initialization failed: {e}")
        return
    
    # Запускаем несколько тестов
    tester = QueryTester(agent)
    
    quick_tests = [
        "Сколько всего записей в таблице sales_summary?",
        "Какая была выручка в декабре 2024?",
        "Покажи топ-3 клиента по сумме покупок"
    ]
    
    tester.run_test_suite(quick_tests, show_progress=True)
    
    # Сохраняем результаты
    tester.save_results("quick_test_results.json")


def run_full_test():
    """Полный тест со всеми вопросами"""
    print("=" * 80)
    print("FULL TEST SUITE - SQL AI AGENT")
    print("=" * 80)
    
    # Проверяем подключение
    try:
        db.test_connection()
        print("✅ Database connection OK\n")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    # Создаём агента
    try:
        agent = get_agent(verbose=False)
        print(f"✅ Agent initialized: {agent}\n")
    except Exception as e:
        print(f"❌ Agent initialization failed: {e}")
        return
    
    # Запускаем все тесты
    tester = QueryTester(agent)
    all_queries = TestQueries.get_all_queries()
    
    print(f"Total queries to test: {len(all_queries)}\n")
    
    tester.run_test_suite(all_queries, show_progress=True, stop_on_error=False)
    
    # Сохраняем результаты
    tester.save_results("full_test_results.json")


def run_difficulty_test(difficulty: str = "easy"):
    """
    Тест вопросов определённой сложности.
    
    Args:
        difficulty: "easy", "medium", "hard"
    """
    print("=" * 80)
    print(f"DIFFICULTY TEST: {difficulty.upper()}")
    print("=" * 80)
    
    # Проверяем подключение
    try:
        db.test_connection()
        agent = get_agent(verbose=False)
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        return
    
    # Получаем вопросы по сложности
    queries = TestQueries.get_queries_by_difficulty(difficulty)
    
    print(f"Testing {len(queries)} {difficulty} queries\n")
    
    # Запускаем тесты
    tester = QueryTester(agent)
    tester.run_test_suite(queries, show_progress=True)
    
    # Сохраняем результаты
    tester.save_results(f"{difficulty}_test_results.json")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="SQL AI Agent Test Suite")
    parser.add_argument(
        "--mode",
        choices=["quick", "full", "easy", "medium", "hard"],
        default="quick",
        help="Test mode to run"
    )
    
    args = parser.parse_args()
    
    if args.mode == "quick":
        run_quick_test()
    elif args.mode == "full":
        run_full_test()
    else:
        run_difficulty_test(args.mode)