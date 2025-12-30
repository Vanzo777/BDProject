"""
SQL Agent module для SQL AI Agent с визуализацией.
"""

from typing import List, Dict, Any, Optional, Tuple
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain_community.agent_toolkits import create_sql_agent, SQLDatabaseToolkit
from langchain.agents import AgentExecutor
import logging
import time
import pandas as pd

from agent.config import settings, get_db_uri
from agent.semantic_layer import SemanticLayer
from agent.database import db
from agent.visualizer import get_visualizer

logger = logging.getLogger(__name__)


class SQLAgent:
    """
    SQL Agent для Text-to-SQL системы с визуализацией.
    """
    
    def __init__(
        self,
        semantic_layer_path: Optional[str] = None,
        use_few_shot: bool = True,
        verbose: bool = True,
        enable_visualization: bool = True
    ):
        """
        Инициализация SQL Agent.
        
        Args:
            semantic_layer_path: Путь к semantic_layer.yaml
            use_few_shot: Использовать few-shot примеры
            verbose: Детальные логи
            enable_visualization: Включить визуализацию
        """
        self.verbose = verbose
        self.use_few_shot = use_few_shot
        self.enable_visualization = enable_visualization
        
        # Загружаем semantic layer
        semantic_path = semantic_layer_path or settings.SEMANTIC_LAYER_PATH
        self.semantic_layer = SemanticLayer.from_yaml(semantic_path)
        
        logger.info(f"Semantic layer loaded: {self.semantic_layer}")
        
        # Проверяем подключение к БД
        db.test_connection()
        
        # Создаём LangChain SQLDatabase wrapper
        self.sql_database = SQLDatabase.from_uri(
            get_db_uri(),
            schema=settings.DB_SCHEMA,
            include_tables=self.semantic_layer.list_tables(),
            sample_rows_in_table_info=2
        )
        
        # Создаём LLM
        self.llm = ChatOpenAI(
            model=settings.LLM_MODEL,
            temperature=settings.LLM_TEMPERATURE,
            max_tokens=settings.LLM_MAX_TOKENS,
            openai_api_key=settings.OPENROUTER_API_KEY,
            base_url=settings.OPENROUTER_BASE_URL,
            default_headers={
                "HTTP-Referer": "https://sql-ai-agent.local",
                "X-Title": "SQL AI Agent"
            }
        )
        
        # SQL toolkit
        self.toolkit = SQLDatabaseToolkit(db=self.sql_database, llm=self.llm)
        
        # System prompt
        self.system_prompt = self._build_system_prompt()
        
        # Создаём агента
        self.agent_executor = self._create_agent()
        
        # Visualizer
        if self.enable_visualization:
            self.visualizer = get_visualizer()
        else:
            self.visualizer = None
        
        logger.info("SQL Agent initialized successfully")
    
    def _build_system_prompt(self) -> str:
        """Формирует system prompt"""
        prompt_parts = []
        prompt_parts.append(self.semantic_layer.get_system_prompt())
        
        if self.use_few_shot:
            examples = self.semantic_layer.get_few_shot_examples(
                limit=settings.FEW_SHOT_EXAMPLES_LIMIT
            )
            if examples:
                prompt_parts.append("\n")
                prompt_parts.append(examples)
        
        return "\n".join(prompt_parts)
    
    def _create_agent(self) -> AgentExecutor:
        """Создаёт LangChain SQL Agent"""
        agent_executor = create_sql_agent(
            llm=self.llm,
            toolkit=self.toolkit,
            verbose=self.verbose,
            agent_type="openai-tools",
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
        return_intermediate_steps: bool = False,
        return_visualization: bool = None
    ) -> Dict[str, Any]:
        """
        Задаёт вопрос агенту
        
        Args:
            question: Вопрос на естественном языке
            return_sql: Возвращать SQL
            return_intermediate_steps: Возвращать промежуточные шаги
            return_visualization: Возвращать визуализацию (None = auto)
        
        Returns:
            Словарь с ответом и визуализацией
        """
        logger.info(f"Question received: {question}")
        
        if return_visualization is None:
            return_visualization = self.enable_visualization
        
        start_time = time.time()
        
        try:
            # Выполняем запрос через агента
            result = self.agent_executor.invoke({"input": question})
            
            execution_time = time.time() - start_time
            
            # Формируем ответ
            response = {
                "answer": result.get("output", "Не удалось получить ответ"),
                "execution_time": round(execution_time, 2),
                "success": True
            }
            
            if return_intermediate_steps:
                response["intermediate_steps"] = result.get("intermediate_steps", [])
            
            # Извлекаем SQL
            sql = self._extract_sql_from_steps(result.get("intermediate_steps", []))
            if return_sql:
                response["sql"] = sql
            
            # Визуализация
            if return_visualization and sql and self.visualizer:
                try:
                    df = pd.read_sql(sql, db.engine)
                    visualization, chart_recommendation = self.visualizer.create_visualization(
                        df=df,
                        query=question,
                        sql=sql
                    )
                    response["visualization"] = visualization
                    response["chart_recommendation"] = chart_recommendation.to_dict() if chart_recommendation else None
                    response["dataframe"] = df
                except Exception as e:
                    logger.error(f"Ошибка визуализации: {e}")
            
            logger.info(f"Question answered in {execution_time:.2f}s")
            return response
        
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Failed to answer: {e}")
            
            return {
                "answer": f"Ошибка: {str(e)}",
                "execution_time": round(execution_time, 2),
                "success": False,
                "error": str(e)
            }
    
    def _extract_sql_from_steps(self, intermediate_steps: List[Tuple]) -> Optional[str]:
        """Извлекает SQL из промежуточных шагов"""
        if not intermediate_steps:
            return None
        
        for step in reversed(intermediate_steps):
            if not isinstance(step, tuple) or len(step) < 2:
                continue
            
            action, observation = step
            
            tool_name = None
            tool_input = None
            
            if hasattr(action, 'tool'):
                tool_name = action.tool
                tool_input = action.tool_input
            elif isinstance(action, dict):
                tool_name = action.get('tool')
                tool_input = action.get('tool_input')
            
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
    
    def switch_model(self, model: str):
        """Переключает LLM модель"""
        logger.info(f"Switching model to: {model}")
        
        self.llm = ChatOpenAI(
            model=model,
            temperature=settings.LLM_TEMPERATURE,
            max_tokens=settings.LLM_MAX_TOKENS,
            openai_api_key=settings.OPENROUTER_API_KEY,
            base_url=settings.OPENROUTER_BASE_URL,
            default_headers={
                "HTTP-Referer": "https://sql-ai-agent.local",
                "X-Title": "SQL AI Agent"
            }
        )
        
        self.toolkit = SQLDatabaseToolkit(db=self.sql_database, llm=self.llm)
        self.agent_executor = self._create_agent()
        
        logger.info(f"Model switched to: {model}")
    
    def test_connection(self) -> bool:
        """Проверить подключение к БД"""
        try:
            db.test_connection()
            return True
        except:
            return False


_agent_instance: Optional[SQLAgent] = None


def get_agent(
    semantic_layer_path: Optional[str] = None,
    use_few_shot: bool = True,
    verbose: bool = False,
    enable_visualization: bool = True,
    force_new: bool = False
) -> SQLAgent:
    """Получает singleton instance SQL Agent с визуализацией"""
    global _agent_instance
    
    if force_new or _agent_instance is None:
        _agent_instance = SQLAgent(
            semantic_layer_path=semantic_layer_path,
            use_few_shot=use_few_shot,
            verbose=verbose,
            enable_visualization=enable_visualization
        )
    
    return _agent_instance


def ask_question(question: str, return_sql: bool = True, return_visualization: bool = True) -> Dict[str, Any]:
    """Быстрый вопрос с визуализацией"""
    agent = get_agent()
    return agent.ask(question, return_sql=return_sql, return_visualization=return_visualization)