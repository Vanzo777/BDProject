"""
Streamlit UI для SQL AI Agent.

Предоставляет веб-интерфейс для взаимодействия с Text-to-SQL системой:
- Чат с AI агентом
- Отображение сгенерированного SQL
- Просмотр результатов в виде таблиц
- История вопросов
- Настройки модели
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
import sys
from pathlib import Path

# Добавляем корневую директорию в PYTHONPATH
root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(root_dir))

from agent.config import settings
from agent.sql_agent import get_agent, SQLAgent
from agent.database import db, DatabaseConnectionError


# Настройка логирования
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Настройка страницы
st.set_page_config(
    page_title=settings.STREAMLIT_TITLE,
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)


# Кастомные стили
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 2rem;
        color: #1f77b4;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        margin: 1rem 0;
    }
    .error-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        margin: 1rem 0;
    }
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        margin: 1rem 0;
    }
    .sql-code {
        background-color: #f4f4f4;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        font-family: monospace;
        margin: 1rem 0;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
        border: 1px solid #dee2e6;
    }
</style>
""", unsafe_allow_html=True)


def init_session_state():
    """Инициализация session state для хранения истории и настроек"""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    if "agent" not in st.session_state:
        st.session_state.agent = None
    
    if "agent_initialized" not in st.session_state:
        st.session_state.agent_initialized = False
    
    if "show_sql" not in st.session_state:
        st.session_state.show_sql = True
    
    if "show_execution_time" not in st.session_state:
        st.session_state.show_execution_time = True
    
    if "current_model" not in st.session_state:
        st.session_state.current_model = settings.LLM_MODEL


def initialize_agent() -> Optional[SQLAgent]:
    """
    Инициализирует SQL Agent.
    
    Returns:
        SQLAgent или None если инициализация не удалась
    """
    if st.session_state.agent_initialized and st.session_state.agent is not None:
        return st.session_state.agent
    
    try:
        with st.spinner("🔄 Инициализация AI агента..."):
            # Проверяем подключение к БД
            db.test_connection()
            
            # Создаём агента
            agent = get_agent(
                use_few_shot=True,
                verbose=(settings.LOG_LEVEL == "DEBUG")
            )
            
            st.session_state.agent = agent
            st.session_state.agent_initialized = True
            
            logger.info("Agent initialized successfully")
            return agent
    
    except DatabaseConnectionError as e:
        st.error(f"❌ Ошибка подключения к базе данных: {e}")
        st.info("💡 Убедитесь что PostgreSQL запущен: `docker-compose up -d`")
        return None
    
    except Exception as e:
        st.error(f"❌ Ошибка инициализации агента: {e}")
        logger.error(f"Agent initialization failed: {e}", exc_info=True)
        return None


def display_message(role: str, content: str, metadata: Optional[Dict[str, Any]] = None):
    """
    Отображает сообщение в чате.
    
    Args:
        role: "user" или "assistant"
        content: Текст сообщения
        metadata: Дополнительная информация (SQL, время выполнения и т.д.)
    """
    with st.chat_message(role):
        st.markdown(content)
        
        # Отображаем метаданные если есть
        if metadata:
            # SQL запрос
            if metadata.get("sql") and st.session_state.show_sql:
                with st.expander("📝 Сгенерированный SQL", expanded=False):
                    st.code(metadata["sql"], language="sql")
            
            # Время выполнения
            if metadata.get("execution_time") and st.session_state.show_execution_time:
                st.caption(f"⏱️ Время выполнения: {metadata['execution_time']}s")
            
            # Результаты в виде таблицы
            if metadata.get("results_df") is not None:
                with st.expander("📊 Результаты запроса", expanded=False):
                    st.dataframe(metadata["results_df"], use_container_width=True)


def add_message(role: str, content: str, metadata: Optional[Dict[str, Any]] = None):
    """
    Добавляет сообщение в историю.
    
    Args:
        role: "user" или "assistant"
        content: Текст сообщения
        metadata: Дополнительная информация
    """
    st.session_state.messages.append({
        "role": role,
        "content": content,
        "metadata": metadata or {},
        "timestamp": datetime.now().isoformat()
    })


def process_question(agent: SQLAgent, question: str):
    """
    Обрабатывает вопрос пользователя.
    
    Args:
        agent: SQL Agent
        question: Вопрос пользователя
    """
    # Добавляем вопрос в историю
    add_message("user", question)
    
    # Отображаем вопрос
    with st.chat_message("user"):
        st.markdown(question)
    
    # Получаем ответ от агента
    with st.chat_message("assistant"):
        with st.spinner("🤔 Думаю..."):
            try:
                response = agent.ask(
                    question,
                    return_sql=st.session_state.show_sql,
                    return_intermediate_steps=False
                )
                
                answer = response.get("answer", "Не удалось получить ответ")
                success = response.get("success", False)
                
                # Отображаем ответ
                if success:
                    st.markdown(answer)
                else:
                    st.error(f"❌ {answer}")
                
                # Метаданные
                metadata = {
                    "execution_time": response.get("execution_time"),
                    "success": success
                }
                
                # SQL
                if response.get("sql"):
                    metadata["sql"] = response["sql"]
                    
                    if st.session_state.show_sql:
                        with st.expander("📝 Сгенерированный SQL", expanded=False):
                            st.code(response["sql"], language="sql")
                
                # Время выполнения
                if st.session_state.show_execution_time and response.get("execution_time"):
                    st.caption(f"⏱️ Время выполнения: {response['execution_time']}s")
                
                # Добавляем в историю
                add_message("assistant", answer, metadata)
            
            except Exception as e:
                error_msg = f"Ошибка при обработке вопроса: {str(e)}"
                st.error(f"❌ {error_msg}")
                add_message("assistant", error_msg, {"success": False})
                logger.error(f"Error processing question: {e}", exc_info=True)


def render_sidebar():
    """Отображает боковую панель с настройками и информацией"""
    with st.sidebar:
        st.markdown("## ⚙️ Настройки")
        
        # Настройки отображения
        st.markdown("### 📊 Отображение")
        st.session_state.show_sql = st.checkbox(
            "Показывать SQL запросы",
            value=st.session_state.show_sql
        )
        st.session_state.show_execution_time = st.checkbox(
            "Показывать время выполнения",
            value=st.session_state.show_execution_time
        )
        
        st.markdown("---")
        
        # Информация о модели
        st.markdown("### 🤖 Модель LLM")
        st.info(f"**Текущая модель:**\n\n`{st.session_state.current_model}`")
        st.caption(f"Temperature: {settings.LLM_TEMPERATURE}")
        st.caption(f"Max tokens: {settings.LLM_MAX_TOKENS}")
        
        # Смена модели
        with st.expander("🔄 Сменить модель", expanded=False):
            available_models = [
                "google/gemini-2.0-flash-exp:free",
                "google/gemini-2.5-flash-lite",
                "deepseek/deepseek-v3.2",
            ]
            
            new_model = st.selectbox(
                "Выберите модель:",
                options=available_models,
                index=available_models.index(st.session_state.current_model) 
                      if st.session_state.current_model in available_models else 0
            )
            
            if st.button("Применить"):
                if st.session_state.agent:
                    with st.spinner("Переключение модели..."):
                        st.session_state.agent.switch_model(new_model)
                        st.session_state.current_model = new_model
                        st.success(f"✅ Модель изменена на {new_model}")
                        st.rerun()
        
        st.markdown("---")
        
        # Информация о базе данных
        st.markdown("### 🗄️ База данных")
        
        if st.session_state.agent_initialized:
            try:
                tables = st.session_state.agent.list_tables()
                st.success("✅ Подключено")
                st.caption(f"База: `{settings.DB_NAME}`")
                st.caption(f"Схема: `{settings.DB_SCHEMA}`")
                st.caption(f"Таблиц: {len(tables)}")
                
                with st.expander("📋 Список таблиц", expanded=False):
                    for table in tables:
                        st.text(f"• {table}")
            except Exception as e:
                st.error(f"❌ Ошибка: {str(e)}")
        else:
            st.warning("⚠️ Не подключено")
        
        st.markdown("---")
        
        # Статистика сессии
        st.markdown("### 📈 Статистика")
        total_questions = len([m for m in st.session_state.messages if m["role"] == "user"])
        st.metric("Вопросов задано", total_questions)
        
        st.markdown("---")
        
        # Действия
        st.markdown("### 🔧 Действия")
        
        if st.button("🗑️ Очистить историю", use_container_width=True):
            st.session_state.messages = []
            st.rerun()
        
        if st.button("🔄 Переинициализировать агента", use_container_width=True):
            st.session_state.agent_initialized = False
            st.session_state.agent = None
            st.rerun()
        
        st.markdown("---")
        
        # Информация о приложении
        st.markdown("### ℹ️ О приложении")
        st.caption("**SQL AI Agent v1.0**")
        st.caption("Text-to-SQL система на базе LangChain")
        st.caption(f"Режим: {'🔧 Dev' if settings.is_development() else '🚀 Prod'}")


def render_example_questions():
    """Отображает примеры вопросов"""
    st.markdown("### 💡 Примеры вопросов:")
    
    examples = [
        "Какая была выручка в декабре 2024?",
        "Покажи топ-5 клиентов по сумме покупок",
        "Какой средний чек за последние 30 дней?",
        "Сколько VIP клиентов в базе?",
        "Какие товары приносят больше всего выручки?",
        "Какая динамика продаж за последнюю неделю?",
        "Какие товары заканчиваются на складе?"
    ]
    
    cols = st.columns(3)
    
    for i, example in enumerate(examples):
        col_idx = i % 3
        with cols[col_idx]:
            if st.button(example, key=f"example_{i}", use_container_width=True):
                return example
    
    return None


def main():
    """Главная функция приложения"""
    # Инициализация
    init_session_state()
    
    # Заголовок
    st.markdown(
        f'<div class="main-header">🤖 {settings.STREAMLIT_TITLE}</div>',
        unsafe_allow_html=True
    )
    
    # Боковая панель
    render_sidebar()
    
    # Инициализация агента
    agent = initialize_agent()
    
    if agent is None:
        st.error("❌ Не удалось инициализировать агента. Проверьте настройки и логи.")
        st.stop()
    
    # Приветственное сообщение
    if len(st.session_state.messages) == 0:
        st.markdown("""
        <div class="info-box">
            <h3>👋 Добро пожаловать в SQL AI Agent!</h3>
            <p>Я помогу вам анализировать данные с помощью естественного языка.</p>
            <p>Просто задайте вопрос о ваших данных, и я сгенерирую SQL запрос и выполню его.</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Примеры вопросов
        example_clicked = render_example_questions()
        
        if example_clicked:
            process_question(agent, example_clicked)
            st.rerun()
    
    # История сообщений
    for message in st.session_state.messages:
        display_message(
            message["role"],
            message["content"],
            message.get("metadata")
        )
    
    # Поле ввода
    if question := st.chat_input("Задайте вопрос к данным..."):
        process_question(agent, question)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ Критическая ошибка: {e}")
        logger.error(f"Critical error in main: {e}", exc_info=True)