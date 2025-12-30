"""
Streamlit UI для SQL AI Agent с визуализацией
"""

import streamlit as st
import pandas as pd
import time
import sys
from pathlib import Path
from datetime import datetime
import plotly.graph_objects as go

# Добавляем корневую директорию проекта в PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from agent.sql_agent import get_agent
from agent.config import settings
from agent.visualizer import ChartType

# Настройка страницы
st.set_page_config(
    page_title="SQL AI Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Кастомный CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #555;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .success-box {
        background-color: #d4edda;
        border-left: 5px solid #28a745;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border-left: 5px solid #dc3545;
        padding: 1rem;
        margin: 1rem 0;
    }
    .sql-code {
        background-color: #282c34;
        color: #abb2bf;
        padding: 1rem;
        border-radius: 0.5rem;
        font-family: 'Courier New', monospace;
        font-size: 0.9rem;
        overflow-x: auto;
    }
</style>
""", unsafe_allow_html=True)


def init_session_state():
    """Инициализация session state"""
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    
    if 'agent' not in st.session_state:
        with st.spinner("🚀 Инициализация AI агента..."):
            st.session_state.agent = get_agent(
                use_few_shot=True,
                verbose=False,
                enable_visualization=True
            )
    
    if 'stats' not in st.session_state:
        st.session_state.stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'total_time': 0.0
        }
    
    if 'show_visualization' not in st.session_state:
        st.session_state.show_visualization = True


def render_header():
    """Отрисовка заголовка"""
    st.markdown('<div class="main-header">🤖 SQL AI Agent</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Задавайте вопросы на русском — получайте SQL + Визуализацию</div>',
        unsafe_allow_html=True
    )


def render_sidebar():
    """Отрисовка боковой панели"""
    with st.sidebar:
        st.title("⚙️ Настройки")
        
        # Информация о текущей модели
        st.info(f"**Модель:** {settings.LLM_MODEL}")
        
        st.divider()
        
        # Смена модели
        st.subheader("🔄 Сменить модель")
        
        available_models = [
            "google/gemini-2.0-flash-exp:free",
            "mistralai/devstral-2512:free",
            "qwen/qwen3-coder:free",
            "deepseek/deepseek-v3.2",
            "google/gemini-2.5-flash-lite",            
        ]
        
        selected_model = st.selectbox(
            "Выберите модель:",
            available_models,
            index=available_models.index(settings.LLM_MODEL)
            if settings.LLM_MODEL in available_models else 0
        )
        
        if st.button("✅ Применить модель", use_container_width=True):
            with st.spinner("Смена модели..."):
                st.session_state.agent.switch_model(selected_model)
                st.success(f"Модель изменена на {selected_model}")
                time.sleep(1)
                st.rerun()
        
        st.divider()
        
        # Настройки визуализации
        st.subheader("📊 Визуализация")
        
        show_viz = st.checkbox(
            "Показывать графики",
            value=st.session_state.show_visualization,
            help="Автоматически генерировать графики для подходящих запросов"
        )
        st.session_state.show_visualization = show_viz
        
        st.divider()
        
        # Статистика сессии
        st.subheader("📈 Статистика сессии")
        
        stats = st.session_state.stats
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Всего запросов", stats['total_queries'])
            st.metric("Успешных", stats['successful_queries'])
        with col2:
            st.metric("Ошибок", stats['failed_queries'])
            if stats['total_queries'] > 0:
                avg_time = stats['total_time'] / stats['total_queries']
                st.metric("Среднее время", f"{avg_time:.2f}s")
        
        st.divider()
        
        # Информация о БД
        st.subheader("💾 База данных")
        st.text(f"Host: {settings.DB_HOST}")
        st.text(f"DB: {settings.DB_NAME}")
        st.text(f"Schema: {settings.DB_SCHEMA}")
        
        if st.button("🔍 Проверить подключение", use_container_width=True):
            if st.session_state.agent.test_connection():
                st.success("✅ Подключение активно")
            else:
                st.error("❌ Ошибка подключения")
        
        st.divider()
        
        # Очистка истории
        if st.button("🗑️ Очистить историю", use_container_width=True):
            st.session_state.messages = []
            st.session_state.stats = {
                'total_queries': 0,
                'successful_queries': 0,
                'failed_queries': 0,
                'total_time': 0.0
            }
            st.rerun()


def render_examples():
    """Отрисовка примеров вопросов"""
    with st.expander("💡 Примеры вопросов", expanded=False):
        st.markdown("""
        **Простые запросы:**
        - Сколько клиентов в базе?
        - Какая была максимальная выручка за один день?
        
        **С фильтрацией:**
        - Какая была выручка в декабре 2024?
        - Сколько заказов было за последние 7 дней?
        
        **Топ-N и рейтинги:**
        - Покажи топ-5 клиентов по сумме покупок
        - Какие 3 товара принесли больше всего выручки?
        
        **Аналитические:**
        - Средний LTV клиентов по сегментам
        - Выручка по месяцам за 2024 год
        """)


def render_message(message: dict):
    """Отрисовка одного сообщения"""
    role = message["role"]
    content = message["content"]
    
    if role == "user":
        with st.chat_message("user", avatar="👤"):
            st.write(content)
    
    else:  # assistant
        with st.chat_message("assistant", avatar="🤖"):
            # Основной ответ
            st.write(content["answer"])
            
            # SQL запрос
            if "sql" in content and content["sql"]:
                with st.expander("📝 SQL запрос", expanded=False):
                    st.code(content["sql"], language="sql")
            
            # Визуализация
            if "visualization" in content:
                viz = content["visualization"]
                chart_rec = content.get("chart_recommendation")
                
                if viz is not None:
                    st.divider()
                    
                    # Заголовок графика с рекомендацией
                    if chart_rec:
                        chart_type = chart_rec.get("chart_type", "unknown")
                        confidence = chart_rec.get("confidence", 0)
                        reason = chart_rec.get("reason", "")
                        
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.subheader("📊 Визуализация")
                        with col2:
                            st.caption(f"Тип: {chart_type}")
                        
                        if reason:
                            st.caption(f"💡 {reason}")
                    
                    # Отрисовка графика
                    st.plotly_chart(viz, use_container_width=True)
                
                elif chart_rec and chart_rec.get("chart_type") == "table":
                    # Если рекомендована таблица, показываем DataFrame
                    if "dataframe" in content and content["dataframe"] is not None:
                        st.divider()
                        st.subheader("📋 Данные")
                        st.dataframe(content["dataframe"], use_container_width=True)
            
            # Метаданные
            execution_time = content.get("execution_time", 0)
            st.caption(f"⏱️ Время выполнения: {execution_time}s")


def render_chat():
    """Отрисовка чата"""
    # Отображаем историю сообщений
    for message in st.session_state.messages:
        render_message(message)
    
    # Поле ввода
    question = st.chat_input("Введите ваш вопрос...")
    
    if question:
        # Добавляем вопрос пользователя
        st.session_state.messages.append({
            "role": "user",
            "content": question
        })
        
        # Отображаем вопрос
        with st.chat_message("user", avatar="👤"):
            st.write(question)
        
        # Получаем ответ от агента
        with st.chat_message("assistant", avatar="🤖"):
            with st.spinner("🤔 Думаю..."):
                response = st.session_state.agent.ask(
                    question=question,
                    return_sql=True,
                    return_visualization=st.session_state.show_visualization
                )
                
                # Обновляем статистику
                st.session_state.stats['total_queries'] += 1
                st.session_state.stats['total_time'] += response.get('execution_time', 0)
                
                if response.get('success', False):
                    st.session_state.stats['successful_queries'] += 1
                else:
                    st.session_state.stats['failed_queries'] += 1
                
                # Добавляем ответ в историю
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": response
                })
                
                # Отрисовываем ответ
                st.rerun()


def main():
    """Главная функция приложения"""
    init_session_state()
    render_header()
    render_sidebar()
    render_examples()
    render_chat()


if __name__ == "__main__":
    main()