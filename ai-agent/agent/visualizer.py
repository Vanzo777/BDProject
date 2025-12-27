"""
Модуль для автоматической визуализации результатов SQL запросов.
Анализирует DataFrame и генерирует подходящие графики.
"""

import logging
from typing import Optional, Dict, Any, Tuple, List
from enum import Enum
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, date

logger = logging.getLogger(__name__)


class ChartType(Enum):
    """Типы поддерживаемых графиков"""
    BAR = "bar"
    LINE = "line"
    PIE = "pie"
    HORIZONTAL_BAR = "horizontal_bar"
    SCATTER = "scatter"
    TABLE = "table"
    NONE = "none"


class ChartRecommendation:
    """Рекомендация по визуализации"""
    
    def __init__(
        self,
        chart_type: ChartType,
        title: str,
        x_column: Optional[str] = None,
        y_column: Optional[str] = None,
        confidence: float = 0.0,
        reason: str = ""
    ):
        self.chart_type = chart_type
        self.title = title
        self.x_column = x_column
        self.y_column = y_column
        self.confidence = confidence
        self.reason = reason
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в словарь"""
        return {
            "chart_type": self.chart_type.value,
            "title": self.title,
            "x_column": self.x_column,
            "y_column": self.y_column,
            "confidence": self.confidence,
            "reason": self.reason
        }


class DataFrameAnalyzer:
    """Анализатор DataFrame для определения типа визуализации"""
    
    @staticmethod
    def analyze_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Анализирует структуру DataFrame
        
        Returns:
            Dict с метаданными о данных
        """
        if df.empty:
            return {
                "is_empty": True,
                "num_rows": 0,
                "num_cols": 0
            }
        
        analysis = {
            "is_empty": False,
            "num_rows": len(df),
            "num_cols": len(df.columns),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "numeric_columns": [],
            "categorical_columns": [],
            "datetime_columns": [],
            "text_columns": []
        }
        
        # Классифицируем колонки по типам
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                analysis["numeric_columns"].append(col)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                analysis["datetime_columns"].append(col)
            elif pd.api.types.is_object_dtype(df[col]):
                # Проверяем, может это дата в строковом формате
                if DataFrameAnalyzer._is_date_column(df[col]):
                    analysis["datetime_columns"].append(col)
                # Проверяем уникальность - если мало уникальных значений, то категория
                elif df[col].nunique() < len(df) * 0.5 and df[col].nunique() < 50:
                    analysis["categorical_columns"].append(col)
                else:
                    analysis["text_columns"].append(col)
        
        # Определяем является ли результат одним числом
        analysis["is_single_value"] = (len(df) == 1 and len(analysis["numeric_columns"]) == 1)
        
        # Определяем есть ли временной ряд
        analysis["has_timeseries"] = len(analysis["datetime_columns"]) > 0
        
        # Определяем есть ли категориальные данные
        analysis["has_categories"] = len(analysis["categorical_columns"]) > 0
        
        return analysis
    
    @staticmethod
    def _is_date_column(series: pd.Series, sample_size: int = 100) -> bool:
        """Проверяет, является ли колонка датой в строковом формате"""
        try:
            sample = series.dropna().head(sample_size)
            if len(sample) == 0:
                return False
            
            # Пробуем распарсить как даты
            parsed = pd.to_datetime(sample, errors='coerce')
            success_rate = parsed.notna().sum() / len(sample)
            
            return success_rate > 0.8
        except:
            return False


class VisualizationEngine:
    """Движок для выбора и генерации визуализаций"""
    
    def __init__(self):
        self.analyzer = DataFrameAnalyzer()
    
    def recommend_visualization(
        self,
        df: pd.DataFrame,
        query: str,
        sql: str
    ) -> ChartRecommendation:
        """
        Рекомендует тип визуализации на основе анализа данных и запроса
        
        Args:
            df: DataFrame с результатами
            query: Исходный вопрос пользователя
            sql: Сгенерированный SQL запрос
            
        Returns:
            ChartRecommendation с рекомендацией
        """
        analysis = self.analyzer.analyze_dataframe(df)
        
        # Случай 1: Пустые данные
        if analysis["is_empty"]:
            return ChartRecommendation(
                chart_type=ChartType.NONE,
                title="Нет данных для визуализации",
                confidence=1.0,
                reason="Запрос не вернул результатов"
            )
        
        # Случай 2: Одно значение
        if analysis["is_single_value"]:
            return ChartRecommendation(
                chart_type=ChartType.NONE,
                title="Единичное значение",
                confidence=1.0,
                reason="Результат - одно число, график не требуется"
            )
        
        # Случай 3: Слишком много строк без агрегации
        if analysis["num_rows"] > 100 and not self._is_aggregated_query(sql):
            return ChartRecommendation(
                chart_type=ChartType.TABLE,
                title="Детальные данные",
                confidence=0.8,
                reason="Слишком много строк, лучше показать таблицу"
            )
        
        # Случай 4: Временной ряд
        if analysis["has_timeseries"] and len(analysis["numeric_columns"]) > 0:
            return self._recommend_timeseries_chart(df, analysis, query)
        
        # Случай 5: Топ-N или рейтинг
        if self._is_top_n_query(query, sql, analysis):
            return self._recommend_ranking_chart(df, analysis, query)
        
        # Случай 6: Распределение/доли (для pie chart)
        if self._is_distribution_query(query) and analysis["num_rows"] <= 10:
            return self._recommend_distribution_chart(df, analysis, query)
        
        # Случай 7: Сравнение категорий
        if analysis["has_categories"] and len(analysis["numeric_columns"]) > 0:
            return self._recommend_category_comparison_chart(df, analysis, query)
        
        # Случай 8: Корреляция двух метрик
        if len(analysis["numeric_columns"]) >= 2 and analysis["num_rows"] > 5:
            return self._recommend_scatter_chart(df, analysis, query)
        
        # По умолчанию - таблица
        return ChartRecommendation(
            chart_type=ChartType.TABLE,
            title="Результаты запроса",
            confidence=0.5,
            reason="Не удалось определить оптимальный тип графика"
        )
    
    def _is_aggregated_query(self, sql: str) -> bool:
        """Проверяет содержит ли SQL агрегацию"""
        sql_lower = sql.lower()
        return any(kw in sql_lower for kw in [
            'group by', 'sum(', 'count(', 'avg(', 'max(', 'min(',
            'having'
        ])
    
    def _is_top_n_query(self, query: str, sql: str, analysis: Dict) -> bool:
        """Проверяет является ли запрос топ-N"""
        query_lower = query.lower()
        sql_lower = sql.lower()
        
        # Проверяем ключевые слова в вопросе
        has_top_keyword = any(kw in query_lower for kw in [
            'топ', 'top', 'лучш', 'наиболее', 'самы', 'больше всего',
            'максимальн', 'наибольш'
        ])
        
        # Проверяем наличие LIMIT в SQL
        has_limit = 'limit' in sql_lower
        
        # Проверяем наличие ORDER BY
        has_order = 'order by' in sql_lower
        
        return (has_top_keyword or has_limit) and has_order
    
    def _is_distribution_query(self, query: str) -> bool:
        """Проверяет запрашивается ли распределение/доли"""
        query_lower = query.lower()
        return any(kw in query_lower for kw in [
            'распределени', 'доля', 'процент', 'соотношени',
            'структура', 'состав'
        ])
    
    def _recommend_timeseries_chart(
        self,
        df: pd.DataFrame,
        analysis: Dict,
        query: str
    ) -> ChartRecommendation:
        """Рекомендация для временных рядов"""
        date_col = analysis["datetime_columns"][0]
        numeric_col = analysis["numeric_columns"][0] if analysis["numeric_columns"] else None
        
        if not numeric_col:
            return ChartRecommendation(
                chart_type=ChartType.TABLE,
                title="Временные данные",
                confidence=0.6,
                reason="Есть даты, но нет числовых метрик"
            )
        
        return ChartRecommendation(
            chart_type=ChartType.LINE,
            title=self._generate_title(query, "Динамика"),
            x_column=date_col,
            y_column=numeric_col,
            confidence=0.9,
            reason="Временной ряд с числовой метрикой"
        )
    
    def _recommend_ranking_chart(
        self,
        df: pd.DataFrame,
        analysis: Dict,
        query: str
    ) -> ChartRecommendation:
        """Рекомендация для топ-N/рейтингов"""
        # Первая колонка обычно категория, вторая - значение
        category_col = None
        value_col = None
        
        if analysis["categorical_columns"]:
            category_col = analysis["categorical_columns"][0]
        elif analysis["text_columns"]:
            category_col = analysis["text_columns"][0]
        
        if analysis["numeric_columns"]:
            value_col = analysis["numeric_columns"][0]
        
        if not category_col or not value_col:
            return ChartRecommendation(
                chart_type=ChartType.TABLE,
                title="Рейтинг",
                confidence=0.6,
                reason="Не удалось определить категорию и значение"
            )
        
        # Если названия категорий длинные - горизонтальная диаграмма
        max_label_length = df[category_col].astype(str).str.len().max()
        chart_type = ChartType.HORIZONTAL_BAR if max_label_length > 15 else ChartType.BAR
        
        return ChartRecommendation(
            chart_type=chart_type,
            title=self._generate_title(query, "Топ"),
            x_column=category_col,
            y_column=value_col,
            confidence=0.95,
            reason="Топ-N запрос с категориями и числовыми значениями"
        )
    
    def _recommend_distribution_chart(
        self,
        df: pd.DataFrame,
        analysis: Dict,
        query: str
    ) -> ChartRecommendation:
        """Рекомендация для распределений"""
        category_col = analysis["categorical_columns"][0] if analysis["categorical_columns"] else None
        value_col = analysis["numeric_columns"][0] if analysis["numeric_columns"] else None
        
        if not category_col or not value_col:
            return ChartRecommendation(
                chart_type=ChartType.TABLE,
                title="Распределение",
                confidence=0.6,
                reason="Не удалось определить структуру для круговой диаграммы"
            )
        
        return ChartRecommendation(
            chart_type=ChartType.PIE,
            title=self._generate_title(query, "Распределение"),
            x_column=category_col,
            y_column=value_col,
            confidence=0.85,
            reason="Запрос о распределении с небольшим количеством категорий"
        )
    
    def _recommend_category_comparison_chart(
        self,
        df: pd.DataFrame,
        analysis: Dict,
        query: str
    ) -> ChartRecommendation:
        """Рекомендация для сравнения категорий"""
        category_col = analysis["categorical_columns"][0]
        value_col = analysis["numeric_columns"][0]
        
        return ChartRecommendation(
            chart_type=ChartType.BAR,
            title=self._generate_title(query, "Сравнение"),
            x_column=category_col,
            y_column=value_col,
            confidence=0.8,
            reason="Сравнение категорий по числовой метрике"
        )
    
    def _recommend_scatter_chart(
        self,
        df: pd.DataFrame,
        analysis: Dict,
        query: str
    ) -> ChartRecommendation:
        """Рекомендация для scatter plot"""
        x_col = analysis["numeric_columns"][0]
        y_col = analysis["numeric_columns"][1]
        
        return ChartRecommendation(
            chart_type=ChartType.SCATTER,
            title=self._generate_title(query, "Зависимость"),
            x_column=x_col,
            y_column=y_col,
            confidence=0.75,
            reason="Две числовые метрики для анализа зависимости"
        )
    
    def _generate_title(self, query: str, default_prefix: str) -> str:
        """Генерирует заголовок графика на основе вопроса"""
        # Берем первые 60 символов вопроса, если он осмысленный
        if len(query) > 0 and len(query) < 80:
            return query.capitalize()
        elif len(query) >= 80:
            return query[:77] + "..."
        else:
            return default_prefix
    
    def generate_chart(
        self,
        df: pd.DataFrame,
        recommendation: ChartRecommendation
    ) -> Optional[go.Figure]:
        """
        Генерирует Plotly график на основе рекомендации
        
        Args:
            df: DataFrame с данными
            recommendation: Рекомендация по визуализации
            
        Returns:
            Plotly Figure или None
        """
        try:
            if recommendation.chart_type == ChartType.NONE:
                return None
            
            if recommendation.chart_type == ChartType.TABLE:
                return None  # Таблицу рендерит сам Streamlit
            
            if recommendation.chart_type == ChartType.LINE:
                return self._create_line_chart(df, recommendation)
            
            if recommendation.chart_type == ChartType.BAR:
                return self._create_bar_chart(df, recommendation)
            
            if recommendation.chart_type == ChartType.HORIZONTAL_BAR:
                return self._create_horizontal_bar_chart(df, recommendation)
            
            if recommendation.chart_type == ChartType.PIE:
                return self._create_pie_chart(df, recommendation)
            
            if recommendation.chart_type == ChartType.SCATTER:
                return self._create_scatter_chart(df, recommendation)
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка при генерации графика: {e}")
            return None
    
    def _create_line_chart(self, df: pd.DataFrame, rec: ChartRecommendation) -> go.Figure:
        """Создает линейный график"""
        # Конвертируем дату если нужно
        df_copy = df.copy()
        if df_copy[rec.x_column].dtype == 'object':
            df_copy[rec.x_column] = pd.to_datetime(df_copy[rec.x_column])
        
        # Сортируем по дате
        df_copy = df_copy.sort_values(rec.x_column)
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_copy[rec.x_column],
            y=df_copy[rec.y_column],
            mode='lines+markers',
            name=rec.y_column,
            line=dict(color='#1f77b4', width=2),
            marker=dict(size=8)
        ))
        
        fig.update_layout(
            title=rec.title,
            xaxis_title=rec.x_column,
            yaxis_title=rec.y_column,
            template='plotly_white',
            hovermode='x unified'
        )
        
        return fig
    
    def _create_bar_chart(self, df: pd.DataFrame, rec: ChartRecommendation) -> go.Figure:
        """Создает столбчатую диаграмму"""
        # Ограничиваем количество баров если их слишком много
        df_plot = df.head(20) if len(df) > 20 else df
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_plot[rec.x_column],
            y=df_plot[rec.y_column],
            marker=dict(color='#2ca02c'),
            text=df_plot[rec.y_column],
            textposition='auto'
        ))
        
        fig.update_layout(
            title=rec.title,
            xaxis_title=rec.x_column,
            yaxis_title=rec.y_column,
            template='plotly_white',
            showlegend=False
        )
        
        return fig
    
    def _create_horizontal_bar_chart(self, df: pd.DataFrame, rec: ChartRecommendation) -> go.Figure:
        """Создает горизонтальную столбчатую диаграмму"""
        df_plot = df.head(20) if len(df) > 20 else df
        
        # Переворачиваем порядок для удобства чтения (наибольшее значение сверху)
        df_plot = df_plot.iloc[::-1]
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_plot[rec.y_column],
            y=df_plot[rec.x_column],
            orientation='h',
            marker=dict(color='#ff7f0e'),
            text=df_plot[rec.y_column],
            textposition='auto'
        ))
        
        fig.update_layout(
            title=rec.title,
            xaxis_title=rec.y_column,
            yaxis_title=rec.x_column,
            template='plotly_white',
            showlegend=False,
            height=max(400, len(df_plot) * 30)  # Динамическая высота
        )
        
        return fig
    
    def _create_pie_chart(self, df: pd.DataFrame, rec: ChartRecommendation) -> go.Figure:
        """Создает круговую диаграмму"""
        df_plot = df.head(10) if len(df) > 10 else df
        
        fig = go.Figure()
        fig.add_trace(go.Pie(
            labels=df_plot[rec.x_column],
            values=df_plot[rec.y_column],
            textinfo='label+percent',
            hovertemplate='%{label}<br>%{value}<br>%{percent}<extra></extra>'
        ))
        
        fig.update_layout(
            title=rec.title,
            template='plotly_white'
        )
        
        return fig
    
    def _create_scatter_chart(self, df: pd.DataFrame, rec: ChartRecommendation) -> go.Figure:
        """Создает scatter plot"""
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df[rec.x_column],
            y=df[rec.y_column],
            mode='markers',
            marker=dict(
                size=10,
                color=df[rec.y_column],
                colorscale='Viridis',
                showscale=True
            )
        ))
        
        fig.update_layout(
            title=rec.title,
            xaxis_title=rec.x_column,
            yaxis_title=rec.y_column,
            template='plotly_white'
        )
        
        return fig


class Visualizer:
    """
    Главный класс для работы с визуализацией.
    Используется агентом для создания графиков.
    """
    
    def __init__(self):
        self.engine = VisualizationEngine()
        logger.info("Visualizer инициализирован")
    
    def create_visualization(
        self,
        df: pd.DataFrame,
        query: str,
        sql: str
    ) -> Tuple[Optional[go.Figure], ChartRecommendation]:
        """
        Создает визуализацию для результатов запроса
        
        Args:
            df: DataFrame с результатами SQL
            query: Исходный вопрос пользователя
            sql: Выполненный SQL запрос
            
        Returns:
            Tuple из (Plotly Figure или None, ChartRecommendation)
        """
        logger.info(f"Создание визуализации для запроса: {query[:50]}...")
        
        # Получаем рекомендацию
        recommendation = self.engine.recommend_visualization(df, query, sql)
        
        logger.info(
            f"Рекомендация: {recommendation.chart_type.value}, "
            f"confidence={recommendation.confidence:.2f}, "
            f"reason='{recommendation.reason}'"
        )
        
        # Генерируем график
        figure = self.engine.generate_chart(df, recommendation)
        
        return figure, recommendation


# Глобальный инстанс
_visualizer_instance = None


def get_visualizer() -> Visualizer:
    """Получить глобальный инстанс Visualizer (синглтон)"""
    global _visualizer_instance
    if _visualizer_instance is None:
        _visualizer_instance = Visualizer()
    return _visualizer_instance