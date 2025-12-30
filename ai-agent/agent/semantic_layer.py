"""
Semantic Layer - модуль для работы с описаниями таблиц и формирования контекста для LLM.

Загружает конфигурацию из semantic_layer.yaml и предоставляет удобный API
для использования в SQL агенте.
"""

import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class ColumnInfo:
    """Информация о колонке таблицы"""
    name: str
    type: str
    description: str
    nullable: bool
    primary_key: bool = False
    business_meaning: Optional[str] = None
    possible_values: Optional[List[str]] = None


@dataclass
class TableInfo:
    """Информация о таблице"""
    name: str
    description: str
    business_context: str
    columns: Dict[str, ColumnInfo]
    data_range: Dict[str, Any]


@dataclass
class QueryExample:
    """Пример запроса для few-shot learning"""
    question: str
    sql: str
    explanation: str


class SemanticLayer:
    """
    Semantic Layer для SQL AI Agent.
    
    Загружает описания таблиц из YAML и предоставляет методы для:
    - Формирования system prompt для LLM
    - Получения примеров запросов (few-shot learning)
    - Извлечения бизнес-правил и контекста
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Инициализация Semantic Layer из конфигурации.
        
        Args:
            config: Словарь с конфигурацией из YAML
        """
        self.config = config
        self.tables = self._parse_tables()
        self.examples = self._parse_examples()
        self.business_rules = config.get('business_rules', [])
        self.synonyms = config.get('synonyms', {})
        self.database_info = config.get('database', {})
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'SemanticLayer':
        """
        Загружает Semantic Layer из YAML файла.
        
        Args:
            yaml_path: Путь к YAML файлу с конфигурацией
            
        Returns:
            Экземпляр SemanticLayer
        """
        path = Path(yaml_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Файл {yaml_path} не найден")
        
        with open(path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        return cls(config)
    
    def _parse_tables(self) -> Dict[str, TableInfo]:
        """Парсит описания таблиц из конфигурации"""
        tables = {}
        
        for table_name, table_data in self.config.get('tables', {}).items():
            columns = {}
            
            for col_name, col_data in table_data.get('columns', {}).items():
                columns[col_name] = ColumnInfo(
                    name=col_name,
                    type=col_data.get('type', 'unknown'),
                    description=col_data.get('description', ''),
                    nullable=col_data.get('nullable', True),
                    primary_key=col_data.get('primary_key', False),
                    business_meaning=col_data.get('business_meaning'),
                    possible_values=col_data.get('possible_values')
                )
            
            tables[table_name] = TableInfo(
                name=table_name,
                description=table_data.get('description', ''),
                business_context=table_data.get('business_context', ''),
                columns=columns,
                data_range=table_data.get('data_range', {})
            )
        
        return tables
    
    def _parse_examples(self) -> List[QueryExample]:
        """Парсит примеры запросов из конфигурации"""
        examples = []
        
        for example_data in self.config.get('examples', []):
            examples.append(QueryExample(
                question=example_data.get('question', ''),
                sql=example_data.get('sql', ''),
                explanation=example_data.get('explanation', '')
            ))
        
        return examples
    
    def get_system_prompt(self) -> str:
        """
        Формирует system prompt для LLM с полным контекстом о базе данных.
        
        Returns:
            Текст system prompt
        """
        prompt_parts = []
        
        # Введение
        prompt_parts.append("Ты — эксперт по SQL и аналитике данных.")
        prompt_parts.append(f"Ты работаешь с базой данных: {self.database_info.get('name', 'Analytics Database')}")
        prompt_parts.append(f"Диалект SQL: {self.database_info.get('dialect', 'postgresql')}\n")
        
        # Описание таблиц
        prompt_parts.append("=== СТРУКТУРА БАЗЫ ДАННЫХ ===\n")
        
        for table_name, table in self.tables.items():
            prompt_parts.append(f"Таблица: {table_name}")
            prompt_parts.append(f"Описание: {table.description}")
            
            if table.business_context:
                prompt_parts.append(f"Бизнес-контекст: {table.business_context}")
            
            prompt_parts.append("Колонки:")
            
            for col_name, col in table.columns.items():
                col_desc = f"  - {col_name} ({col.type}): {col.description}"
                
                if col.primary_key:
                    col_desc += " [PRIMARY KEY]"
                
                if not col.nullable:
                    col_desc += " [NOT NULL]"
                
                if col.business_meaning:
                    col_desc += f" — {col.business_meaning}"
                
                if col.possible_values:
                    col_desc += f" (возможные значения: {', '.join(col.possible_values)})"
                
                prompt_parts.append(col_desc)
            
            if table.data_range:
                range_info = ", ".join([f"{k}: {v}" for k, v in table.data_range.items()])
                prompt_parts.append(f"Диапазон данных: {range_info}")
            
            prompt_parts.append("")  # Пустая строка между таблицами
        
        # Бизнес-правила
        if self.business_rules:
            prompt_parts.append("=== БИЗНЕС-ПРАВИЛА ===\n")
            for rule in self.business_rules:
                prompt_parts.append(f"- {rule.get('rule', '')}")
            prompt_parts.append("")
        
        # Инструкции по генерации SQL
        prompt_parts.append("=== ИНСТРУКЦИИ ===\n")
        prompt_parts.append("1. Генерируй ТОЛЬКО валидный SQL запрос без дополнительных объяснений")
        prompt_parts.append("2. Используй корректный синтаксис PostgreSQL")
        prompt_parts.append("3. Учитывай бизнес-правила и контекст данных")
        prompt_parts.append("4. Для дат используй явное приведение типов: '2024-12-01'::DATE")
        prompt_parts.append("5. Для текущей даты используй CURRENT_DATE")
        prompt_parts.append("6. Всегда указывай алиасы для агрегатных функций (AS ...)")
        prompt_parts.append("7. Если вопрос неоднозначен, выбирай наиболее вероятную интерпретацию")
        
        return "\n".join(prompt_parts)
    
    def get_few_shot_examples(self, limit: Optional[int] = None) -> str:
        """
        Формирует строку с примерами запросов для few-shot learning.
        
        Args:
            limit: Максимальное количество примеров (None = все)
            
        Returns:
            Форматированная строка с примерами
        """
        examples_text = ["=== ПРИМЕРЫ ЗАПРОСОВ ===\n"]
        
        examples_to_use = self.examples[:limit] if limit else self.examples
        
        for i, example in enumerate(examples_to_use, 1):
            examples_text.append(f"Пример {i}:")
            examples_text.append(f"Вопрос: {example.question}")
            examples_text.append(f"SQL: {example.sql}")
            
            if example.explanation:
                examples_text.append(f"Объяснение: {example.explanation}")
            
            examples_text.append("")  # Пустая строка между примерами
        
        return "\n".join(examples_text)
    
    def get_table_schemas_text(self) -> str:
        """
        Возвращает краткое текстовое описание схем таблиц.
        Полезно для кратких промптов.
        
        Returns:
            Текстовое описание схем
        """
        schemas = []
        
        for table_name, table in self.tables.items():
            columns_list = []
            
            for col_name, col in table.columns.items():
                columns_list.append(f"{col_name} ({col.type})")
            
            schemas.append(f"{table_name}: {', '.join(columns_list)}")
        
        return "\n".join(schemas)
    
    def get_business_rules_text(self) -> str:
        """
        Возвращает бизнес-правила в текстовом виде.
        
        Returns:
            Текст с бизнес-правилами
        """
        if not self.business_rules:
            return "Бизнес-правила не определены."
        
        rules_text = ["Бизнес-правила:"]
        
        for rule in self.business_rules:
            rules_text.append(f"- {rule.get('rule', '')}")
        
        return "\n".join(rules_text)
    
    def get_synonyms_mapping(self) -> Dict[str, List[str]]:
        """
        Возвращает маппинг синонимов для обработки естественного языка.
        
        Returns:
            Словарь: {термин: [список синонимов]}
        """
        return self.synonyms
    
    def get_full_context(self, include_examples: bool = True, examples_limit: int = 5) -> str:
        """
        Формирует полный контекст для LLM: system prompt + примеры.
        
        Args:
            include_examples: Включать ли примеры запросов
            examples_limit: Максимальное количество примеров
            
        Returns:
            Полный текст контекста
        """
        context_parts = [self.get_system_prompt()]
        
        if include_examples and self.examples:
            context_parts.append("\n")
            context_parts.append(self.get_few_shot_examples(limit=examples_limit))
        
        return "\n".join(context_parts)
    
    def get_table_info(self, table_name: str) -> Optional[TableInfo]:
        """
        Возвращает информацию о конкретной таблице.
        
        Args:
            table_name: Название таблицы
            
        Returns:
            TableInfo или None, если таблица не найдена
        """
        return self.tables.get(table_name)
    
    def list_tables(self) -> List[str]:
        """
        Возвращает список всех таблиц.
        
        Returns:
            Список названий таблиц
        """
        return list(self.tables.keys())
    
    def __repr__(self) -> str:
        return (f"SemanticLayer(tables={len(self.tables)}, "
                f"examples={len(self.examples)}, "
                f"rules={len(self.business_rules)})")