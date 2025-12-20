# 🤖 SQL AI Agent

**Text-to-SQL система на базе LangChain и OpenRouter**

Интеллектуальный агент для анализа данных с помощью естественного языка. Задавайте вопросы на русском — получайте ответы из базы данных.

---

## 📋 Содержание

- [Описание проекта](#-описание-проекта)
- [Архитектура](#-архитектура)
- [Технологический стек](#-технологический-стек)
- [Быстрый старт](#-быстрый-старт)
- [Установка](#-установка)
- [Конфигурация](#-конфигурация)
- [Использование](#-использование)
- [Тестирование](#-тестирование)
- [Структура проекта](#-структура-проекта)
- [Примеры вопросов](#-примеры-вопросов)
- [Разработка](#-разработка)
- [FAQ](#-faq)
- [Лицензия](#-лицензия)

---

## 🎯 Описание проекта

SQL AI Agent — это система для взаимодействия с базами данных на естественном языке. Вместо написания SQL запросов вручную, просто задайте вопрос:

**Вопрос:**

Какая была выручка в декабре 2024?

**Агент автоматически:**
1. Анализирует вопрос
2. Изучает структуру базы данных
3. Генерирует SQL запрос
4. Выполняет запрос
5. Форматирует ответ

**Ответ:**

Выручка в декабре 2024 составила 1,485,623.45 рублей.

SQL: SELECT SUM(total_amount) FROM sales_summary
WHERE sale_date >= '2024-12-01' AND sale_date <= '2024-12-31'

---

## 🏗️ Архитектура

### Компоненты системы

┌─────────────────────────────────────────────────────────────────┐
│ User Interface │
│ (Streamlit Web UI) │
└────────────────────────────┬────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────────┐
│ SQL AI Agent │
│ (LangChain + OpenRouter) │
│ │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │
│ │ Semantic │ │ LLM │ │ Database │ │
│ │ Layer │ │ (Gemini) │ │ Executor │ │
│ └──────────────┘ └──────────────┘ └──────────────┘ │
└────────────────────────────┬────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────────┐
│ PostgreSQL Database │
│ (Аналитические витрины) │
└─────────────────────────────────────────────────────────────────┘

### Поток данных

1. Пользователь → Вопрос на естественном языке

2. Semantic Layer → Контекст о данных (схема БД, примеры)

3. LLM → Генерация SQL запроса

4. Database → Выполнение SQL

5. LLM → Форматирование результатов

6. Пользователь ← Ответ на естественном языке

---

## 🛠️ Технологический стек

### Backend
- **Python 3.12** — язык программирования
- **LangChain** — фреймворк для LLM приложений
- **OpenRouter** — единый API для всех LLM
- **SQLAlchemy** — ORM и SQL toolkit
- **Pydantic** — валидация данных

### Database
- **PostgreSQL 16** — хранилище аналитических витрин
- **Docker** — контейнеризация

### Frontend
- **Streamlit** — веб-интерфейс

### LLM Models (через OpenRouter)
- **Google Gemini 2.0 Flash** (по умолчанию, бесплатный)
- **Deepseek Chat**
- **OpenAI GPT-3.5/4**
- **Anthropic Claude 3.5**

---

## 🚀 Быстрый старт

### Предварительные требования

- Python 3.12+
- Docker и Docker Compose
- API ключ OpenRouter ([получить здесь](https://openrouter.ai/))

### Установка за 5 минут

1. Клонировать репозиторий
git clone <your-repo-url>
cd sql_ai_agent

2. Создать виртуальное окружение
python3.12 -m venv venv
source venv/bin/activate # Linux/Mac

или venv\Scripts\activate для Windows
3. Установить зависимости
pip install -r requirements.txt

4. Настроить конфигурацию
cp .env.example .env
nano .env # Добавить OPENROUTER_API_KEY

5. Запустить базу данных
docker-compose up -d

6. Проверить подключение
python -c "from agent.database import db; db.test_connection()"

7. Запустить UI
streamlit run ui/app.py

Откройте браузер: [**http://localhost:8501**](http://localhost:8501)

---

## 📦 Установка

### 1. Клонирование проекта

git clone <your-repo-url>
cd sql_ai_agent

### 2. Создание виртуального окружения

python3.12 -m venv venv
source venv/bin/activate # Linux/Mac
venv\Scripts\activate # Windows

### 3. Установка зависимостей

**Полная установка (с dev tools):**

pip install -r requirements.txt

**Минимальная установка:**

pip install langchain langchain-community langchain-openai langchain-core
sqlalchemy psycopg2-binary openai pydantic pydantic-settings
python-dotenv pyyaml streamlit pandas

### 4. Настройка конфигурации

Копировать шаблон
cp .env.example .env

Отредактировать .env
nano .env

**Обязательно установить:**

OPENROUTER_API_KEY=your-api-key-here

### 5. Запуск базы данных

Запустить PostgreSQL с mock данными
docker-compose up -d

Проверить статус
docker-compose ps

Посмотреть логи
docker-compose logs -f postgres

### 6. Проверка установки

Тест подключения к БД
python -c "from agent.database import db; db.test_connection()"

Вывод конфигурации
python -c "from agent.config import settings; settings.print_config_summary()"

Быстрый тест агента
python tests/test_queries.py --mode quick

---

## ⚙️ Конфигурация

### Основные параметры (.env)

=== DATABASE ===
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mock_analytics
DB_USER=user
DB_PASSWORD=123
DB_SCHEMA=public

=== LLM ===
OPENROUTER_API_KEY=your-key-here
LLM_MODEL=google/gemini-2.0-flash-exp:free
LLM_TEMPERATURE=0.0
LLM_MAX_TOKENS=2000

=== SEMANTIC LAYER ===
SEMANTIC_LAYER_PATH=config/semantic_layer.yaml
FEW_SHOT_EXAMPLES_LIMIT=5

=== APPLICATION ===
LOG_LEVEL=INFO
ENABLE_SQL_LOGGING=true
MAX_RETRY_ATTEMPTS=3
SQL_EXECUTION_TIMEOUT=30

### Смена LLM модели

**В .env файле:**

LLM_MODEL=deepseek/deepseek-chat

**Через UI:**
- Откройте боковую панель
- Раздел "Сменить модель"
- Выберите модель из списка
- Нажмите "Применить"

**Программно:**

from agent.sql_agent import get_agent

agent = get_agent()
agent.switch_model("anthropic/claude-3.5-sonnet")

### Semantic Layer

Файл `config/semantic_layer.yaml` содержит описание данных для LLM:

- Описания таблиц и колонок
- Бизнес-правила
- Примеры запросов (few-shot learning)
- Синонимы для NLP

**Обновление для продакшена:**
1. Получите схему БД от команды данных
2. Обновите секцию `tables` в YAML
3. Добавьте новые примеры в `examples`
4. Обновите `business_rules`

---

## 💻 Использование

### Web UI (Streamlit)

Запустить интерфейс
streamlit run ui/app.py

С кастомным портом
streamlit run ui/app.py --server.port 8080

**Возможности UI:**
- ✅ Чат с AI агентом
- ✅ Просмотр сгенерированного SQL
- ✅ История вопросов
- ✅ Смена LLM модели
- ✅ Информация о БД
- ✅ Статистика сессии

### Программный доступ (API)

from agent.sql_agent import get_agent, ask_question

Простой способ
answer = ask_question("Какая была выручка в декабре?")
print(answer)

С деталями
agent = get_agent()
response = agent.ask(
"Топ-5 клиентов по сумме покупок",
return_sql=True
)

print(f"Ответ: {response['answer']}")
print(f"SQL: {response['sql']}")
print(f"Время: {response['execution_time']}s")

### Python скрипт

my_analysis.py
from agent.sql_agent import get_agent

agent = get_agent(use_few_shot=True, verbose=False)

questions = [
"Сколько клиентов в базе?",
"Какая средняя выручка за день?",
"Топ-3 товара по выручке"
]

for question in questions:
print(f"\nВопрос: {question}")

response = agent.ask(question, return_sql=True)

print(f"Ответ: {response['answer']}")
print(f"SQL: {response['sql']}")

---

## 🧪 Тестирование

### Быстрый тест (3 вопроса)

python tests/test_queries.py --mode quick

### Полный тест (~40 вопросов)

python tests/test_queries.py --mode full

### Тесты по сложности

Легкие вопросы
python tests/test_queries.py --mode easy

Средние
python tests/test_queries.py --mode medium

Сложные
python tests/test_queries.py --mode hard

### Результаты тестов

Сохраняются в JSON файлы:
- `quick_test_results.json`
- `full_test_results.json`
- `easy_test_results.json`

**Пример результата:**

{
"statistics": {
"total_queries": 10,
"successful": 9,
"failed": 1,
"success_rate": 90.0,
"avg_execution_time": 2.45
},
"results": [...]
}

### Программное тестирование

from tests.test_queries import QueryTester, TestQueries
from agent.sql_agent import get_agent

agent = get_agent()
tester = QueryTester(agent)

Свои вопросы
my_questions = [
"Сколько заказов было вчера?",
"Какой топ-5 товаров?"
]

tester.run_test_suite(my_questions)
stats = tester.get_statistics()

print(f"Success rate: {stats['success_rate']}%")
tester.save_results("my_test.json")

---

## 📁 Структура проекта

sql_ai_agent/
├── .env.example # Шаблон конфигурации
├── .gitignore # Игнорируемые файлы
├── docker-compose.yml # PostgreSQL для разработки
├── requirements.txt # Python зависимости
├── README.md # Эта документация
│
├── config/
│ └── semantic_layer.yaml # Описание данных для LLM
│
├── mock_data/
│ └── init.sql # Mock данные для разработки
│
├── agent/ # Основной код агента
│ ├── init.py
│ ├── config.py # Загрузка настроек
│ ├── database.py # Работа с PostgreSQL
│ ├── llm.py # LLM через OpenRouter
│ ├── semantic_layer.py # Парсинг semantic layer
│ └── sql_agent.py # LangChain SQL Agent
│
├── ui/
│ └── app.py # Streamlit веб-интерфейс
│
└── tests/
└── test_queries.py # Набор тестов

---

## 💡 Примеры вопросов

### Простые запросы

Сколько записей в таблице sales_summary?
Какая была максимальная выручка за один день?
Сколько клиентов в базе данных?

### С фильтрацией

Какая была выручка в декабре 2024?
Сколько заказов было за последние 7 дней?
Какие товары имеют остаток меньше 20 единиц?

### Топ-N и рейтинги

Покажи топ-5 клиентов по сумме покупок
Какие 3 товара принесли больше всего выручки?
Топ-10 дней с максимальной выручкой

### Группировка и агрегация

Средний LTV клиентов по сегментам
Количество товаров в каждой категории
Выручка по месяцам за 2024 год

### Аналитические

Какие клиенты не делали покупок последние 30 дней?
Какие товары скоро закончатся на складе?
Какая доля выручки приходится на VIP клиентов?

### Сложные (множественные условия)

Какие товары из категории Электроника принесли больше 100000 рублей?
Клиенты с более чем 10 заказами и суммой покупок больше 50000
Дни когда выручка была выше среднего и количество заказов больше 50

---

## 👨‍💻 Разработка

### Подключение к продакшен БД

Когда команда данных предоставит доступы:

Обновить .env
DB_HOST=prod-postgres.example.com
DB_PORT=5432
DB_NAME=analytics_prod
DB_USER=ai_agent_user
DB_PASSWORD=secure_password
DB_SCHEMA=marts

**Обновить semantic layer:**

Получить схему от команды
Обновить config/semantic_layer.yaml
Перезапустить агента

### Добавление новых таблиц

1. Обновить `config/semantic_layer.yaml`:

tables:
new_table:
description: "Описание новой таблицы"
columns:
column1:
type: "integer"
description: "Описание колонки"

2. Добавить примеры запросов:

examples:

question: "Вопрос к новой таблице"
sql: "SELECT * FROM new_table"

3. Перезапустить агента (он автоматически загрузит новую конфигурацию)

### Логирование

В коде
import logging
logger = logging.getLogger(name)

logger.debug("Debug информация")
logger.info("Информационное сообщение")
logger.error("Ошибка!")

**Настройка уровня логов в .env:**

LOG_LEVEL=DEBUG # DEBUG, INFO, WARNING, ERROR, CRITICAL

### Отладка

Детальное логирование
agent = get_agent(verbose=True)

Просмотр промежуточных шагов
response = agent.ask(
"Ваш вопрос",
return_sql=True,
return_intermediate_steps=True
)

print(response["intermediate_steps"])

---

## ❓ FAQ

### Как получить API ключ OpenRouter?

1. Перейти на [openrouter.ai](https://openrouter.ai/)
2. Зарегистрироваться
3. Перейти в Keys
4. Создать новый ключ
5. Добавить в `.env`: `OPENROUTER_API_KEY=sk-or-v1-xxxxx`

### Какая модель лучше?

**Для разработки (бесплатно):**
- `google/gemini-2.0-flash-exp:free` — быстро, бесплатно, качественно

**Для продакшена:**
- `deepseek/deepseek-chat` — отличное соотношение цена/качество
- `anthropic/claude-3.5-sonnet` — лучшее качество для сложных запросов
- `openai/gpt-4-turbo` — очень хорошее качество, дороже

### Агент генерирует неправильный SQL

**Решения:**
1. Добавить больше примеров в `semantic_layer.yaml`
2. Улучшить описания колонок
3. Добавить бизнес-правила
4. Увеличить `FEW_SHOT_EXAMPLES_LIMIT`
5. Попробовать другую LLM модель

### База данных не подключается

Проверить что контейнер запущен
docker-compose ps

Перезапустить
docker-compose restart postgres

Посмотреть логи
docker-compose logs postgres

Проверить порт
netstat -an | grep 5432

### Streamlit не запускается

Проверить установку
pip install streamlit

Запустить с debug
streamlit run ui/app.py --logger.level=debug

Проверить порт
streamlit run ui/app.py --server.port 8080

### Как очистить базу данных?

Остановить и удалить данные
docker-compose down -v

Запустить заново
docker-compose up -d

---

## 📊 Производительность

### Типичные показатели

| Метрика | Значение |
|---------|----------|
| Время ответа | 2-5 секунд |
| Точность SQL | 85-95% |
| Success rate | 90-95% |
| Стоимость запроса (Gemini free) | $0.00 |
| Стоимость запроса (Deepseek) | $0.0001-0.0003 |

### Оптимизация

**Скорость:**
- Используйте `google/gemini-2.0-flash-exp` для быстрых ответов
- Уменьшите `FEW_SHOT_EXAMPLES_LIMIT` до 3
- Включите кэширование (TODO)

**Точность:**
- Увеличьте `FEW_SHOT_EXAMPLES_LIMIT` до 7-10
- Добавьте больше примеров в semantic layer
- Используйте `claude-3.5-sonnet` или `gpt-4-turbo`

**Стоимость:**
- Используйте бесплатные модели для разработки
- `deepseek/deepseek-chat` для продакшена (дешево и качественно)

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Как внести вклад

1. Fork проекта
2. Создать feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit изменений (`git commit -m 'Add some AmazingFeature'`)
4. Push в branch (`git push origin feature/AmazingFeature`)
5. Открыть Pull Request

---

## 📝 Лицензия

MIT License - см. файл LICENSE для деталей

---

## 👥 Авторы

- **Ваше Имя** - *Разработчик AI Agent* - [GitHub](https://github.com/yourusername)

---

## 🙏 Благодарности

- [LangChain](https://langchain.com/) за отличный фреймворк
- [OpenRouter](https://openrouter.ai/) за единый API ко всем LLM
- [Streamlit](https://streamlit.io/) за простой UI фреймворк
- Команда данных за поддержку проекта

---

## 📞 Контакты

**Вопросы по проекту:**
- Email: your.email@example.com
- Telegram: @yourusername

**Issues:**
- GitHub Issues: [your-repo/issues](https://github.com/your-repo/issues)

---

<div align="center">
Made with ❤️ and 🤖 by SQL AI Agent Team
</div>