-- Mock аналитические витрины для разработки AI-агента
-- Эти таблицы имитируют реальные витрины, которые создаст команда данных

-- ============================================================================
-- Витрина 1: Продажи по дням (sales_summary)
-- ============================================================================

CREATE TABLE sales_summary (
    sale_date DATE NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    order_count INTEGER NOT NULL,
    avg_order_value DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (sale_date)
);

COMMENT ON TABLE sales_summary IS 'Агрегированные продажи по дням';
COMMENT ON COLUMN sales_summary.sale_date IS 'Дата продажи';
COMMENT ON COLUMN sales_summary.total_amount IS 'Общая сумма продаж за день в рублях';
COMMENT ON COLUMN sales_summary.order_count IS 'Количество заказов за день';
COMMENT ON COLUMN sales_summary.avg_order_value IS 'Средний чек за день';

-- Генерируем данные за последние 90 дней
INSERT INTO sales_summary (sale_date, total_amount, order_count, avg_order_value)
SELECT 
    date_series AS sale_date,
    (RANDOM() * 50000 + 10000)::DECIMAL(12, 2) AS total_amount,
    (RANDOM() * 100 + 20)::INTEGER AS order_count,
    ((RANDOM() * 50000 + 10000) / (RANDOM() * 100 + 20))::DECIMAL(10, 2) AS avg_order_value
FROM generate_series(
    CURRENT_DATE - INTERVAL '90 days',
    CURRENT_DATE - INTERVAL '1 day',
    '1 day'::INTERVAL
) AS date_series;

-- ============================================================================
-- Витрина 2: Статистика по клиентам (customer_stats)
-- ============================================================================

CREATE TABLE customer_stats (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    total_orders INTEGER NOT NULL,
    lifetime_value DECIMAL(12, 2) NOT NULL,
    first_order_date DATE NOT NULL,
    last_order_date DATE NOT NULL,
    avg_order_value DECIMAL(10, 2) NOT NULL,
    customer_segment VARCHAR(20) NOT NULL
);

COMMENT ON TABLE customer_stats IS 'Статистика по клиентам';
COMMENT ON COLUMN customer_stats.customer_id IS 'Уникальный ID клиента';
COMMENT ON COLUMN customer_stats.customer_name IS 'Имя клиента';
COMMENT ON COLUMN customer_stats.email IS 'Email клиента';
COMMENT ON COLUMN customer_stats.total_orders IS 'Общее количество заказов клиента';
COMMENT ON COLUMN customer_stats.lifetime_value IS 'Общая сумма покупок клиента за всё время';
COMMENT ON COLUMN customer_stats.first_order_date IS 'Дата первого заказа';
COMMENT ON COLUMN customer_stats.last_order_date IS 'Дата последнего заказа';
COMMENT ON COLUMN customer_stats.avg_order_value IS 'Средний чек клиента';
COMMENT ON COLUMN customer_stats.customer_segment IS 'Сегмент клиента: VIP, Regular, New';

-- Генерируем 50 клиентов
INSERT INTO customer_stats (
    customer_id, 
    customer_name, 
    email, 
    total_orders, 
    lifetime_value, 
    first_order_date, 
    last_order_date, 
    avg_order_value,
    customer_segment
)
SELECT 
    id AS customer_id,
    'Клиент ' || id AS customer_name,
    'customer' || id || '@example.com' AS email,
    (RANDOM() * 50 + 1)::INTEGER AS total_orders,
    (RANDOM() * 200000 + 5000)::DECIMAL(12, 2) AS lifetime_value,
    CURRENT_DATE - (RANDOM() * 365 + 30)::INTEGER AS first_order_date,
    CURRENT_DATE - (RANDOM() * 30)::INTEGER AS last_order_date,
    ((RANDOM() * 200000 + 5000) / (RANDOM() * 50 + 1))::DECIMAL(10, 2) AS avg_order_value,
    CASE 
        WHEN RANDOM() < 0.2 THEN 'VIP'
        WHEN RANDOM() < 0.7 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment
FROM generate_series(1, 50) AS id;

-- ============================================================================
-- Витрина 3: Производительность товаров (product_performance)
-- ============================================================================

CREATE TABLE product_performance (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50) NOT NULL,
    total_quantity_sold INTEGER NOT NULL,
    total_revenue DECIMAL(12, 2) NOT NULL,
    avg_price DECIMAL(10, 2) NOT NULL,
    units_in_stock INTEGER NOT NULL,
    last_sale_date DATE
);

COMMENT ON TABLE product_performance IS 'Производительность товаров';
COMMENT ON COLUMN product_performance.product_id IS 'Уникальный ID товара';
COMMENT ON COLUMN product_performance.product_name IS 'Название товара';
COMMENT ON COLUMN product_performance.category IS 'Категория товара';
COMMENT ON COLUMN product_performance.total_quantity_sold IS 'Общее количество проданных единиц';
COMMENT ON COLUMN product_performance.total_revenue IS 'Общая выручка от товара';
COMMENT ON COLUMN product_performance.avg_price IS 'Средняя цена товара';
COMMENT ON COLUMN product_performance.units_in_stock IS 'Остаток на складе';
COMMENT ON COLUMN product_performance.last_sale_date IS 'Дата последней продажи';

-- Генерируем 30 товаров
INSERT INTO product_performance (
    product_id,
    product_name,
    category,
    total_quantity_sold,
    total_revenue,
    avg_price,
    units_in_stock,
    last_sale_date
)
SELECT 
    id AS product_id,
    'Товар ' || id AS product_name,
    CASE 
        WHEN id <= 10 THEN 'Электроника'
        WHEN id <= 20 THEN 'Одежда'
        ELSE 'Продукты питания'
    END AS category,
    (RANDOM() * 500 + 50)::INTEGER AS total_quantity_sold,
    (RANDOM() * 500000 + 10000)::DECIMAL(12, 2) AS total_revenue,
    (RANDOM() * 5000 + 500)::DECIMAL(10, 2) AS avg_price,
    (RANDOM() * 100)::INTEGER AS units_in_stock,
    CASE 
        WHEN RANDOM() < 0.9 THEN CURRENT_DATE - (RANDOM() * 30)::INTEGER
        ELSE NULL
    END AS last_sale_date
FROM generate_series(1, 30) AS id;

-- ============================================================================
-- Создаём индексы для производительности
-- ============================================================================

CREATE INDEX idx_sales_date ON sales_summary(sale_date);
CREATE INDEX idx_customer_segment ON customer_stats(customer_segment);
CREATE INDEX idx_customer_lifetime_value ON customer_stats(lifetime_value DESC);
CREATE INDEX idx_product_category ON product_performance(category);
CREATE INDEX idx_product_revenue ON product_performance(total_revenue DESC);

-- ============================================================================
-- Добавляем несколько специфичных записей для тестирования
-- ============================================================================

-- Обновляем данные для декабря 2024 (для тестовых вопросов)
INSERT INTO sales_summary (sale_date, total_amount, order_count, avg_order_value)
SELECT 
    date_series AS sale_date,
    (RANDOM() * 60000 + 30000)::DECIMAL(12, 2) AS total_amount,
    (RANDOM() * 120 + 40)::INTEGER AS order_count,
    ((RANDOM() * 60000 + 30000) / (RANDOM() * 120 + 40))::DECIMAL(10, 2) AS avg_order_value
FROM generate_series(
    '2024-12-01'::DATE,
    '2024-12-31'::DATE,
    '1 day'::INTERVAL
) AS date_series
ON CONFLICT (sale_date) DO NOTHING;

-- Добавляем VIP клиента с известными параметрами
INSERT INTO customer_stats VALUES 
(999, 'Иван Петров', 'ivan.petrov@example.com', 120, 500000.00, '2023-01-15', '2024-12-15', 4166.67, 'VIP')
ON CONFLICT (customer_id) DO NOTHING;

-- Добавляем топ товар
INSERT INTO product_performance VALUES 
(999, 'iPhone 15 Pro', 'Электроника', 850, 1200000.00, 1411.76, 45, CURRENT_DATE - 1)
ON CONFLICT (product_id) DO NOTHING;

-- ============================================================================
-- Выводим статистику по созданным данным
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '=== Mock данные успешно загружены ===';
    RAISE NOTICE 'Таблица sales_summary: % записей', (SELECT COUNT(*) FROM sales_summary);
    RAISE NOTICE 'Таблица customer_stats: % записей', (SELECT COUNT(*) FROM customer_stats);
    RAISE NOTICE 'Таблица product_performance: % записей', (SELECT COUNT(*) FROM product_performance);
    RAISE NOTICE '======================================';
END $$;