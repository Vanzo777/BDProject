-- ============================================================================
-- Расширенные Mock аналитические витрины с реалистичными данными
-- Данные за 2 года (730+ дней), 500 клиентов, 200 товаров
-- ============================================================================

-- Очищаем существующие данные для полной перезагрузки
TRUNCATE TABLE sales_summary, customer_stats, product_performance RESTART IDENTITY;

-- ============================================================================
-- Витрина 1: Продажи по дням (sales_summary) - 730 дней данных
-- ============================================================================

-- Генерируем реалистичные продажи за 2 года с сезонностью
INSERT INTO sales_summary (sale_date, total_amount, order_count, avg_order_value)
WITH seasonal_pattern AS (
    SELECT 
        date_series,
        -- Базовая сезонность: пик в декабре (+50%), спад в январе (-20%)
        CASE 
            WHEN EXTRACT(MONTH FROM date_series) IN (12) THEN 1.5
            WHEN EXTRACT(MONTH FROM date_series) IN (1) THEN 0.8  
            WHEN EXTRACT(MONTH FROM date_series) IN (3,6,9) THEN 1.2  -- Квартальные пики
            ELSE 1.0
        END AS seasonality_factor,
        -- День недели: выходные +30%
        CASE 
            WHEN EXTRACT(DOW FROM date_series) IN (0,6) THEN 1.3
            ELSE 1.0
        END AS weekday_factor
    FROM generate_series(
        CURRENT_DATE - INTERVAL '730 days',
        CURRENT_DATE - INTERVAL '1 day',
        '1 day'::INTERVAL
    ) AS date_series
)
SELECT 
    date_series AS sale_date,
    -- Реалистичная динамика: 15k-120k руб/день
    GREATEST(15000, (RANDOM() * 80000 + 20000)::DECIMAL(12, 2) * seasonality_factor * weekday_factor) AS total_amount,
    -- 25-350 заказов/день
    GREATEST(25, (RANDOM() * 250 + 50)::INTEGER * seasonality_factor * weekday_factor)::INTEGER AS order_count,
    -- Средний чек 800-3500 руб
    GREATEST(800, (total_amount / order_count))::DECIMAL(10, 2) AS avg_order_value
FROM seasonal_pattern
ORDER BY date_series;

-- ============================================================================
-- Витрина 2: Статистика по клиентам (customer_stats) - 500 клиентов
-- ============================================================================

INSERT INTO customer_stats (
    customer_id, customer_name, email, total_orders, lifetime_value,
    first_order_date, last_order_date, avg_order_value, customer_segment
)
SELECT 
    id AS customer_id,
    CASE 
        WHEN id <= 50 THEN names_array[(RANDOM()*array_length(names_array,1)+1)::INTEGER]
        WHEN id <= 150 THEN surnames_array[(RANDOM()*array_length(surnames_array,1)+1)::INTEGER] || ' ' || names_array[(RANDOM()*array_length(names_array,1)+1)::INTEGER]
        ELSE 'Клиент #' || id
    END AS customer_name,
    
    LOWER(
        REPLACE(
            REPLACE(names_array[(RANDOM()*array_length(names_array,1)+1)::INTEGER], ' ', ''), 
            ' ', ''
        ) 
    ) || '.' || 
    LOWER(
        REPLACE(
            REPLACE(surnames_array[(RANDOM()*array_length(surnames_array,1)+1)::INTEGER], ' ', ''), 
            ' ', ''
        )
    ) || 
    '@' || domains_array[(RANDOM()*array_length(domains_array,1)+1)::INTEGER] AS email,
    
    GREATEST(1, (RANDOM() * 200 + 5)::INTEGER) AS total_orders,
    
    GREATEST(5000, (RANDOM() * 800000 + 10000)::DECIMAL(12, 2)) AS lifetime_value,
    
    CURRENT_DATE - (RANDOM() * 600 + 30)::INTEGER AS first_order_date,
    CURRENT_DATE - (RANDOM() * 90)::INTEGER AS last_order_date,
    
    GREATEST(500, ((RANDOM() * 800000 + 10000) / (RANDOM() * 200 + 5)))::DECIMAL(10, 2) AS avg_order_value,
    
    CASE 
        WHEN lifetime_value > 300000 OR total_orders > 100 THEN 'VIP'
        WHEN lifetime_value > 50000 OR total_orders > 20 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment

FROM generate_series(1, 500) AS id,
LATERAL (VALUES 
    ('Иван'), ('Мария'), ('Александр'), ('Елена'), ('Дмитрий'), 
    ('Анна'), ('Сергей'), ('Ольга'), ('Алексей'), ('Наталья'),
    ('Михаил'), ('Татьяна'), ('Владимир'), ('Светлана'), ('Павел')
) AS names(name),
unnest(ARRAY[
    'Иванов', 'Петров', 'Сидоров', 'Кузнецов', 'Смирнов', 'Попов', 'Васильев',
    'Соколов', 'Михайлов', 'Новиков', 'Федоров', 'Морозов', 'Волков', 'Алексеев',
    'Лебедев', 'Семенов', 'Егоров', 'Павлов', 'Козлов', 'Степанов'
]::text[]) AS surnames_array,
unnest(ARRAY[
    'yandex.ru', 'mail.ru', 'gmail.com', 'outlook.com', 'example.com'
]::text[]) AS domains_array
ORDER BY RANDOM();

-- ============================================================================
-- Витрина 3: Производительность товаров (product_performance) - 200 реалистичных товаров
-- ============================================================================

INSERT INTO product_performance (
    product_id, product_name, category, total_quantity_sold, total_revenue,
    avg_price, units_in_stock, last_sale_date
)
SELECT 
    id AS product_id,
    product_names[id % array_length(product_names,1) + 1] AS product_name,
    categories[(id / 20)::INTEGER % array_length(categories,1) + 1] AS category,
    
    GREATEST(10, (RANDOM() * 2000 + 100)::INTEGER) AS total_quantity_sold,
    GREATEST(50000, (RANDOM() * 2000000 + 100000)::DECIMAL(12, 2)) AS total_revenue,
    
    GREATEST(299, (total_revenue / total_quantity_sold))::DECIMAL(10, 2) AS avg_price,
    GREATEST(0, (100 - (RANDOM() * 80))::INTEGER) AS units_in_stock,
    
    CASE 
        WHEN RANDOM() < 0.95 THEN CURRENT_DATE - (RANDOM() * 60)::INTEGER
        ELSE NULL
    END AS last_sale_date

FROM generate_series(1, 200) AS id,
LATERAL (VALUES 
    -- Электроника (60 товаров)
    ('iPhone 15 Pro 256GB'), ('iPhone 15 Pro Max 512GB'), ('Samsung Galaxy S24 Ultra'),
    ('MacBook Air M3 13"'), ('MacBook Pro M3 Pro 14"'), ('iPad Pro 12.9" M4'),
    ('AirPods Pro 2'), ('Apple Watch Ultra 2'), ('Sony WH-1000XM5'), ('Samsung Galaxy Watch 7'),
    ('Xiaomi 14 Ultra'), ('Google Pixel 9 Pro'), ('OnePlus 12'), ('PS5 Slim 1TB'),
    ('Nintendo Switch OLED'), ('RTX 4080 16GB Founders'), ('i7-14700K + Z790'), 
    ('Samsung 990 PRO 2TB SSD'), ('LG 27" UltraGear 1440p'), ('Apple Vision Pro'),
    
    -- Одежда и обувь (60 товаров)  
    ('Nike Air Force 1 Low'), ('Adidas Ultraboost 23'), ('New Balance 550 White'),
    ('Levi''s 501 Original'), ('Patagonia Nano Puff'), ('Canada Goose Expedition'),
    ('Moncler Maya Down Jacket'), ('The North Face Summit'), ('Arc''teryx Beta AR'),
    ('Supreme Box Logo Hoodie'), ('Off-White Track Pants'), ('Gucci Ace Sneakers'),
    ('Balenciaga Triple S'), ('Yeezy Boost 350 V2'), ('Air Jordan 1 Retro High'),
    
    -- Продукты питания и бытовая химия (40 товаров)
    ('ВкусВилл Гречка 1кг'), ('Мираторг Стейк 300г'), ('Simple Прозрачный гель'),
    ('L''Oreal Paris Эльвеон'), ('Garnier Fructis Шампунь'), ('Nivea Крем 400мл'),
    ('AHC Premium Hydra'), ('La Roche-Posay Toleriane'), ('CeraVe PM Лосьон'),
    ('Bioderma Micellar Water'), ('Colgate Total 100мл'), ('Blend-a-med Pro-Expert'),
    
    -- Спортивное питание и товары для дома (40 товаров)
    ('Optimum Nutrition Gold Whey'), ('BSN Syntha-6'), ('MyProtein Impact Whey'),
    ('Dyson V15 Detect'), ('iRobot Roomba j9+'), ('Philips Series 9000 S9987'),
    ('Braun Series 9 Pro'), ('Xiaomi Mi Electric Scooter 4'), ('DJI Mini 4 Pro')
    
) AS products(product_name),
unnest(ARRAY[
    'Электроника', 'Одежда', 'Обувь', 'Спортивное питание', 
    'Продукты питания', 'Бытовая химия', 'Косметика', 'Товары для дома'
]::text[]) AS categories
ORDER BY RANDOM();

-- ============================================================================
-- Специальные записи для тестирования AI-агента
-- ============================================================================

-- Топ-5 VIP клиентов
INSERT INTO customer_stats VALUES 
(1001, 'Максим Кузнецов', 'max.kuznetsov@yandex.ru', 245, 1250000.00, '2023-03-15', '2025-12-19', 5102.04, 'VIP') ON CONFLICT DO NOTHING,
(1002, 'Екатерина Смирнова', 'ekaterina.smirnova@mail.ru', 198, 980000.00, '2023-05-10', '2025-12-18', 4950.00, 'VIP') ON CONFLICT DO NOTHING,
(1003, 'Андрей Иванов', 'andrey.ivanov@gmail.com', 312, 2100000.00, '2022-11-01', '2025-12-20', 6730.77, 'VIP') ON CONFLICT DO NOTHING,
(1004, 'Ольга Петрова', 'olga.petrov@yandex.ru', 167, 750000.00, '2023-08-22', '2025-12-15', 4491.02, 'VIP') ON CONFLICT DO NOTHING,
(1005, 'Дмитрий Соколов', 'dmitry.sokolov@outlook.com', 289, 1650000.00, '2023-01-05', '2025-12-19', 5709.35, 'VIP') ON CONFLICT DO NOTHING;

-- Новые трендовые товары декабря 2025
INSERT INTO product_performance VALUES 
(1001, 'iPhone 17 Pro Max 1TB', 'Электроника', 1250, 2850000.00, 2280.00, 120, '2025-12-19') ON CONFLICT DO NOTHING,
(1002, 'Samsung Galaxy S26 Ultra', 'Электроника', 890, 1980000.00, 2224.72, 210, '2025-12-20') ON CONFLICT DO NOTHING,
(1003, 'PS6 2TB Digital Edition', 'Электроника', 450, 1245000.00, 2766.67, 80, '2025-12-18') ON CONFLICT DO NOTHING,
(1004, 'AirPods Pro 3 ANC', 'Электроника', 3200, 896000.00, 280.00, 1500, '2025-12-19') ON CONFLICT DO NOTHING;

-- Праздничный всплеск продаж декабря 2025
INSERT INTO sales_summary (sale_date, total_amount, order_count, avg_order_value) VALUES
('2025-12-19', 285000.50, 892, 3193.16),
('2025-12-20', 312450.75, 1015, 3078.57),
('2025-12-18', 267890.20, 784, 3417.48)
ON CONFLICT (sale_date) DO UPDATE SET 
    total_amount = EXCLUDED.total_amount,
    order_count = EXCLUDED.order_count,
    avg_order_value = EXCLUDED.avg_order_value;

-- ============================================================================
-- Оптимизация: индексы и статистика
-- ============================================================================

CREATE INDEX CONCURRENTLY idx_sales_date ON sales_summary(sale_date);
CREATE INDEX CONCURRENTLY idx_sales_amount ON sales_summary(total_amount DESC);
CREATE INDEX CONCURRENTLY idx_customer_segment ON customer_stats(customer_segment);
CREATE INDEX CONCURRENTLY idx_customer_lifetime ON customer_stats(lifetime_value DESC);
CREATE INDEX CONCURRENTLY idx_customer_orders ON customer_stats(total_orders DESC);
CREATE INDEX CONCURRENTLY idx_product_category ON product_performance(category);
CREATE INDEX CONCURRENTLY idx_product_revenue ON product_performance(total_revenue DESC);
CREATE INDEX CONCURRENTLY idx_product_stock ON product_performance(units_in_stock ASC);

-- Обновляем статистику
ANALYZE sales_summary;
ANALYZE customer_stats; 
ANALYZE product_performance;

-- ============================================================================
-- Финальная статистика
-- ============================================================================

DO $$
DECLARE
    sales_count INTEGER;
    customer_count INTEGER;
    product_count INTEGER;
    total_revenue DECIMAL;
    avg_daily_sales DECIMAL;
BEGIN
    SELECT COUNT(*) INTO sales_count FROM sales_summary;
    SELECT COUNT(*) INTO customer_count FROM customer_stats;
    SELECT COUNT(*) INTO product_count FROM product_performance;
    SELECT SUM(total_amount) INTO total_revenue FROM sales_summary;
    SELECT AVG(total_amount) INTO avg_daily_sales FROM sales_summary;
END $$;