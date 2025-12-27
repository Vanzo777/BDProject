#!/bin/bash

echo "=========================================="
echo "Quick Setup Script - BDProject"
echo "=========================================="

# Проверяем наличие CSV файлов
if [ ! -f "data/raw/order_classification.csv" ] || [ ! -f "data/raw/product_info.csv" ]; then
    echo "❌ CSV файлы не найдены в data/raw/"
    echo ""
    echo "Пожалуйста, скопируйте следующие файлы в data/raw/:"
    echo "  - order_classification.csv"
    echo "  - product_info.csv"
    echo ""
    exit 1
fi

echo "✓ CSV файлы найдены"

# Проверяем, запущен ли Docker
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker не запущен. Пожалуйста, запустите Docker Desktop."
    exit 1
fi

echo "✓ Docker запущен"

# Запускаем стек
echo ""
echo "Запускаем инфраструктуру..."
./run_stack.sh

# Ждем, пока MinIO станет доступен
echo ""
echo "Ожидание запуска MinIO..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        echo "✓ MinIO запущен и доступен"
        break
    fi
    
    attempt=$((attempt + 1))
    echo "  Попытка $attempt/$max_attempts..."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "❌ MinIO не запустился за отведенное время"
    exit 1
fi

# Устанавливаем Python зависимости
echo ""
echo "Устанавливаем Python зависимости..."
pip install -q minio

# Загружаем данные в MinIO
echo ""
echo "Загружаем данные в MinIO..."
python scripts/init_minio_data.py

echo ""
echo "=========================================="
echo "✓ Настройка завершена успешно!"
echo "=========================================="
echo ""
echo "Доступные сервисы:"
echo "  MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "  Airflow UI: http://localhost:8080 (airflow/airflow)"
echo ""
echo "Следующие шаги:"
echo "  1. Откройте MinIO Console и проверьте данные в bucket 'lakehouse-bronze'"
echo "  2. Запустите Airflow DAG для обработки данных"
echo ""