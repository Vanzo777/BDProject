#!/usr/bin/env bash
set -e

echo "=========================================="
echo "Starting BDProject Stack"
echo "=========================================="

# Создаем общую сеть
echo "[1/7] Creating Docker network..."
docker network create bdproject_default || true

# Собираем образ Airflow
echo "[2/7] Building Airflow image..."
docker build -f Dockerfile.airflow -t my-airflow-spark:latest .

# Запускаем сервисы в правильном порядке с ожиданием готовности
echo "[3/7] Starting Hive Metastore..."
docker compose -f hive-metastore/hivedocker-compose.yaml up -d
sleep 10

echo "[4/7] Starting Spark cluster..."
docker compose -f sparkdocker-compose.yaml up -d
sleep 5

echo "[5/7] Starting MinIO storage..."
docker compose -f minio.yml up -d
sleep 10

echo "[6/7] Starting Trino coordinator..."
docker compose -f trinodocker-compose.yaml up -d
echo "Waiting for Trino to be ready..."
sleep 30  # Даем Trino время на запуск

# Проверяем готовность Trino
echo "Checking Trino availability..."
for i in {1..30}; do
  if docker exec trino-coordinator curl -s http://localhost:8080/v1/info > /dev/null 2>&1; then
    echo "✓ Trino is ready!"
    break
  fi
  echo "  Waiting for Trino... ($i/30)"
  sleep 2
done

echo "[7/7] Starting Airflow..."
docker compose -f airflowdocker-compose.yaml up -d

echo "[8/8] Starting additional services..."
docker compose -f docker-compose.yaml up -d

echo "=========================================="
echo "Stack started successfully!"
echo "=========================================="
echo ""
echo "Services:"
echo "  - Airflow UI:     http://localhost:8080"
echo "  - MinIO Console:  http://localhost:9001"
echo "  - Trino:          http://localhost:8090"
echo ""
echo "Wait 30-60 seconds for all services to be ready."
echo "=========================================="

