# PowerShell скрипт для запуска Docker Compose стека
$ErrorActionPreference = "Stop"

# Создаем общую сеть
docker network create bdproject_default 2>$null

# Собираем образ Airflow
docker build -f Dockerfile.airflow -t my-airflow-spark:latest .

# Запускаем сервисы в правильном порядке
docker compose -f hive-metastore/hivedocker-compose.yaml up -d
docker compose -f sparkdocker-compose.yaml up -d
docker compose -f minio.yml up -d
docker compose -f airflowdocker-compose.yaml up -d
docker compose -f trinodocker-compose.yaml up -d
docker compose -f docker-compose.yaml up -d

