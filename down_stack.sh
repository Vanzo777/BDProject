#!/usr/bin/env bash
set -e

# Останавливаем и удаляем контейнеры стека
docker compose -f sparkdocker-compose.yaml down
docker compose -f airflowdocker-compose.yaml down
docker compose -f minio.yml down

# Удаляем общую сеть (если не используется другими контейнерами)
docker network rm big_data_net || true
