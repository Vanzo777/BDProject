#!/usr/bin/env bash
set -e

# Останавливаем и удаляем контейнеры стека в обратном порядке
docker compose -f airflowdocker-compose.yaml down
docker compose -f minio.yml down
docker compose -f sparkdocker-compose.yaml down
docker compose -f hive-metastore/hivedocker-compose.yaml down
docker compose -f trinodocker-compose.yaml down


# Удаляем общую сеть (если не используется другими контейнерами)
docker network rm bdproject_default || true

