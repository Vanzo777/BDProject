#!/usr/bin/env bash
set -e

docker network create big_data_net || true

docker build -f Dockerfile.airflow -t my-airflow-spark:latest .

docker compose -f sparkdocker-compose.yaml up -d
docker compose -f airflowdocker-compose.yaml up -d
docker compose -f minio.yml up -d
