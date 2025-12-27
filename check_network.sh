#!/bin/bash

echo "=== DOCKER NETWORK DIAGNOSTICS ==="
echo ""

echo "1. Checking if Trino is running:"
docker ps | grep trino

echo ""
echo "2. Checking Airflow containers:"
docker ps | grep airflow

echo ""
echo "3. Checking network 'bdproject_default':"
docker network inspect bdproject_default --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null || echo "Network bdproject_default does NOT exist!"

echo ""
echo "4. Checking which network Trino is on:"
docker inspect trino-coordinator --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null || echo "trino-coordinator NOT running!"

echo ""
echo "5. Checking which network Airflow worker is on:"
docker ps --filter "name=airflow-worker" --format "{{.Names}}" | head -1 | xargs docker inspect --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null
