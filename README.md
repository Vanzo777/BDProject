# BD Project

BigData project with PySpark, Spark, Airflow, MinIO

Как работать с этим проектом? 

1. После загрузки перейти в терминал. 
2. Перейти в корневую папку проекта. 
3. Ввести команду: ./run_stack.sh
4. Чтобы все опустить, ввести команду: ./down_stack.sh

# Исходные данные

Исходные данные надо закинуть в data/raw и для загрузки в minio запустить скрипт init_minio_data.py


Команда после перезапуска: 
docker exec -it bdproject-airflow-worker-1 airflow connections delete trino_default

docker exec -it bdproject-airflow-worker-1 airflow connections add trino_default \
  --conn-type trino \
  --conn-host trino-coordinator \
  --conn-port 8080 \
  --conn-schema iceberg \
  --conn-login airflow
