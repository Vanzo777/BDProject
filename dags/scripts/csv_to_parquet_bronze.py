#!/usr/bin/env python3
"""
Скрипт для загрузки CSV из MinIO в Bronze слой (Parquet).
Использует Polars для быстрой обработки без PySpark.
"""

import polars as pl
import logging
from minio import Minio
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO настройки
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "lakehouse"

def main():
    """Основная функция конвертации CSV -> Parquet"""
    
    # Создаем клиент MinIO
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    # Файлы для обработки
    files_to_process = [
        {
            "csv_path": "raw/order_classification.csv",
            "parquet_path": "bronze/order_classification.parquet",
            "temp_csv": "/tmp/order_classification.csv",
            "temp_parquet": "/tmp/order_classification.parquet"
        },
        {
            "csv_path": "raw/product_info.csv",
            "parquet_path": "bronze/product_info.parquet",
            "temp_csv": "/tmp/product_info.csv",
            "temp_parquet": "/tmp/product_info.parquet"
        }
    ]
    
    for file_info in files_to_process:
        try:
            logger.info(f"📥 Скачиваем {file_info['csv_path']} из MinIO...")
            
            # Скачиваем CSV из MinIO
            minio_client.fget_object(
                BUCKET_NAME,
                file_info["csv_path"],
                file_info["temp_csv"]
            )
            
            logger.info(f"📊 Читаем CSV с помощью Polars...")
            
            # Читаем CSV через Polars (БЫСТРО!)
            df = pl.read_csv(file_info["temp_csv"])
            
            logger.info(f"✅ Прочитано строк: {len(df)}, колонок: {len(df.columns)}")
            
            # Сохраняем в Parquet
            logger.info(f"💾 Записываем Parquet...")
            df.write_parquet(file_info["temp_parquet"])
            
            # Загружаем в MinIO
            logger.info(f"📤 Загружаем в MinIO: {file_info['parquet_path']}")
            minio_client.fput_object(
                BUCKET_NAME,
                file_info["parquet_path"],
                file_info["temp_parquet"]
            )
            
            # Удаляем временные файлы
            os.remove(file_info["temp_csv"])
            os.remove(file_info["temp_parquet"])
            
            logger.info(f"🎉 SUCCESS: {file_info['csv_path']} -> {file_info['parquet_path']}")
            
        except Exception as e:
            logger.error(f"❌ ERROR при обработке {file_info['csv_path']}: {e}")
            raise

if __name__ == "__main__":
    main()