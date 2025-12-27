"""
Загружает CSV файлы из локальной папки в MinIO
"""

import os
from minio import Minio
from minio.error import S3Error

# MinIO настройки
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "lakehouse"

# Локальная папка с CSV файлами (монтируется в Airflow контейнер)
LOCAL_DATA_DIR = "/opt/airflow/data/raw"

print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
print(f"Bucket: {BUCKET_NAME}")
print(f"Local data dir: {LOCAL_DATA_DIR}")

# Создаём MinIO клиент
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Создаём bucket если не существует
try:
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"✅ Bucket '{BUCKET_NAME}' created")
    else:
        print(f"✅ Bucket '{BUCKET_NAME}' already exists")
except S3Error as e:
    print(f"❌ Error checking/creating bucket: {e}")
    exit(1)

# Загружаем все CSV файлы из локальной папки
csv_files = [
    "order_classification.csv",
    "product_info.csv"
]

for csv_file in csv_files:
    local_path = os.path.join(LOCAL_DATA_DIR, csv_file)
    
    if not os.path.exists(local_path):
        print(f"⚠️  File not found: {local_path}")
        continue
    
    # Путь в MinIO
    minio_path = f"raw/{csv_file}"
    
    try:
        file_size = os.path.getsize(local_path)
        print(f"\nUploading {csv_file} ({file_size / (1024**3):.2f} GB)...")
        
        client.fput_object(
            BUCKET_NAME,
            minio_path,
            local_path,
            content_type="text/csv"
        )
        
        print(f"✅ Uploaded: s3a://{BUCKET_NAME}/{minio_path}")
        
    except S3Error as e:
        print(f"❌ Error uploading {csv_file}: {e}")

print("\n✅ Upload complete!")
