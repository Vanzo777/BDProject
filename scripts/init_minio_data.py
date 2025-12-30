"""
Скрипт для однократной загрузки исходных данных в MinIO.
Проверяет наличие данных перед загрузкой (idempotent).
"""
import os
import sys
from pathlib import Path
from minio import Minio
from minio.error import S3Error
from datetime import datetime
import hashlib
import json


class MinioDataInitializer:
    """Класс для инициализации данных в MinIO lakehouse"""
    
    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "miniominio",
        secret_key: str = "miniominio",
        secure: bool = False
    ):
        """
        Инициализация клиента MinIO
        
        Args:
            endpoint: Адрес MinIO сервера
            access_key: Access key для MinIO
            secret_key: Secret key для MinIO
            secure: Использовать HTTPS (True) или HTTP (False)
        """
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bronze_bucket = "lakehouse-bronze"
        self.metadata_bucket = "lakehouse-metadata"
        
    def create_buckets(self):
        """Создание необходимых buckets если они не существуют"""
        buckets = [self.bronze_bucket, self.metadata_bucket]
        
        for bucket_name in buckets:
            try:
                if not self.client.bucket_exists(bucket_name):
                    self.client.make_bucket(bucket_name)
                    print(f"✓ Bucket '{bucket_name}' создан")
                else:
                    print(f"✓ Bucket '{bucket_name}' уже существует")
            except S3Error as e:
                print(f"✗ Ошибка при создании bucket '{bucket_name}': {e}")
                raise
    
    def calculate_file_hash(self, file_path: Path) -> str:
        """
        Вычисление MD5 хеша файла для проверки изменений
        
        Args:
            file_path: Путь к файлу
            
        Returns:
            MD5 хеш файла в виде строки
        """
        md5_hash = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    
    def check_file_uploaded(self, object_name: str, local_file_hash: str) -> bool:
        """
        Проверка, был ли файл уже загружен и не изменился ли он
        
        Args:
            object_name: Имя объекта в MinIO
            local_file_hash: MD5 хеш локального файла
            
        Returns:
            True если файл уже загружен и хеш совпадает, иначе False
        """
        try:
            # Получаем метаданные объекта
            stat = self.client.stat_object(self.bronze_bucket, object_name)
            
            # Проверяем хеш в метаданных (если мы его сохранили ранее)
            stored_hash = stat.metadata.get('x-amz-meta-md5hash', '')
            
            if stored_hash == local_file_hash:
                return True
            else:
                print(f"  Файл {object_name} изменился (хеш не совпадает), будет перезагружен")
                return False
                
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            else:
                print(f"✗ Ошибка при проверке файла {object_name}: {e}")
                return False
    
    def upload_file(
        self,
        local_path: Path,
        object_name: str,
        metadata: dict = None
    ) -> bool:
        """
        Загрузка файла в MinIO с метаданными
        
        Args:
            local_path: Путь к локальному файлу
            object_name: Имя объекта в MinIO
            metadata: Дополнительные метаданные
            
        Returns:
            True если загрузка успешна, иначе False
        """
        try:
            # Вычисляем хеш файла
            file_hash = self.calculate_file_hash(local_path)
            
            # Проверяем, нужно ли загружать файл
            if self.check_file_uploaded(object_name, file_hash):
                print(f"  Файл {object_name} уже загружен (пропускаем)")
                return True
            
            # Готовим метаданные
            file_metadata = {
                'md5hash': file_hash,
                'upload_timestamp': datetime.utcnow().isoformat(),
                'original_filename': local_path.name,
                'file_size_bytes': str(local_path.stat().st_size)
            }
            
            if metadata:
                file_metadata.update(metadata)
            
            # Загружаем файл
            self.client.fput_object(
                bucket_name=self.bronze_bucket,
                object_name=object_name,
                file_path=str(local_path),
                metadata=file_metadata
            )
            
            print(f"✓ Файл {local_path.name} загружен как {object_name}")
            return True
            
        except S3Error as e:
            print(f"✗ Ошибка при загрузке файла {local_path.name}: {e}")
            return False
        except Exception as e:
            print(f"✗ Неожиданная ошибка при загрузке {local_path.name}: {e}")
            return False
    
    def upload_dataset(self, data_dir: Path) -> bool:
        """
        Загрузка всех CSV файлов из директории data/raw
        
        Args:
            data_dir: Путь к директории с данными
            
        Returns:
            True если все файлы успешно загружены, иначе False
        """
        raw_dir = data_dir / "raw"
        
        if not raw_dir.exists():
            print(f"✗ Директория {raw_dir} не найдена")
            return False
        
        # Находим все CSV файлы
        csv_files = list(raw_dir.glob("*.csv"))
        
        if not csv_files:
            print(f"✗ В директории {raw_dir} не найдено CSV файлов")
            return False
        
        print(f"\nНайдено {len(csv_files)} CSV файл(ов) для загрузки:")
        for f in csv_files:
            print(f"  - {f.name}")
        
        # Загружаем каждый файл
        print("\nНачинаем загрузку в MinIO...")
        success_count = 0
        
        for csv_file in csv_files:
            # Формируем путь в MinIO: bronze/raw/filename.csv
            object_name = f"raw/{csv_file.name}"
            
            # Метаданные специфичные для датасета
            metadata = {
                'dataset': 'hft_trading_data',
                'layer': 'bronze',
                'format': 'csv',
                'source': 'local_upload'
            }
            
            if self.upload_file(csv_file, object_name, metadata):
                success_count += 1
        
        print(f"\n{'='*60}")
        print(f"Загружено {success_count} из {len(csv_files)} файлов")
        print(f"{'='*60}")
        
        return success_count == len(csv_files)
    
    def save_upload_manifest(self) -> bool:
        """
        Сохранение манифеста загрузки для отслеживания состояния
        
        Returns:
            True если манифест успешно сохранен
        """
        try:
            manifest = {
                'upload_timestamp': datetime.utcnow().isoformat(),
                'status': 'completed',
                'bronze_bucket': self.bronze_bucket,
                'files': []
            }
            
            # Получаем список всех файлов в bronze bucket
            objects = self.client.list_objects(
                self.bronze_bucket,
                prefix="raw/",
                recursive=True
            )
            
            for obj in objects:
                stat = self.client.stat_object(self.bronze_bucket, obj.object_name)
                manifest['files'].append({
                    'object_name': obj.object_name,
                    'size_bytes': obj.size,
                    'last_modified': obj.last_modified.isoformat(),
                    'md5hash': stat.metadata.get('x-amz-meta-md5hash', ''),
                    'upload_timestamp': stat.metadata.get('x-amz-meta-upload_timestamp', '')
                })
            
            # Сохраняем манифест как JSON
            manifest_json = json.dumps(manifest, indent=2, ensure_ascii=False)
            manifest_bytes = manifest_json.encode('utf-8')
            
            from io import BytesIO
            manifest_stream = BytesIO(manifest_bytes)
            
            self.client.put_object(
                bucket_name=self.metadata_bucket,
                object_name="bronze/upload_manifest.json",
                data=manifest_stream,
                length=len(manifest_bytes),
                content_type='application/json'
            )
            
            print("\n✓ Манифест загрузки сохранен в lakehouse-metadata/bronze/upload_manifest.json")
            return True
            
        except S3Error as e:
            print(f"✗ Ошибка при сохранении манифеста: {e}")
            return False
    
    def verify_upload(self) -> bool:
        """
        Проверка успешности загрузки данных
        
        Returns:
            True если данные загружены корректно
        """
        try:
            print("\nПроверка загруженных данных...")
            
            # Проверяем наличие файлов в bronze bucket
            objects = list(self.client.list_objects(
                self.bronze_bucket,
                prefix="raw/",
                recursive=True
            ))
            
            if not objects:
                print("✗ В bronze bucket нет файлов")
                return False
            
            print(f"✓ Найдено {len(objects)} файл(ов) в bronze layer:")
            for obj in objects:
                size_mb = obj.size / (1024 * 1024)
                print(f"  - {obj.object_name} ({size_mb:.2f} MB)")
            
            return True
            
        except S3Error as e:
            print(f"✗ Ошибка при проверке данных: {e}")
            return False


def main():
    """Основная функция для запуска инициализации"""
    print("="*60)
    print("MinIO Data Initializer - Загрузка данных в Lakehouse")
    print("="*60)
    
    # Получаем параметры подключения из переменных окружения или используем значения по умолчанию
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "miniominio")      # ← miniominio
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "miniominio")  # ← miniominio
    
    # Определяем путь к данным
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    
    print(f"\nПараметры подключения:")
    print(f"  MinIO endpoint: {endpoint}")
    print(f"  Access key: {access_key}")
    print(f"  Директория данных: {data_dir}")
    
    try:
        # Создаем инициализатор
        initializer = MinioDataInitializer(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        
        # Создаем buckets
        print("\n" + "="*60)
        print("Шаг 1: Создание buckets")
        print("="*60)
        initializer.create_buckets()
        
        # Загружаем данные
        print("\n" + "="*60)
        print("Шаг 2: Загрузка данных")
        print("="*60)
        if not initializer.upload_dataset(data_dir):
            print("\n✗ Не все файлы были загружены успешно")
            sys.exit(1)
        
        # Сохраняем манифест
        print("\n" + "="*60)
        print("Шаг 3: Сохранение манифеста")
        print("="*60)
        initializer.save_upload_manifest()
        
        # Проверяем загрузку
        print("\n" + "="*60)
        print("Шаг 4: Проверка загрузки")
        print("="*60)
        if not initializer.verify_upload():
            print("\n✗ Проверка загрузки не пройдена")
            sys.exit(1)
        
        print("\n" + "="*60)
        print("✓ Инициализация данных завершена успешно!")
        print("="*60)
        print("\nДанные доступны в MinIO:")
        print(f"  Bucket: {initializer.bronze_bucket}")
        print(f"  Префикс: raw/")
        print(f"  MinIO Console: http://{endpoint.split(':')[0]}:9001")
        
    except Exception as e:
        print(f"\n✗ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()