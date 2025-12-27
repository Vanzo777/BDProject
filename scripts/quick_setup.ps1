Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Quick Setup Script - BDProject" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# Проверяем наличие CSV файлов
if (-not (Test-Path "data/raw/order_classification.csv") -or -not (Test-Path "data/raw/product_info.csv")) {
    Write-Host "❌ CSV файлы не найдены в data/raw/" -ForegroundColor Red
    Write-Host ""
    Write-Host "Пожалуйста, скопируйте следующие файлы в data/raw/:"
    Write-Host "  - order_classification.csv"
    Write-Host "  - product_info.csv"
    Write-Host ""
    exit 1
}

Write-Host "✓ CSV файлы найдены" -ForegroundColor Green

# Проверяем, запущен ли Docker
try {
    docker info | Out-Null
    Write-Host "✓ Docker запущен" -ForegroundColor Green
}
catch {
    Write-Host "❌ Docker не запущен. Пожалуйста, запустите Docker Desktop." -ForegroundColor Red
    exit 1
}

# Запускаем стек
Write-Host ""
Write-Host "Запускаем инфраструктуру..." -ForegroundColor Yellow
.\run_stack.ps1

# Ждем, пока MinIO станет доступен
Write-Host ""
Write-Host "Ожидание запуска MinIO..." -ForegroundColor Yellow
$maxAttempts = 30
$attempt = 0

while ($attempt -lt $maxAttempts) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:9000/minio/health/live" -UseBasicParsing -TimeoutSec 2
        if ($response.StatusCode -eq 200) {
            Write-Host "✓ MinIO запущен и доступен" -ForegroundColor Green
            break
        }
    }
    catch {
        # Игнорируем ошибки
    }
    
    $attempt++
    Write-Host "  Попытка $attempt/$maxAttempts..."
    Start-Sleep -Seconds 2
}

if ($attempt -eq $maxAttempts) {
    Write-Host "❌ MinIO не запустился за отведенное время" -ForegroundColor Red
    exit 1
}

# Устанавливаем Python зависимости
Write-Host ""
Write-Host "Устанавливаем Python зависимости..." -ForegroundColor Yellow
pip install -q minio

# Загружаем данные в MinIO
Write-Host ""
Write-Host "Загружаем данные в MinIO..." -ForegroundColor Yellow
python scripts/init_minio_data.py

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "✓ Настройка завершена успешно!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Доступные сервисы:"
Write-Host "  MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
Write-Host "  Airflow UI: http://localhost:8080 (airflow/airflow)"
Write-Host ""
Write-Host "Следующие шаги:"
Write-Host "  1. Откройте MinIO Console и проверьте данные в bucket 'lakehouse-bronze'"
Write-Host "  2. Запустите Airflow DAG для обработки данных"
Write-Host ""