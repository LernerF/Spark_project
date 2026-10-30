Spark Table Comparison Tool

Универсальный инструмент для сравнения двух таблиц (оригинальной и shadow) при валидации миграции данных.

## Структура проекта

```
spark_table_comparison/
├── config.py              # Конфигурационные настройки
├── spark_utils.py         # Утилиты для Spark сессии и S3
├── comparison_engine.py   # Основная логика сравнения
├── report_generator.py    # Генерация отчетов и загрузка в S3
├── dag.py                 # Оркестрация DAG workflow
├── main.py                # Точка входа
└── __init__.py            # Инициализация пакета

data/
├── original_table.csv     # Данные оригинальной таблицы
└── shadow_table.csv       # Данные shadow таблицы

requirements.txt           # Зависимости Python
run.py                     # Скрипт запуска
.env.example               # Шаблон конфигурации окружения
```

## Возможности

- **Универсальное сравнение**: Работает с любыми CSV таблицами
- **Комплексные проверки**:
  - Сравнение схем (колонки, типы данных)
  - Сравнение количества строк
  - Сравнение контента (построчно или по первичному ключу)
- **DAG Workflow**: Структурированное выполнение с управлением зависимостями
- **Отчеты в S3**: Автоматическая загрузка отчетов о сравнении в S3

## Быстрый старт

### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 2. Настройка S3 (при использовании S3 хранилища)

Скопируйте пример файла окружения и обновите значения:

```bash
cp .env.example .env
```

Отредактируйте `.env` с вашими S3 учетными данными:

```env
S3_ENDPOINT=https://s3.amazonaws.com
S3_ACCESS_KEY=ваш_access_key
S3_SECRET_KEY=ваш_secret_key
S3_BUCKET=ваш-bucket-name
S3_REGION=us-east-1
```

Для локального тестирования с MinIO:

```env
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
```

### 3. Подготовка данных

Поместите ваши CSV файлы в директорию `data/`:

- `data/original_table.csv` - Оригинальная таблица
- `data/shadow_table.csv` - Shadow таблица

**Требования к CSV**:
- Первая строка должна содержать заголовки (имена колонок)
- Разделитель: запятая (`,`)
- Кодировка: UTF-8

### 4. Настройка параметров сравнения (Опционально)

Отредактируйте `spark_table_comparison/config.py`:

```python
# Укажите колонки первичного ключа для сравнения
PRIMARY_KEY_COLUMNS = ["id"]  # или None для полного сравнения строк

# Колонки для игнорирования
IGNORE_COLUMNS = ["created_at", "updated_at"]

# Пути к данным
ORIGINAL_TABLE_PATH = "data/original_table.csv"
SHADOW_TABLE_PATH = "data/shadow_table.csv"
```

### 5. Запуск сравнения

```bash
python run.py
```

Или напрямую:

```bash
python -m spark_table_comparison.main
```

## Выходные отчеты

Инструмент генерирует три типа отчетов в S3:

### 1. JSON отчет (`comparison_report_TIMESTAMP.json`)

Полный детальный отчет в формате JSON:

```json
{
  "comparison_timestamp": "2024-01-15T10:30:00",
  "schema_differences": {...},
  "row_differences": {...},
  "content_differences": {...},
  "summary": {
    "comparison_passed": false,
    "issues_found": ["Разница в количестве строк: 1 строка", ...]
  }
}
```

### 2. Summary CSV (`comparison_summary_TIMESTAMP.csv`)

Сводная таблица:

```csv
comparison_type,metric,original_value,shadow_value,status
schema,columns_count,5,5,MATCH
rows,total_rows,5,6,DIFFER
content,matching_rows,4,-,INFO
```

### 3. Detailed CSV (`comparison_detailed_TIMESTAMP.csv`)

Детальный список всех расхождений:

```csv
category,issue_type,column_name,details,table
schema,column_missing_in_original,updated_at,string,shadow
```

## Методы сравнения

### 1. Сравнение по хешу строк (По умолчанию)

Сравнивает целые строки с использованием SHA-256 хеша. Обнаруживает:
- Строки только в оригинале
- Строки только в shadow
- Совпадающие строки

Используйте, когда таблицы не имеют первичного ключа или нужно полное сравнение строк.

### 2. Сравнение по первичному ключу

Сравнивает строки на основе колонок первичного ключа. Обнаруживает:
- Отсутствующие строки (PK существует только в одной таблице)
- Измененные строки (одинаковый PK, разные данные)

Настройка в `config.py`:

```python
PRIMARY_KEY_COLUMNS = ["id"]
```

## Запуск на другом компьютере

### Способ 1: Клонирование и запуск

```bash
# Клонируйте проект
git clone <repository-url>
cd spark_table_comparison

# Установите зависимости
pip install -r requirements.txt

# Настройте окружение
cp .env.example .env
# Отредактируйте .env с вашими настройками

# Добавьте ваши файлы с данными
cp /путь/к/вашему/original.csv data/original_table.csv
cp /путь/к/вашему/shadow.csv data/shadow_table.csv

# Запустите
python run.py
```

### Способ 2: Docker (Скоро)

```bash
docker build -t table-comparison .
docker run --env-file .env -v $(pwd)/data:/app/data table-comparison
```

## Устранение неполадок

### Ошибка "Module not found"

Убедитесь, что запускаете из корневой директории проекта:

```bash
cd spark_table_comparison_project_root
python run.py
```

### Проблемы со Spark

Если Spark не запускается:

1. Проверьте, что Java установлена: `java -version`
2. Проверьте переменную окружения SPARK_HOME
3. Попробуйте установить `SPARK_MASTER=local[*]` в `.env`

### Проблемы подключения к S3

1. Проверьте правильность S3 учетных данных
2. Проверьте URL endpoint (используйте `https://s3.amazonaws.com` для AWS)
3. Для MinIO убедитесь, что `path.style.access=true` (по умолчанию в конфигурации)

### Проблемы с памятью

Для больших таблиц увеличьте память Spark:

```python
# В spark_utils.py добавьте:
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "4g")
```

## Параметры конфигурации

### Переменные окружения

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `S3_ENDPOINT` | URL S3 endpoint | `http://localhost:9000` |
| `S3_ACCESS_KEY` | S3 access key | `minioadmin` |
| `S3_SECRET_KEY` | S3 secret key | `minioadmin` |
| `S3_BUCKET` | Имя S3 bucket | `comparison-reports` |
| `S3_REGION` | S3 регион | `us-east-1` |

### Опции конфигурационного файла (`config.py`)

| Опция | Описание | По умолчанию |
|-------|----------|--------------|
| `PRIMARY_KEY_COLUMNS` | Список колонок PK | `None` |
| `IGNORE_COLUMNS` | Колонки для пропуска | `[]` |
| `IGNORE_NULL_DIFFERENCES` | Считать NULL = пустая строка | `False` |

## Пример вывода

```
============================================================
Table Comparison Tool for Migration Validation
============================================================

Original table: data/original_table.csv
Shadow table: data/shadow_table.csv
Output path: s3a://comparison-reports/comparison_reports/

Creating Spark session...
Spark session created successfully

Creating comparison DAG...
DAG created: TableComparisonMigrationDAG

Executing comparison DAG...
------------------------------------------------------------
Loading original table...
Original table loaded: 5 rows, 5 columns
Loading shadow table...
Shadow table loaded: 6 rows, 5 columns
Comparing schemas...
Schema comparison complete: 5 vs 5 columns
Comparing row counts...
Row count comparison complete: 5 vs 6 rows
Comparing content...
Content comparison complete: 4 matching rows
Generating summary...
Comparison FAILED
Issues found:
  - Row count mismatch: 1 rows difference
  - Content differences: 0 rows differ
  - 0 rows only in original table
  - 1 rows only in shadow table
Generating reports...
Reports saved:
  json_report: s3a://comparison-reports/.../comparison_report_20240115_103000.json
  summary_csv: s3a://comparison-reports/.../comparison_summary_20240115_103000.csv
  detailed_csv: s3a://comparison-reports/.../comparison_detailed_20240115_103000.csv
------------------------------------------------------------

============================================================
EXECUTION SUMMARY
============================================================
DAG Status: COMPLETED

Node Execution Details:
  ✓ load_original_table: completed (0.52s)
  ✓ load_shadow_table: completed (0.48s)
  ✓ compare_schemas: completed (0.12s)
  ✓ compare_row_counts: completed (0.08s)
  ✓ compare_content: completed (0.95s)
  ✓ generate_summary: completed (0.02s)
  ✓ generate_reports: completed (1.24s)

============================================================
COMPARISON RESULTS
============================================================
✗ COMPARISON FAILED - Differences detected

Issues found:
  - Row count mismatch: 1 rows difference
  - 1 rows only in shadow table

============================================================
REPORTS GENERATED
============================================================
  json_report: s3a://comparison-reports/comparison_reports/comparison_report_20240115_103000.json
  summary_csv: s3a://comparison-reports/comparison_reports/comparison_summary_20240115_103000.csv
  detailed_csv: s3a://comparison-reports/comparison_reports/comparison_detailed_20240115_103000.csv

============================================================
Comparison completed
============================================================
```

## Лицензия

MIT License

## Поддержка

При возникновении проблем и вопросов, пожалуйста, создайте issue в репозитории.
