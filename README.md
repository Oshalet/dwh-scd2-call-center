```markdown
# DWH с SCD Type 2 для колл-центра

Проект демонстрирует построение хранилища данных (DWH) с историей изменений (Slowly Changing Dimension Type 2) для заявок колл-центра.

**Архитектура:**
PostgreSQL (источник OLTP) → Airflow (оркестрация) → PySpark (трансформация SCD2) → ClickHouse (OLAP DWH).

## 🚀 Быстрый старт

### Требования
- Docker & Docker Compose
- Python 3.11+ (для локальной разработки скриптов)
- Git

### Установка окружения

1. **Клонировать репозиторий:**
   ```bash
   git clone <url-of-your-repo>
   cd dwh-scd2-call-center
   ```

2. **Поднять инфраструктуру (PostgreSQL, ClickHouse, Airflow):**
   ```bash
   docker-compose up -d --build
   ```
   *Первая сборка может занять 3–5 минут из-за установки Java и зависимостей Airflow.*

3. **Проверить доступность сервисов:**
   - PostgreSQL: `localhost:5433`
   - ClickHouse HTTP: `localhost:8123`
   - Airflow UI: `http://localhost:8080` (логин/пароль: `admin/admin`)

4. **Установить локальные зависимости Python (для запуска генератора и тестов):**
   ```bash
   pip install -r requirements.txt
   ```

## 📊 Генерация тестовых данных

Используем CLI-скрипт для эмуляции бизнес-процессов колл-центра.

### 1. Создать начальный снапшот (100 заявок за последние 30 дней)
```bash
python -m scripts.generate_data init
```
*Данные пишутся в PostgreSQL. Статусы всех новых заявок — `new`.*

### 2. Сгенерировать волну изменений (~20% заявок меняют статус)
```bash
python -m scripts.generate_data changes
```
*Триггер в PostgreSQL обновляет поле `updated_at`, что позволяет инкрементальному пайплайну поймать дельту.*

## ⚙️ Работа с Airflow DAG

DAG `incremental_load` автоматизирует перенос данных:

1. **Extract Delta:** Читает изменения из PostgreSQL (где `updated_at > watermark`) и пишет сырые данные в ClickHouse ODS (`ods_call_center_requests`).
2. **Transform SCD2:** Использует PySpark для применения логики Slowly Changing Dimension Type 2 к данным ODS и записи результатов в DWD (`dwd_call_center_requests_scd2`).
3. **Load Marts:** Агрегирует данные из DWD в витрины (DWS) для BI-аналитики.

### Как запустить вручную:
1. Откройте Airflow UI (`http://localhost:8080`).
2. Найдите DAG `incremental_load`.
3. Нажмите кнопку **Trigger DAG** (значок ▶️).
4. Дождитесь завершения всех задач (зеленый цвет).

### Расписание
В коде настроено расписание `@daily` (ежедневно в 00:00 UTC), но для демо рекомендуется ручной запуск после генерации изменений.

## 🏗 Архитектура и ключевые решения

### Почему такой стек?
| Компонент | Выбор | Обоснование |
|-----------|-------|-------------|
| **Source** | PostgreSQL | Стандартный OLTP источник, имитирующий CRM колл-центра. |
| **Orchestration** | Apache Airflow | Де-факто стандарт для ETL. Позволяет управлять зависимостями, retry-политикой и хранить stateful watermarks в Variables. |
| **Compute** | PySpark (Local Mode) | Демонстрация навыков Big Data processing. Для учебного объема данных используется local[*], чтобы избежать развертывания кластера YARN/K8s. В проде масштабировается горизонтально. |
| **Storage (Target)** | ClickHouse | Высокопроизводительная OLAP БД. Идеальна для агрегатных запросов по большим массивам данных. |

### Ключевые архитектурные паттерны

1. **Watermark Management (Инкрементальная загрузка):**
   - Хранится в Airflow Variables (`wm_incremental_load`).
   - Гарантирует идемпотентность: повторный запуск без изменений в источнике не создает дубликатов.
   - Избегает полного сканирования таблицы источника при каждом цикле.

2. **SCD Type 2 через PySpark:**
   - Логика закрытия старых версий (`is_active=0`, `valid_to=now()`) и открытия новых (`is_active=1`, `valid_to=NULL`) реализована на стороне Spark.
   - Использование оконных функций (`row_number`) гарантирует корректное определение последней версии записи даже при наличии нескольких апдейтов в одной партии.

3. **TRUNCATE + INSERT в ClickHouse:**
   - Вместо дорогих мутаций (`ALTER TABLE DELETE`) или сложных UPSERT (которых нет в CH), мы перезаписываем DWD-таблицу целиком.
   - Обеспечивает простоту поддержки и отсутствие рассинхрона истории.
   - Для больших объемов данных в проде заменено бы на `ReplacingMergeTree` или партиционированную замену.

4. **Разделение слоев (Medallion Architecture Lite):**
   - **ODS:** Сырая копия изменений (append-only).
   - **DWD:** Детализированная история (SCD2).
   - **DWS:** Агрегированные метрики для бизнес-запросов.

## 📂 Структура проекта

```text
.
├── dags/                   # Определение Airflow DAG
│   └── incremental_load_dag.py
├── docker/                 # Конфигурация контейнеров
│   ├── airflow/            # Dockerfile и requirements для Airflow (+Java)
│   ├── postgres/           # init.sql для создания таблиц источника
│   └── clickhouse/         # init.sql для создания ODS/DWD/DWS таблиц
├── scripts/                # Бизнес-логика и утилиты
│   ├── db/                 # Клиенты БД (psycopg, clickhouse-connect)
│   │   ├── postgres.py
│   │   └── clickhouse.py
│   ├── pipeline/           # Логика ETL
│   │   ├── extract.py      # Инкрементальная выборка дельты
│   │   ├── scd2.py         # Трансформация SCD2 через PySpark
│   │   └── load_marts.py   # Агрегация витрин (SQL in ClickHouse)
│   ├── config.py           # Конфигурация подключения
│   └── generate_data.py    # Генератор тестовых данных
├── .env                    # Переменные окружения (не коммитить!)
├── docker-compose.yml      # Оркестрация сервисов
├── README.md               # Этот файл
└── requirements.txt        # Зависимости Python для локального запуска
```

## 📊 Схема данных (Data Model)

### 1. Источник: PostgreSQL
```sql
CREATE TABLE call_center_requests (
    id SERIAL PRIMARY KEY,
    client_phone VARCHAR(20),
    operator_id INT,
    status VARCHAR(20) CHECK (status IN ('new', 'in_progress', 'closed', 'rejected')),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
-- Триггер автоматически обновляет updated_at при любом изменении строки
```

### 2. ODS: ClickHouse (`ods_call_center_requests`)
```sql
CREATE TABLE ods_call_center_requests (
    id UInt64,
    client_phone String,
    operator_id UInt32,
    status String,
    created_at DateTime,
    updated_at DateTime,
    _loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (id, updated_at);
```

### 3. DWD: ClickHouse (`dwd_call_center_requests_scd2`)
```sql
CREATE TABLE dwd_call_center_requests_scd2 (
    request_id UInt64,
    client_phone String,
    operator_id UInt32,
    status String,
    created_at DateTime,
    updated_at DateTime,
    valid_from DateTime,       -- Начало действия версии
    valid_to Nullable(DateTime), -- Конец действия версии (NULL если активна)
    is_active UInt8            -- 1 = текущая версия, 0 = история
) ENGINE = MergeTree() ORDER BY (request_id, valid_from);
```

### 4. DWS: Витрины
- **`dws_operator_performance_daily`**: Метрики по операторам (SLA, время решения, кол-во закрытых).
- **`dws_call_center_stats_daily`**: Общая статистика КЦ (нагрузка, пиковый час, avg cycle time).

## ✅ Статус этапов

- [x] **Этап 1:** Окружение, клиенты БД (PostgreSQL, ClickHouse)
- [x] **Этап 2:** Генератор тестовых данных с конечным автоматом статусов
- [x] **Этап 3:** Airflow Infrastructure + Инкрементальная загрузка в ODS
- [x] **Этап 4:** SCD2 Transformation (PySpark) в DWD
- [x] **Этап 5:** Агрегационные витрины (DWS)
- [ ] **Этап 6:** Финализация документации и подготовка к собеседованию

## 💡 Возможные улучшения (Roadmap)

1. **CDC вместо Watermark:** Внедрение Debezium/Kafka для захвата изменений в реальном времени, минуя polling `updated_at`.
2. **Partitioning & Pruning:** Партиционирование DWD по месяцам для ускорения запросов к истории.
3. **Testing:** Покрытие unit-тестами логики SCD2 (pytest + mock Spark Session).
4. **Monitoring:** Интеграция Prometheus/Grafana для мониторинга лагов Airflow и здоровья ClickHouse.
```