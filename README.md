# DWH с SCD Type 2 для колл-центра

Построение хранилища данных с историей изменений статусов заявок:
PostgreSQL (источник) → Airflow (оркестрация) → PySpark (SCD2) → ClickHouse (DWH).

## Быстрый старт

```bash
docker-compose up -d                      # поднять PostgreSQL и ClickHouse
pip install -r requirements.txt           # зависимости
python -m scripts.test_connections        # проверка подключений
```

## Как запустить генератор тестовых данных

```bash
python -m scripts.generate_data init      # создать начальный снапшот (100 заявок за 30 дней)
python -m scripts.generate_data changes   # сгенерировать волну изменений статусов
```

## Структура проекта

- `scripts/db/` — клиенты PostgreSQL (psycopg) и ClickHouse (clickhouse-connect)
- `scripts/generate_data.py` — генератор тестовых данных (CLI: init / changes)
- `docker/` — init.sql для обеих СУБД
- `dags/` — DAG-и Airflow (появятся на этапе 3)

## Статус

- [x] Этап 1: окружение, клиенты БД
- [x] Этап 2: генератор тестовых данных
- [x] Этап 3: Airflow и инкрементальная загрузка
- [x] Этап 4: SCD2 в PySpark
- [] Этап 5: загрузка в ClickHouse и проверки
- [] Этап 6: README, схемы, подготовка к собесе

## Airflow DAG

DAG `incremental_load` загружает дельту из PostgreSQL в ClickHouse ODS:
- Расписание: ежедневно в 00:00 UTC
- Watermark: хранится в Airflow Variable `wm_incremental_load`
- Retries: 3 попытки с паузой 5 минут

Запуск вручную: UI → DAG → Trigger DAG

## Этап 4: SCD2 Transformation

PySpark применяет логику Slowly Changing Dimension Type 2:
- Закрывает старые версии заявок (`valid_to = updated_at`, `is_active = 0`)
- Добавляет новые активные версии (`valid_to = NULL`, `is_active = 1`)
- Результат записывается в таблицу `dwd_call_center_requests_scd2` в ClickHouse

Зависимость в DAG: `extract_delta >> transform_scd2`