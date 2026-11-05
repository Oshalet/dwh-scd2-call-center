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
- [ ] Этап 3: Airflow и инкрементальная загрузка
- [ ] Этап 4: SCD2 в PySpark
- [ ] Этап 5: загрузка в ClickHouse и проверки
- [ ] Этап 6: README, схемы, подготовка к собесе