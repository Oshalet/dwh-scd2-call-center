"""Быстрая проверка подключений к базам"""
from scripts.db.postgres import fetch_all as pg_fetch
from scripts.db.clickhouse import fetch_all as ch_fetch


def test_postgres():
    try:
        result = pg_fetch("SELECT 1 AS test")
        print(f"✅ PostgreSQL OK: {result}")
    except Exception as e:
        print(f"❌ PostgreSQL ERROR: {e}")


def test_clickhouse():
    try:
        result = ch_fetch("SELECT 1 AS test")
        print(f"✅ ClickHouse OK: {result}")
    except Exception as e:
        print(f"❌ ClickHouse ERROR: {e}")


if __name__ == "__main__":
    test_postgres()
    test_clickhouse()
