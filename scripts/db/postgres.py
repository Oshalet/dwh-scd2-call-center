import psycopg
from typing import List, Dict, Any
from scripts.config import PG_CONFIG


def get_connection():
    """Создает подключение к PostgreSQL"""
    return psycopg.connect(**PG_CONFIG)


def fetch_all(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """Выполняет SELECT и возвращает список словарей"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def execute(query: str, params: tuple = ()) -> None:
    """Выполняет INSERT/UPDATE/DELETE"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()
