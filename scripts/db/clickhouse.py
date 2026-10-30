import clickhouse_connect
from typing import List, Dict, Any
from scripts.config import CH_CONFIG


def get_client():
    """Создает клиента ClickHouse"""
    return clickhouse_connect.get_client(**CH_CONFIG)


def fetch_all(query: str, parameters: dict = None) -> List[Dict[str, Any]]:
    """Выполняет SELECT и возвращает список словарей"""
    client = get_client()
    try:
        result = client.query(query, parameters=parameters or {})
        columns = result.column_names
        return [dict(zip(columns, row)) for row in result.result_rows]
    finally:
        client.close()


def insert_rows(table: str, rows: List[Dict[str, Any]], columns: List[str]) -> int:
    """Записывает батч строк в таблицу"""
    if not rows:
        return 0
    client = get_client()
    try:
        data = [[row[col] for col in columns] for row in rows]
        client.insert(table=table, data=data, column_names=columns)
        return len(rows)
    finally:
        client.close()
