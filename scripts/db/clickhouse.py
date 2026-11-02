"""ClickHouse client: connection, fetch, batch insert."""

import clickhouse_connect

from scripts.config import CH_CONFIG

from contextlib import contextmanager

@contextmanager
def get_client():
    """Context manager: yields a ClickHouse client, closes it on exit."""
    conn = clickhouse_connect.get_client(**CH_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

def fetch_all(query: str, parameters: dict | None = None) -> list[dict]:
    """Execute SELECT; return list of dicts."""
    with get_client() as client:
        result = client.query(query=query, parameters=parameters or {})
        columns = result.column_names
        return [dict(zip(columns, row)) for row in result.result_rows]

def fetch_one(query: str, parameters: dict | None =None) -> dict | None:
    """Execute SELECT; return first row as dict, or None if empty."""
    rows = fetch_all(query=query,parameters=parameters)
    return rows[0] if rows else None 

def insert_rows(table: str, rows: list[dict], columns: list[str]) -> int:
    """Insert a batch of rows; return number of inserted rows."""
    if not rows:
        return 0
    with get_client() as client:
        data = [[row[col] for col in columns] for row in rows]
        client.insert(table=table, data=data, column_names=columns)
        return len(rows)
    
def test_clickhouse() -> bool:
    """Check DB availability; return True if everything is fine."""
    try:
        result = fetch_one("SELECT 1 AS ok")
        return result is not None and result["ok"] == 1
    except Exception as e:
        print(f"CH connection failed: {e}")
        return False
    
if __name__ == "__main__":
    print("=== ClickHouse health-check ===")
    if test_clickhouse():
        print("ClickHouse is available.")
    else:
        print("ClickHouse is NOT available.")