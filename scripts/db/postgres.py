""""PostgreSQL client: connection managment, execute, fetch."""

import psycopg

from scripts.config import PG_CONFIG

from contextlib import contextmanager

from typing import List, Dict, Optional

@contextmanager
def get_connection():
    """Open a connection to PostgreSQL. commit on success, rollback on error, always close."""
    conn = psycopg.connect(**PG_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute(query: str, params: tuple = ()) -> int:
    """Execute INSERT/UPDATE/DELETE; return number of affected rows."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query,params)
            return cur.rowcount

def fetch_all(query:str, params: tuple = ()) -> List[dict]:
    """Execute SELECT; return list of dicts."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns,row)) for row in cur.fetchall()]
        
def fetch_one(query: str, params: tuple = ()) -> Optional[Dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            if row is None:
                return None
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))
        
def test_postgres() -> bool:
    """Check DB availability; return True if everything is fine."""
    try:
        result = fetch_one("SELECT 1 AS ok")
        return result is not None and result["ok"] == 1
    except Exception as e:
        print(f"PostgreSQL connection failed: {e}")
        return False
    
if __name__ == "__main__":
    print("=== PostgreSQL health-check ===")
    if test_postgres():
        print("PostgreSQL is available.")
    else:
        print("PostgreSQL is NOT available.")
        
def execute_many(query: str, params_seq: list[tuple]) -> int:
    """Execute one query with a sequence of parameter sets; return rows sent."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, params_seq)
            return len(params_seq)