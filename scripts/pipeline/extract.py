"""Extract delta from PostgreSQL to ClickHouse ODS."""

from datetime import datetime
from typing import Optional

from scripts.db.postgres import fetch_all
from scripts.db.clickhouse import insert_rows

ODS_TABLE = "ods_call_center_requests"
ODS_COLUMNS = ["id", "client_phone", "operator_id", "status", "created_at", "updated_at"]

"""Extract delta from PostgreSQL to ClickHouse ODS."""

from datetime import datetime
from typing import Optional

from scripts.db.postgres import fetch_all
from scripts.db.clickhouse import insert_rows

ODS_TABLE = "ods_call_center_requests"
ODS_COLUMNS = ["id", "client_phone", "operator_id", "status", "created_at", "updated_at"]


def extract_delta(watermark: Optional[datetime] = None) -> int:
    """
    Load rows from PostgreSQL where updated_at > watermark into ClickHouse ODS.
    Returns number of rows loaded.
    """
    if watermark is None:
        watermark = datetime(1970, 1, 1)

    query = """
        SELECT id, client_phone, operator_id, status, created_at, updated_at
        FROM call_center_requests
        WHERE updated_at > %s
        ORDER BY updated_at
    """
    rows = fetch_all(query, (watermark,))

    if not rows:
        return 0

    return insert_rows(ODS_TABLE, rows, ODS_COLUMNS)

