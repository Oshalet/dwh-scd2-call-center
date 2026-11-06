"""Incremental load DAG: PostgreSQL → ClickHouse ODS."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from scripts.pipeline.extract import extract_delta

DAG_ID = "incremental_load"
WATERMARK_KEY = f"wm_{DAG_ID}"

# Default args для всех задач в DAG
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def load_delta(**context):
    """Task: extract delta and update watermark."""
    wm_str = Variable.get(WATERMARK_KEY, default_var="1970-01-01T00:00:00")
    watermark = datetime.fromisoformat(wm_str)

    print(f"Loading delta since {watermark}")
    n_loaded = extract_delta(watermark)
    print(f"Loaded {n_loaded} rows")

    if n_loaded > 0:
        from scripts.db.postgres import fetch_one
        result = fetch_one("SELECT max(updated_at) AS wm FROM call_center_requests")
        if result and result["wm"]:
            new_wm = result["wm"].isoformat()
            Variable.set(WATERMARK_KEY, new_wm)
            print(f"Watermark updated to {new_wm}")

    return n_loaded


with DAG(
    dag_id=DAG_ID,
    description="Incremental load from PostgreSQL to ClickHouse ODS",
    default_args=default_args,
    schedule="@daily",  # Раз в день в 00:00 UTC
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["demo", "incremental"],
) as dag:
    load_task = PythonOperator(
        task_id="load_delta",
        python_callable=load_delta,
    )