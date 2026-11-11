"""Incremental load DAG: PostgreSQL → ClickHouse ODS → DWD (SCD2)."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from scripts.pipeline.extract import extract_delta
from scripts.pipeline.scd2 import transform_scd2

DAG_ID = "incremental_load"
WATERMARK_KEY = f"wm_{DAG_ID}"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def load_delta(**context):
    """Task 1: extract delta from PostgreSQL to ClickHouse ODS."""
    wm_str = Variable.get(WATERMARK_KEY, default_var="1970-01-01T00:00:00")
    watermark = datetime.fromisoformat(wm_str)

    print(f"Loading delta since {watermark}")
    n_loaded = extract_delta(watermark)
    print(f"Loaded {n_loaded} rows into ODS")

    if n_loaded > 0:
        from scripts.db.postgres import fetch_one
        result = fetch_one("SELECT max(updated_at) AS wm FROM call_center_requests")
        if result and result["wm"]:
            new_wm = result["wm"].isoformat()
            Variable.set(WATERMARK_KEY, new_wm)
            print(f"Watermark updated to {new_wm}")

    return n_loaded


def apply_scd2_transformation(**context):
    """Task 2: transform ODS to DWD with SCD Type 2 logic."""
    print("Starting SCD2 transformation: ODS → DWD")
    n_rows = transform_scd2()
    print(f"DWD table now contains {n_rows} rows")
    return n_rows


with DAG(
    dag_id=DAG_ID,
    description="Incremental load with SCD2: PostgreSQL → ODS → DWD",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["demo", "incremental", "scd2"],
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_delta",
        python_callable=load_delta,
    )
    
    transform_task = PythonOperator(
        task_id="transform_scd2",
        python_callable=apply_scd2_transformation,
    )
    
    # Зависимость: extract → transform
    extract_task >> transform_task