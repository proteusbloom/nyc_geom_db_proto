"""
DAG: nyc_buildings_bronze
=========================
Ingests the NYC Buildings dataset (Socrata 5zhs-2jue) into the DuckDB bronze
layer using a medallion architecture pattern.

Task flow
---------
check_api_connectivity
        |
check_source_freshness          <-- ShortCircuitOperator: skips downstream
        |                           tasks entirely if source is unchanged
decide_load_mode
        |
ingest_bronze
        |
update_watermark

Pre-check validation tasks (check_api_connectivity, check_source_freshness)
are deliberately separated so failures are clearly attributed and observable
in the Airflow UI without touching the database.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone

import duckdb
import requests
from airflow.decorators import dag, task
from airflow.operators.python import ShortCircuitOperator

from pipeline import extract, state
from pipeline.config import BUILDINGS_DATASET_ID, DB_PATH


# ---------------------------------------------------------------------------
# DAG defaults
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Helper: shared DuckDB connection
# ---------------------------------------------------------------------------

def _get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return duckdb.connect(DB_PATH)


# ---------------------------------------------------------------------------
# Pre-check 1 – API connectivity
# ---------------------------------------------------------------------------

def _check_api_connectivity():
    """
    Confirm the Socrata metadata endpoint is reachable before doing any work.

    Raises on non-2xx response so Airflow marks the task as failed and retries
    according to the DAG's retry policy rather than silently skipping.
    """
    url = f"https://data.cityofnewyork.us/api/views/{BUILDINGS_DATASET_ID}.json"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    print(f"API reachable. Status: {resp.status_code}")


# ---------------------------------------------------------------------------
# Pre-check 2 – Source freshness (ShortCircuit)
# ---------------------------------------------------------------------------

def _check_source_freshness():
    """
    Returns True  → source has new data, proceed with ingest.
    Returns False → source unchanged, skip all downstream tasks.
    """
    current_ts = extract.get_dataset_updated_at(BUILDINGS_DATASET_ID)

    conn = _get_conn()
    state.ensure(conn)
    _, last_dataset_ts = state.get_watermark(conn, BUILDINGS_DATASET_ID)
    conn.close()

    if (
        last_dataset_ts is not None
        and current_ts is not None
        and current_ts <= last_dataset_ts
    ):
        print("Dataset unchanged since last run. Skipping downstream tasks.")
        return False

    print(f"New data detected. current_ts={current_ts}, last_ts={last_dataset_ts}")
    return True


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="nyc_buildings_bronze",
    description="Ingest NYC buildings dataset into the DuckDB bronze layer.",
    schedule="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["bronze", "nyc", "buildings", "geospatial"],
)
def nyc_buildings_bronze_dag():

    # -- Pre-check 1: API reachable? -----------------------------------------
    api_check = task(task_id="check_api_connectivity")(_check_api_connectivity)()

    # -- Pre-check 2: Source data changed? ------------------------------------
    freshness_check = ShortCircuitOperator(
        task_id="check_source_freshness",
        python_callable=_check_source_freshness,
    )

    # -- Decide full vs incremental load -------------------------------------
    @task(task_id="decide_load_mode")
    def decide_load_mode():
        conn = _get_conn()
        state.ensure(conn)
        last_run_at, _ = state.get_watermark(conn, BUILDINGS_DATASET_ID)
        conn.close()

        mode = "full" if last_run_at is None else "incremental"
        watermark = None if last_run_at is None else last_run_at.isoformat()

        print(f"Load mode : {mode}")
        if watermark:
            print(f"Watermark : {watermark} UTC")

        return {"mode": mode, "watermark": watermark}

    # -- Ingest into bronze layer --------------------------------------------
    @task(task_id="ingest_bronze")
    def ingest_bronze(load_config: dict):
        from pipeline import bronze

        mode = load_config["mode"]
        watermark_iso = load_config["watermark"]
        watermark_ts = (
            datetime.fromisoformat(watermark_iso).replace(tzinfo=timezone.utc)
            if watermark_iso
            else None
        )

        run_id = str(uuid.uuid4())
        ingested_at = datetime.now(timezone.utc)

        print(f"Run ID : {run_id}")
        print(f"Mode   : {mode}")

        client = extract.get_client()
        conn = _get_conn()
        total_ingested = 0

        conn.begin()
        try:
            for page_df, total_rows in extract.fetch_pages(
                client, BUILDINGS_DATASET_ID, watermark_ts=watermark_ts
            ):
                rows = bronze.append(
                    conn, page_df, run_id, ingested_at, BUILDINGS_DATASET_ID
                )
                total_ingested += rows
                print(f"  {total_ingested:,} / {total_rows:,} rows ingested")

            conn.commit()
            print(f"Ingestion complete. Total rows: {total_ingested:,}")

        except Exception:
            conn.rollback()
            conn.close()
            raise

        conn.close()

        return {
            "run_id": run_id,
            "ingested_at": ingested_at.isoformat(),
            "rows_ingested": total_ingested,
        }

    # -- Update watermark ----------------------------------------------------
    @task(task_id="update_watermark")
    def update_watermark(ingest_result: dict):
        current_ts = extract.get_dataset_updated_at(BUILDINGS_DATASET_ID)
        ingested_at = datetime.fromisoformat(ingest_result["ingested_at"])

        conn = _get_conn()
        state.set_watermark(
            conn,
            BUILDINGS_DATASET_ID,
            ingested_at,
            current_ts,
            "success",
            ingest_result["rows_ingested"],
        )
        conn.close()

        print(f"Watermark advanced to {ingested_at.isoformat()}")

    # -- Wire up the task graph ----------------------------------------------
    load_config = decide_load_mode()
    ingest_result = ingest_bronze(load_config)

    api_check >> freshness_check >> load_config
    ingest_result >> update_watermark(ingest_result)


nyc_buildings_bronze_dag()
