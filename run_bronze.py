"""
Bronze pipeline entry point.

Run this script weekly (cron, task scheduler, etc.) to ingest the NYC
buildings dataset into the bronze layer of the DuckDB medallion store.

Flow
----
1. Pre-flight metadata check – compare Socrata's rowsUpdatedAt against the
   stored watermark.  Skip the run entirely if the source hasn't changed.
2. Determine load mode:
     Full load   – no prior successful run exists (first-time setup)
     Incremental – filter to records where last_edited_date > last_run_at
3. Page through Socrata within a single DuckDB transaction.
4. On success: commit + advance the watermark.
   On failure:  rollback (no partial data in bronze) + record the error.

Usage
-----
    python run_bronze.py
"""

import os
import uuid
from datetime import datetime, timezone

import duckdb

from pipeline import bronze, extract, state
from pipeline.config import BUILDINGS_DATASET_ID, DB_PATH


def run():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = duckdb.connect(DB_PATH)

    # --- Setup and watermark check (auto-committed, outside data transaction) ---
    state.ensure(conn)
    last_run_at, last_dataset_ts = state.get_watermark(conn, BUILDINGS_DATASET_ID)
    current_dataset_ts = extract.get_dataset_updated_at(BUILDINGS_DATASET_ID)

    if (
        last_dataset_ts is not None
        and current_dataset_ts is not None
        and current_dataset_ts <= last_dataset_ts
    ):
        print("Dataset unchanged since last run. Skipping.")
        conn.close()
        return

    run_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc)
    is_full_load = last_run_at is None
    watermark = None if is_full_load else last_run_at

    print(f"Run ID : {run_id}")
    if is_full_load:
        print("Mode   : full load")
    else:
        print(f"Mode   : incremental (since {watermark} UTC)")

    client = extract.get_client()
    total_ingested = 0
    success = False

    # --- Data ingestion (single transaction – rollback on any failure) ---
    conn.begin()
    try:
        for page_df, total_rows in extract.fetch_pages(
            client, BUILDINGS_DATASET_ID, watermark_ts=watermark
        ):
            rows = bronze.append(
                conn, page_df, run_id, ingested_at, BUILDINGS_DATASET_ID
            )
            total_ingested += rows
            print(f"  {total_ingested:,} / {total_rows:,} rows", end="\r")

        conn.commit()
        success = True
        print(f"\nDone. Rows ingested : {total_ingested:,}")

    except Exception as e:
        conn.rollback()
        print(f"\nFailed after {total_ingested:,} rows: {e}")
        raise

    finally:
        # Always record the outcome; runs outside the data transaction so it
        # persists regardless of commit/rollback.
        state.set_watermark(
            conn,
            BUILDINGS_DATASET_ID,
            ingested_at,
            current_dataset_ts if success else last_dataset_ts,
            "success" if success else "failed",
            total_ingested,
        )
        conn.close()


if __name__ == "__main__":
    run()
