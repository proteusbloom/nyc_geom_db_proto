"""
Silver layer one-time load for the NYC subgrade dataset (bsin-59hv).

Fetches only the required columns from Socrata, parses geometry with
ST_GeomFromGeoJSON(), casts numeric fields to DOUBLE, and stores the
result in silver.subgrade.

Run:
    python -m subgrade_pipeline.run_silver            # full load
    python -m subgrade_pipeline.run_silver --limit N  # test with N rows
"""

import argparse
import os
import uuid
from datetime import datetime, timezone

import duckdb
import polars as pl

from building_pipeline.config import DB_PATH, PAGE_SIZE
from building_pipeline.extract import _row_count, _serialize_geom, get_client
from subgrade_pipeline.config import SUBGRADE_COLUMNS, SUBGRADE_DATASET_ID

_TABLE = "silver.subgrade"

_CREATE_DDL = """
CREATE TABLE silver.subgrade (
    the_geom      GEOMETRY,
    bin           VARCHAR PRIMARY KEY,
    z_grade       DOUBLE,
    z_floor       DOUBLE,
    subgrade      VARCHAR,
    notes1        VARCHAR,
    notes2        VARCHAR,
    notes3        VARCHAR,
    latitude      DOUBLE,
    longitude     DOUBLE,
    run_id        VARCHAR,
    ingested_at   TIMESTAMPTZ
)
"""


def _fetch_pages(client, row_limit=None):
    select_str = ",".join(SUBGRADE_COLUMNS)
    total = _row_count(client, SUBGRADE_DATASET_ID)
    if row_limit is not None:
        total = min(total, row_limit)
    print(f"Target rows : {total:,}")

    offset = 0
    while offset < total:
        page = client.get(
            SUBGRADE_DATASET_ID,
            select=select_str,
            limit=PAGE_SIZE,
            offset=offset,
        )
        if not page:
            break
        _serialize_geom(page)
        yield pl.DataFrame(page), total
        offset += len(page)
        if len(page) < PAGE_SIZE:
            break


def _append(conn, df, run_id, ingested_at):
    df = df.with_columns(
        pl.lit(run_id).alias("run_id"),
        pl.lit(ingested_at).alias("ingested_at"),
    )
    conn.register("_subgrade_batch", df)
    conn.execute(f"""
        INSERT INTO {_TABLE}
        SELECT
            ST_GeomFromGeoJSON(the_geom),
            bin,
            TRY_CAST(z_grade AS DOUBLE),
            TRY_CAST(z_floor AS DOUBLE),
            subgrade,
            notes1,
            notes2,
            notes3,
            TRY_CAST(latitude AS DOUBLE),
            TRY_CAST(longitude AS DOUBLE),
            run_id,
            ingested_at::TIMESTAMPTZ
        FROM _subgrade_batch
    """)
    conn.unregister("_subgrade_batch")
    return len(df)


def run(row_limit=None):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = duckdb.connect(DB_PATH)
    try:
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")

        conn.execute(f"DROP TABLE IF EXISTS {_TABLE}")

        run_id = str(uuid.uuid4())
        ingested_at = datetime.now(timezone.utc)
        print(f"Run ID     : {run_id}")
        print(f"Dataset ID : {SUBGRADE_DATASET_ID}")

        client = get_client()
        total_ingested = 0
        total_rows = 0

        conn.begin()
        try:
            conn.execute(_CREATE_DDL)
            for page_df, total_rows in _fetch_pages(client, row_limit):
                rows = _append(conn, page_df, run_id, ingested_at)
                total_ingested += rows
                print(f"  {total_ingested:,} / {total_rows:,} rows", end="\r")
            if total_ingested != total_rows:
                raise RuntimeError(
                    f"Completeness check failed: expected {total_rows:,} rows "
                    f"but ingested {total_ingested:,}."
                )
            conn.commit()
            print(f"\nDone. Rows ingested: {total_ingested:,}")
        except Exception as e:
            conn.rollback()
            print(f"\nFailed after {total_ingested:,} rows: {e}")
            raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="One-time full load of NYC subgrade dataset into silver layer."
    )
    parser.add_argument(
        "--limit", type=int, default=None, metavar="N",
        help="Cap total rows fetched (useful for testing).",
    )
    args = parser.parse_args()
    run(row_limit=args.limit)
