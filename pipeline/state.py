"""
Watermark state management for the bronze pipeline.

Tracks per-dataset run history in bronze.pipeline_state so the extractor
knows whether to do a full load or an incremental pull.
"""

_SCHEMA = "bronze"
_TABLE = f"{_SCHEMA}.pipeline_state"

_INIT_DDL = f"""
    CREATE SCHEMA IF NOT EXISTS {_SCHEMA};
    CREATE TABLE IF NOT EXISTS {_TABLE} (
        dataset_id          VARCHAR PRIMARY KEY,
        last_run_at         TIMESTAMP,
        dataset_updated_at  BIGINT,
        last_run_status     VARCHAR,
        rows_ingested       INTEGER
    );
"""


def ensure(conn):
    """Create schema and state table if they don't exist (auto-committed DDL)."""
    conn.execute(_INIT_DDL)


def get_watermark(conn, dataset_id):
    """
    Return (last_run_at: datetime | None, dataset_updated_at: int | None).

    last_run_at        – UTC timestamp of the last successful run; used as the
                         last_edited_date watermark on the next incremental pull.
    dataset_updated_at – rowsUpdatedAt epoch from Socrata metadata; used to
                         skip the run entirely if the source hasn't changed.
    """
    row = conn.execute(
        f"SELECT last_run_at, dataset_updated_at FROM {_TABLE} WHERE dataset_id = ?",
        [dataset_id],
    ).fetchone()
    return (row[0], row[1]) if row else (None, None)


def set_watermark(conn, dataset_id, run_at, dataset_updated_at, status, rows_ingested):
    """Upsert a run record into the state table."""
    conn.execute(
        f"""
        INSERT INTO {_TABLE}
            (dataset_id, last_run_at, dataset_updated_at, last_run_status, rows_ingested)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (dataset_id) DO UPDATE SET
            last_run_at        = excluded.last_run_at,
            dataset_updated_at = excluded.dataset_updated_at,
            last_run_status    = excluded.last_run_status,
            rows_ingested      = excluded.rows_ingested
        """,
        [dataset_id, run_at, dataset_updated_at, status, rows_ingested],
    )
