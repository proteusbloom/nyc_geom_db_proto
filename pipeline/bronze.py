"""
Bronze layer loader.

Appends raw Socrata pages to bronze.buildings_raw in DuckDB.

Design notes:
  - Append-only.  Rows are never updated or deleted here.
  - Schema is inferred from the first page and then reused; all data columns
    arrive as Utf8 strings from Socrata (including numeric fields like
    shape_area), so the schema is stable across pages.
  - Three metadata columns are injected per row:
      run_id            – UUID shared across all pages in one pipeline run
      ingested_at       – UTC timestamp of the run start
      source_dataset_id – Socrata dataset identifier
  - the_geom is stored as a plain VARCHAR (GeoJSON string).  Silver will
    parse it with ST_GeomFromGeoJSON() via the DuckDB spatial extension.
  - Table creation is lazy: the first call to append() creates the table by
    reflecting the DataFrame schema; subsequent calls just INSERT.
"""

import polars as pl

_SCHEMA = "bronze"
_TABLE = f"{_SCHEMA}.buildings_raw"


def _ensure_table(conn, df):
    """
    Create bronze.buildings_raw if it doesn't exist, deriving the schema
    from the supplied DataFrame.  The bronze schema itself is guaranteed to
    exist before any data transaction begins (state.ensure() creates it).
    """
    exists = conn.execute(
        f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = '{_SCHEMA}' AND table_name = 'buildings_raw'
        """
    ).fetchone()[0]

    if not exists:
        conn.register("_schema_ref", df)
        conn.execute(
            f"CREATE TABLE {_TABLE} AS SELECT * FROM _schema_ref WHERE 1=0"
        )
        conn.unregister("_schema_ref")


def append(conn, df, run_id, ingested_at, source_dataset_id):
    """
    Append one page of raw Socrata data to bronze.buildings_raw.

    Metadata columns are appended on the right so they don't interfere with
    the raw field layout.  Returns the number of rows written.
    """
    df = df.with_columns(
        pl.lit(run_id).alias("run_id"),
        pl.lit(ingested_at).alias("ingested_at"),
        pl.lit(source_dataset_id).alias("source_dataset_id"),
    )

    _ensure_table(conn, df)

    conn.register("_bronze_batch", df)
    conn.execute(f"INSERT INTO {_TABLE} SELECT * FROM _bronze_batch")
    conn.unregister("_bronze_batch")

    return len(df)
