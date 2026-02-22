"""
Socrata extraction layer.

Responsibilities:
  - Metadata pre-flight check (skip run if source unchanged)
  - Row-count queries for progress reporting
  - Paginated fetch with watermark filtering
  - In-place geometry serialization (dict -> JSON string) so every DataFrame
    that leaves this module has a consistent Utf8 the_geom column rather than
    a nested Struct, which DuckDB can store as VARCHAR and later parse with
    ST_GeomFromGeoJSON().
"""

import json

import polars as pl
import requests
from sodapy import Socrata

from pipeline.config import (
    PAGE_SIZE,
    SOCRATA_APP_TOKEN,
    SOCRATA_DOMAIN,
    SOCRATA_TIMEOUT,
)


def get_client():
    return Socrata(SOCRATA_DOMAIN, app_token=SOCRATA_APP_TOKEN, timeout=SOCRATA_TIMEOUT)


def get_dataset_updated_at(dataset_id):
    """
    Return the rowsUpdatedAt Unix epoch (int) from the Socrata metadata endpoint.

    This is a cheap HEAD-style check: if the value hasn't advanced since the
    last recorded run, the pipeline can skip the fetch entirely.
    Returns None if the field is absent (treat as unknown / always fetch).
    """
    url = f"https://{SOCRATA_DOMAIN}/api/views/{dataset_id}.json"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json().get("rowsUpdatedAt")


def _row_count(client, dataset_id, where=None):
    params = {"select": "COUNT(*)"}
    if where:
        params["where"] = where
    result = client.get(dataset_id, **params)
    return int(list(result[0].values())[0])


def _serialize_geom(records):
    """
    Serialize the_geom from a Python dict (GeoJSON) to a JSON string in-place.

    sodapy deserializes the Socrata GeoJSON geometry into a nested Python dict.
    Polars would auto-infer this as a Struct, which is inconvenient for storage
    and later spatial parsing.  Converting to a plain string here keeps the
    schema predictable across all pages and lets DuckDB handle it as VARCHAR.
    """
    for record in records:
        geom = record.get("the_geom")
        if isinstance(geom, dict):
            record["the_geom"] = json.dumps(geom)
        elif geom is None:
            record["the_geom"] = None
    return records


def fetch_pages(client, dataset_id, watermark_ts=None, row_limit=None):
    """
    Generator that yields (page: pl.DataFrame, total_rows: int).

    On a full load (watermark_ts=None) this will page through the entire
    dataset.  On an incremental run it filters to records edited after the
    watermark, keeping memory usage bounded at PAGE_SIZE rows at a time.

    Parameters
    ----------
    watermark_ts : datetime | None
        UTC datetime used as the lower-bound filter on last_edited_date.
        Pass None to fetch the full dataset.
    row_limit : int | None
        Cap the total number of rows fetched.  Useful for testing without
        pulling the full dataset.
    """
    where = None
    if watermark_ts is not None:
        # Socrata floating timestamp format (no timezone suffix)
        ts_str = watermark_ts.strftime("%Y-%m-%dT%H:%M:%S.000")
        where = f"last_edited_date > '{ts_str}'"

    total_rows = _row_count(client, dataset_id, where=where)
    if row_limit is not None:
        total_rows = min(total_rows, row_limit)
    print(f"Target rows : {total_rows:,}")

    if total_rows == 0:
        return

    query_params = {}
    if where:
        query_params["where"] = where

    offset = 0
    while offset < total_rows:
        query_params["limit"] = PAGE_SIZE
        query_params["offset"] = offset

        page = client.get(dataset_id, **query_params)
        if not page:
            break

        _serialize_geom(page)
        yield pl.DataFrame(page), total_rows

        offset += len(page)
        # API returned a short page – we've hit the end
        if len(page) < PAGE_SIZE:
            break
