from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

logger = logging.getLogger(__name__)

import duckdb
import polars as pl

from imagery_pipeline.config import PipelineConfig
from imagery_pipeline.models import RunPlan


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def ensure_schema(conn: duckdb.DuckDBPyConnection, schema_sql_path: Path) -> None:
    conn.execute(Path(schema_sql_path).read_text())


def insert_run(
    conn: duckdb.DuckDBPyConnection,
    plan: RunPlan,
    mode: str,
    status: str,
) -> str:
    run_id = str(uuid4())
    now = datetime.now(UTC)
    df = pl.DataFrame(
        [
            {
                "run_id": run_id,
                "run_started_at": now,
                "mode": mode,
                "status": status,
                "year": plan.year,
                "zoom": plan.zoom,
                "building_count": len(plan.buildings),
                "distinct_tile_count": plan.estimate.distinct_tile_count,
                "raw_tile_bytes_estimate": plan.estimate.raw_tile_bytes,
                "cropped_output_bytes_estimate": plan.estimate.cropped_output_bytes,
                "peak_working_bytes_estimate": plan.estimate.peak_working_bytes,
                "disk_cap_bytes": plan.disk_cap_bytes,
            }
        ]
    )
    conn.register("_runs_df", df)
    conn.execute("INSERT INTO imagery.pipeline_runs SELECT * FROM _runs_df")
    conn.unregister("_runs_df")
    logger.debug("Inserted run %s (mode=%s, status=%s).", run_id, mode, status)
    return run_id


def insert_tile_manifest(conn: duckdb.DuckDBPyConnection, run_id: str, plan: RunPlan) -> None:
    df = pl.DataFrame(
        [
            {
                "run_id": run_id,
                "tile_key": tile.key(),
                "year": tile.year,
                "zoom": tile.z,
                "x": tile.x,
                "y": tile.y,
                "planned_url": f"https://maps.nyc.gov/xyz/1.0.0/photo/{tile.year}/{tile.z}/{tile.x}/{tile.y}.png8",
            }
            for tile in plan.distinct_tiles
        ]
    )
    conn.register("_tile_df", df)
    conn.execute("INSERT INTO imagery.tile_manifest SELECT * FROM _tile_df")
    conn.unregister("_tile_df")
    logger.debug("Inserted tile manifest for run %s (%d tiles).", run_id, len(plan.distinct_tiles))


def insert_bbl_manifest(conn: duckdb.DuckDBPyConnection, run_id: str, plan: RunPlan) -> None:
    df = pl.DataFrame(
        [
            {
                "run_id": run_id,
                "bbl": building.bbl,
                "output_path": str(building.output_path),
                "center_tile_key": building.center_tile.key(),
                "tile_count": len(building.tiles),
                "crop_left": building.crop_window.left,
                "crop_top": building.crop_window.top,
                "crop_right": building.crop_window.right,
                "crop_bottom": building.crop_window.bottom,
                "crop_width": building.crop_window.width(),
                "crop_height": building.crop_window.height(),
            }
            for building in plan.buildings
        ]
    )
    conn.register("_bbl_df", df)
    conn.execute("INSERT INTO imagery.bbl_crops SELECT * FROM _bbl_df")
    conn.unregister("_bbl_df")
    logger.debug("Inserted BBL manifest for run %s (%d buildings).", run_id, len(plan.buildings))


def update_run_status(conn: duckdb.DuckDBPyConnection, run_id: str, status: str) -> None:
    conn.execute(
        "UPDATE imagery.pipeline_runs SET status = ? WHERE run_id = ?",
        [status, run_id],
    )
    logger.debug("Run %s status -> %s.", run_id, status)
