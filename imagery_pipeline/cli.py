from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from pathlib import Path

from imagery_pipeline.config import PipelineConfig
from imagery_pipeline.cropper import crop_building
from imagery_pipeline.downloader import TileDownloader
from imagery_pipeline.duckdb_store import (
    connect,
    ensure_schema,
    insert_bbl_manifest,
    insert_run,
    insert_tile_manifest,
    update_run_status,
)
from imagery_pipeline.geometry import StubGeometryResolver
from imagery_pipeline.models import Bounds4326
from imagery_pipeline.planner import plan_run

logger = logging.getLogger(__name__)


def _load_bbl_mapping(path: Path) -> dict[str, Bounds4326]:
    raw = json.loads(path.read_text())
    mapping: dict[str, Bounds4326] = {}
    for bbl, bounds in raw.items():
        mapping[str(bbl)] = Bounds4326(
            min_lon=bounds["min_lon"],
            min_lat=bounds["min_lat"],
            max_lon=bounds["max_lon"],
            max_lat=bounds["max_lat"],
        )
    return mapping


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="imagery-pipeline")
    parser.add_argument("command", choices=["preflight", "run"])
    parser.add_argument("--input", required=True, help="Path to BBL->bounds JSON for stubbed resolution.")
    parser.add_argument("--db-path", default="db/nyc_buildings.duckdb")
    parser.add_argument("--year", type=int, default=2018)
    parser.add_argument("--zoom", type=int, default=19)
    parser.add_argument("--disk-cap-bytes", type=int, default=5_000_000_000)
    parser.add_argument("--temp-tile-dir", default="data/imagery/tmp_tiles")
    parser.add_argument("--output-dir", default="data/imagery/crops")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging verbosity (default: INFO).",
    )
    return parser


def run_cli(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    config = PipelineConfig(
        db_path=Path(args.db_path),
        year=args.year,
        zoom=args.zoom,
        disk_cap_bytes=args.disk_cap_bytes,
        temp_tile_dir=Path(args.temp_tile_dir),
        output_dir=Path(args.output_dir),
    )

    logger.info("Starting imagery pipeline: command=%s input=%s", args.command, args.input)
    resolver = StubGeometryResolver(_load_bbl_mapping(Path(args.input)))
    buildings = resolver.resolve_many(_load_bbl_mapping(Path(args.input)).keys())
    plan = plan_run(buildings, config)

    conn = connect(config.db_path)
    ensure_schema(conn, config.schema_sql_path)
    run_id = insert_run(conn, plan, mode=args.command, status="planned")
    insert_tile_manifest(conn, run_id, plan)
    insert_bbl_manifest(conn, run_id, plan)

    if not plan.estimate.within_cap:
        update_run_status(conn, run_id, "rejected_storage_cap")
        logger.warning(
            "Projected working set %d bytes exceeds cap %d bytes — run rejected.",
            plan.estimate.peak_working_bytes,
            plan.disk_cap_bytes,
        )
        return 2

    logger.info(
        "Planned %d crops across %d distinct tiles. Projected peak bytes: %d.",
        len(plan.buildings),
        len(plan.distinct_tiles),
        plan.estimate.peak_working_bytes,
    )
    if args.command == "preflight":
        update_run_status(conn, run_id, "preflight_ok")
        return 0

    downloader = TileDownloader(config)
    tile_paths = downloader.download_tiles(plan.distinct_tiles)
    tile_ref_counts = Counter(tile for building in plan.buildings for tile in building.tiles)
    for building in plan.buildings:
        crop_building(building, tile_paths)
        if config.delete_temp_tiles:
            for tile in building.tiles:
                tile_ref_counts[tile] -= 1
                if tile_ref_counts[tile] == 0:
                    tile_path = tile_paths[tile]
                    if tile_path.exists():
                        tile_path.unlink()

    update_run_status(conn, run_id, "completed")
    logger.info("Completed run %s.", run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(run_cli())
