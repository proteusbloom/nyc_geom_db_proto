from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from imagery_pipeline.config import PipelineConfig
from imagery_pipeline.models import BuildingPlan, CropWindow, ResolvedBuilding, RunPlan, StorageEstimate, TileRef
from imagery_pipeline.tile_math import (
    bounds_to_pixel_window,
    center_tile_for_bounds,
    crop_window_relative_to_tiles,
    required_tiles_for_window,
)


def estimate_storage(
    distinct_tile_count: int,
    crop_windows: Iterable[CropWindow],
    config: PipelineConfig,
) -> StorageEstimate:
    raw_tile_bytes = distinct_tile_count * config.tile_bytes_estimate
    cropped_output_bytes = sum(
        window.width() * window.height() * config.output_bytes_per_pixel
        for window in crop_windows
    )
    peak_working_bytes = raw_tile_bytes + cropped_output_bytes
    return StorageEstimate(
        distinct_tile_count=distinct_tile_count,
        raw_tile_bytes=raw_tile_bytes,
        cropped_output_bytes=cropped_output_bytes,
        peak_working_bytes=peak_working_bytes,
        within_cap=peak_working_bytes < config.disk_cap_bytes,
    )


def plan_building(building: ResolvedBuilding, config: PipelineConfig) -> BuildingPlan:
    absolute_window = bounds_to_pixel_window(building.bounds, config.zoom, config.tile_size)
    center_tile = center_tile_for_bounds(building.bounds, config.year, config.zoom, config.tile_size)
    required_tiles = required_tiles_for_window(absolute_window, config.year, config.zoom, config.tile_size)

    if center_tile in required_tiles:
        tiles = required_tiles
    else:
        tiles = (center_tile,)

    relative_window = crop_window_relative_to_tiles(absolute_window, tiles, config.tile_size)
    return BuildingPlan(
        bbl=building.bbl,
        bounds=building.bounds,
        center_tile=center_tile,
        tiles=tiles,
        crop_window=relative_window,
        output_path=Path(config.output_dir) / f"{building.bbl}.png",
    )


def plan_run(buildings: Iterable[ResolvedBuilding], config: PipelineConfig) -> RunPlan:
    building_plans = tuple(plan_building(building, config) for building in buildings)
    tile_map: dict[str, TileRef] = {}
    for plan in building_plans:
        for tile in plan.tiles:
            tile_map[tile.key()] = tile

    distinct_tiles = tuple(sorted(tile_map.values(), key=lambda tile: (tile.z, tile.x, tile.y)))
    estimate = estimate_storage(len(distinct_tiles), (plan.crop_window for plan in building_plans), config)
    return RunPlan(
        year=config.year,
        zoom=config.zoom,
        disk_cap_bytes=config.disk_cap_bytes,
        buildings=building_plans,
        distinct_tiles=distinct_tiles,
        estimate=estimate,
    )
