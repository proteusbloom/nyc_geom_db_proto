from __future__ import annotations

import logging
from pathlib import Path

from PIL import Image

logger = logging.getLogger(__name__)

from imagery_pipeline.config import TILE_SIZE
from imagery_pipeline.models import BuildingPlan, TileRef


def _mosaic_dimensions(tiles: tuple[TileRef, ...], tile_size: int = TILE_SIZE) -> tuple[int, int, int, int]:
    min_x = min(tile.x for tile in tiles)
    max_x = max(tile.x for tile in tiles)
    min_y = min(tile.y for tile in tiles)
    max_y = max(tile.y for tile in tiles)
    width = (max_x - min_x + 1) * tile_size
    height = (max_y - min_y + 1) * tile_size
    return min_x, min_y, width, height


def crop_building(plan: BuildingPlan, tile_paths: dict[TileRef, Path], tile_size: int = TILE_SIZE) -> Path:
    try:
        min_x, min_y, width, height = _mosaic_dimensions(plan.tiles, tile_size)
        mosaic = Image.new("RGBA", (width, height))

        for tile in plan.tiles:
            tile_image = Image.open(tile_paths[tile]).convert("RGBA")
            offset_x = (tile.x - min_x) * tile_size
            offset_y = (tile.y - min_y) * tile_size
            mosaic.paste(tile_image, (offset_x, offset_y))

        cropped = mosaic.crop(
            (
                plan.crop_window.left,
                plan.crop_window.top,
                plan.crop_window.right,
                plan.crop_window.bottom,
            )
        )
        plan.output_path.parent.mkdir(parents=True, exist_ok=True)
        cropped.save(plan.output_path)
        logger.debug("Cropped BBL %s -> %s", plan.bbl, plan.output_path)
        return plan.output_path
    except Exception:
        logger.warning("Failed to crop BBL %s", plan.bbl, exc_info=True)
        raise
