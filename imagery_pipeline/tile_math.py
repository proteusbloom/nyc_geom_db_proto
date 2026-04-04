from __future__ import annotations

import math

from imagery_pipeline.config import TILE_SIZE
from imagery_pipeline.models import Bounds4326, CropWindow, TileRef


def lon_lat_to_global_pixels(lon: float, lat: float, zoom: int, tile_size: int = TILE_SIZE) -> tuple[float, float]:
    lat = max(min(lat, 85.05112878), -85.05112878)
    scale = tile_size * (2**zoom)
    x = (lon + 180.0) / 360.0 * scale
    lat_rad = math.radians(lat)
    y = (
        (1.0 - math.log(math.tan(lat_rad) + (1.0 / math.cos(lat_rad))) / math.pi)
        / 2.0
        * scale
    )
    return x, y


def global_pixels_to_tile(px: float, py: float, year: int, zoom: int, tile_size: int = TILE_SIZE) -> TileRef:
    return TileRef(year=year, z=zoom, x=int(px // tile_size), y=int(py // tile_size))


def bounds_to_pixel_window(bounds: Bounds4326, zoom: int, tile_size: int = TILE_SIZE) -> CropWindow:
    left, top = lon_lat_to_global_pixels(bounds.min_lon, bounds.max_lat, zoom, tile_size)
    right, bottom = lon_lat_to_global_pixels(bounds.max_lon, bounds.min_lat, zoom, tile_size)
    left_i = math.floor(left)
    top_i = math.floor(top)
    right_i = math.ceil(right)
    bottom_i = math.ceil(bottom)
    if right_i <= left_i:
        right_i = left_i + 1
    if bottom_i <= top_i:
        bottom_i = top_i + 1
    return CropWindow(left=left_i, top=top_i, right=right_i, bottom=bottom_i)


def required_tiles_for_window(window: CropWindow, year: int, zoom: int, tile_size: int = TILE_SIZE) -> tuple[TileRef, ...]:
    min_tile_x = window.left // tile_size
    max_tile_x = (window.right - 1) // tile_size
    min_tile_y = window.top // tile_size
    max_tile_y = (window.bottom - 1) // tile_size
    refs = []
    for tile_y in range(min_tile_y, max_tile_y + 1):
        for tile_x in range(min_tile_x, max_tile_x + 1):
            refs.append(TileRef(year=year, z=zoom, x=tile_x, y=tile_y))
    return tuple(refs)


def center_tile_for_bounds(bounds: Bounds4326, year: int, zoom: int, tile_size: int = TILE_SIZE) -> TileRef:
    center_lon = (bounds.min_lon + bounds.max_lon) / 2.0
    center_lat = (bounds.min_lat + bounds.max_lat) / 2.0
    px, py = lon_lat_to_global_pixels(center_lon, center_lat, zoom, tile_size)
    return global_pixels_to_tile(px, py, year, zoom, tile_size)


def crop_window_relative_to_tiles(window: CropWindow, tiles: tuple[TileRef, ...], tile_size: int = TILE_SIZE) -> CropWindow:
    min_x = min(tile.x for tile in tiles)
    min_y = min(tile.y for tile in tiles)
    origin_x = min_x * tile_size
    origin_y = min_y * tile_size
    return CropWindow(
        left=window.left - origin_x,
        top=window.top - origin_y,
        right=window.right - origin_x,
        bottom=window.bottom - origin_y,
    )
