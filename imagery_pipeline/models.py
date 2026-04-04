from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Bounds4326:
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    def width(self) -> float:
        return self.max_lon - self.min_lon

    def height(self) -> float:
        return self.max_lat - self.min_lat


@dataclass(frozen=True)
class ResolvedBuilding:
    bbl: str
    bounds: Bounds4326


@dataclass(frozen=True)
class TileRef:
    year: int
    z: int
    x: int
    y: int

    def key(self) -> str:
        return f"{self.year}/{self.z}/{self.x}/{self.y}"


@dataclass(frozen=True)
class CropWindow:
    left: int
    top: int
    right: int
    bottom: int

    def width(self) -> int:
        return self.right - self.left

    def height(self) -> int:
        return self.bottom - self.top


@dataclass(frozen=True)
class BuildingPlan:
    bbl: str
    bounds: Bounds4326
    center_tile: TileRef
    tiles: tuple[TileRef, ...]
    crop_window: CropWindow
    output_path: Path


@dataclass(frozen=True)
class StorageEstimate:
    distinct_tile_count: int
    raw_tile_bytes: int
    cropped_output_bytes: int
    peak_working_bytes: int
    within_cap: bool


@dataclass(frozen=True)
class RunPlan:
    year: int
    zoom: int
    disk_cap_bytes: int
    buildings: tuple[BuildingPlan, ...]
    distinct_tiles: tuple[TileRef, ...]
    estimate: StorageEstimate
