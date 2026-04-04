from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


TILE_SIZE = 256
DEFAULT_YEAR = 2018
DEFAULT_ZOOM = 19
DEFAULT_DISK_CAP_BYTES = 5_000_000_000
DEFAULT_TILE_BYTES_ESTIMATE = 180_000
DEFAULT_OUTPUT_BYTES_PER_PIXEL = 2
DEFAULT_HTTP_TIMEOUT = 30.0
DEFAULT_USER_AGENT = "nyc-imagery-pipeline/0.1"
TILE_URL_TEMPLATE = "https://maps.nyc.gov/xyz/1.0.0/photo/{year}/{z}/{x}/{y}.png8"


@dataclass(frozen=True)
class PipelineConfig:
    db_path: Path = Path("db/nyc_buildings.duckdb")
    schema_sql_path: Path = Path("db/imagery_schema.sql")
    temp_tile_dir: Path = Path("data/imagery/tmp_tiles")
    output_dir: Path = Path("data/imagery/crops")
    year: int = DEFAULT_YEAR
    zoom: int = DEFAULT_ZOOM
    disk_cap_bytes: int = DEFAULT_DISK_CAP_BYTES
    tile_size: int = TILE_SIZE
    tile_bytes_estimate: int = DEFAULT_TILE_BYTES_ESTIMATE
    output_bytes_per_pixel: int = DEFAULT_OUTPUT_BYTES_PER_PIXEL
    http_timeout: float = DEFAULT_HTTP_TIMEOUT
    user_agent: str = DEFAULT_USER_AGENT
    delete_temp_tiles: bool = True
    dry_run: bool = False

    def tile_url(self, x: int, y: int) -> str:
        return TILE_URL_TEMPLATE.format(year=self.year, z=self.zoom, x=x, y=y)
