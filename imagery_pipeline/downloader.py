from __future__ import annotations

from pathlib import Path

import httpx

from imagery_pipeline.config import PipelineConfig
from imagery_pipeline.models import TileRef


class TileDownloader:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def tile_path(self, tile: TileRef) -> Path:
        return Path(self.config.temp_tile_dir) / str(tile.year) / str(tile.z) / str(tile.x) / f"{tile.y}.png"

    def download_tile(self, client: httpx.Client, tile: TileRef) -> Path:
        path = self.tile_path(tile)
        if path.exists():
            return path

        path.parent.mkdir(parents=True, exist_ok=True)
        response = client.get(self.config.tile_url(tile.x, tile.y))
        response.raise_for_status()
        path.write_bytes(response.content)
        return path

    def download_tiles(self, tiles: list[TileRef] | tuple[TileRef, ...]) -> dict[TileRef, Path]:
        headers = {"User-Agent": self.config.user_agent}
        downloaded: dict[TileRef, Path] = {}
        with httpx.Client(timeout=self.config.http_timeout, headers=headers, follow_redirects=True) as client:
            for tile in tiles:
                downloaded[tile] = self.download_tile(client, tile)
        return downloaded
