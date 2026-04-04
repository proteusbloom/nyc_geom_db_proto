from __future__ import annotations

import logging
from pathlib import Path

import httpx
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt, wait_exponential

from imagery_pipeline.config import PipelineConfig
from imagery_pipeline.models import TileRef

logger = logging.getLogger(__name__)


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransientError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500 or exc.response.status_code == 429
    return False


def _log_retry(retry_state: RetryCallState) -> None:
    logger.warning(
        "Tile download failed (attempt %d), retrying in %.1fs — %s",
        retry_state.attempt_number,
        retry_state.next_action.sleep,  # type: ignore[union-attr]
        retry_state.outcome.exception(),
    )


class TileDownloader:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def tile_path(self, tile: TileRef) -> Path:
        return Path(self.config.temp_tile_dir) / str(tile.year) / str(tile.z) / str(tile.x) / f"{tile.y}.png"

    @retry(
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
        before_sleep=_log_retry,
    )
    def download_tile(self, client: httpx.Client, tile: TileRef) -> Path:
        path = self.tile_path(tile)
        if path.exists():
            logger.debug("Tile cache hit: %s", tile.key())
            return path

        logger.debug("Downloading tile: %s", tile.key())
        path.parent.mkdir(parents=True, exist_ok=True)
        response = client.get(self.config.tile_url(tile.x, tile.y))
        response.raise_for_status()
        path.write_bytes(response.content)
        return path

    def download_tiles(self, tiles: list[TileRef] | tuple[TileRef, ...]) -> dict[TileRef, Path]:
        headers = {"User-Agent": self.config.user_agent}
        downloaded: dict[TileRef, Path] = {}
        total = len(tiles)
        logger.info("Downloading %d distinct tiles.", total)
        with httpx.Client(timeout=self.config.http_timeout, headers=headers, follow_redirects=True) as client:
            for i, tile in enumerate(tiles, 1):
                downloaded[tile] = self.download_tile(client, tile)
                logger.debug("Tile %d/%d done: %s", i, total, tile.key())
        logger.info("Tile downloads complete: %d tiles.", total)
        return downloaded
