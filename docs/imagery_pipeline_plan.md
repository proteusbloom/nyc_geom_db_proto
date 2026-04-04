# NYC Building Imagery Pipeline Plan

## Goal

Build a storage-bounded pipeline that accepts a dictionary of BBLs, resolves each BBL to a building geometry footprint or bounds, downloads only the required NYC aerial imagery tiles, crops one output image per BBL, and writes run/manifests into DuckDB for downstream joins with an adjacent imagery table.

## Constraints Captured

- Imagery source is fixed to `https://maps.nyc.gov/xyz/1.0.0/photo/{year}/{z}/{x}/{y}.png8`
- Defaults: `year=2018`, `zoom=19`
- Primary libraries: `polars`, `duckdb`
- Additional dependencies kept to `pillow` and `httpx`
- Default fetch mode is center tile only
- Neighbor tiles are fetched only if a crop crosses a tile boundary
- Distinct tiles are deduplicated before download
- A preflight storage estimate is required before any download
- Total disk use must remain under a configurable cap, defaulting to `< 5 GB`
- Temporary raw tiles are deleted after successful crop when they are no longer referenced

## Proposed Flow

1. Accept BBL dictionary input from CLI or Python API.
2. Resolve each BBL to lon/lat bounds through a supplied resolver callable.
3. Convert building bounds to tile-space pixel windows at the target year/zoom.
4. In planning mode:
   - determine center tile
   - determine whether boundary-crossing requires neighbor tiles
   - deduplicate all required `(year, z, x, y)` tiles
   - estimate raw-tile bytes, cropped-image bytes, and total working-set bytes
   - abort if projected storage exceeds cap
5. Download missing tiles into a temp cache.
6. Crop one output image per BBL from a 1x1, 2x1, 1x2, or 2x2 stitched tile mosaic.
7. Record manifests in DuckDB:
   - one run row
   - one row per building crop
   - one row per distinct tile planned/downloaded
8. Delete raw temp tiles after all dependent crops complete.

## Package Layout

```text
imagery_pipeline/
  __init__.py
  cli.py
  config.py
  cropper.py
  downloader.py
  duckdb_store.py
  geometry.py
  models.py
  planner.py
  tile_math.py
db/
  imagery_schema.sql
tests/
  test_cropper.py
  test_planner.py
  test_tile_math.py
```

## Data Model

- `imagery.pipeline_runs`: one row per CLI/API run with defaults, byte estimates, counts, and status
- `imagery.tile_manifest`: one row per distinct tile planned/downloaded
- `imagery.bbl_crops`: one row per BBL crop output with image path, dimensions, tile coverage, and crop bounds in tile pixel space

## Implementation Notes

- Geometry resolver contract is intentionally small: `resolve_many(bbls) -> list[ResolvedBuilding]`
- The initial scaffold uses bounding boxes in EPSG:4326 to avoid introducing spatial dependencies
- Tile math is pure Python and deterministic, so it is cheap to unit test
- DuckDB writes use registered Polars DataFrames for simple batch inserts
- The CLI exposes:
  - `preflight`: resolve, plan, estimate, and write manifests without download
  - `run`: execute full pipeline after passing preflight

## Risks / Follow-Up

- Bounding-box crops are a pragmatic first step, but exact footprint masking would require additional geometry tooling
- Storage estimation for PNG outputs is approximate; the planner uses pessimistic crop sizing to stay on the safe side
- If the future resolver returns multipart or invalid geometries, add a normalization stage before planning
