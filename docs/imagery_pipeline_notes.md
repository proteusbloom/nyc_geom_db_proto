# Imagery Pipeline Notes

Analysis of `imagery_pipeline/` module. Captured for reference — imagery pipeline is not yet active; building pipeline takes priority.

---

## CLI (`imagery_pipeline/cli.py`)

Entry point: `python -m imagery_pipeline.cli <command>`

**Commands:** `preflight` | `run`

| Flag | Default | Purpose |
|---|---|---|
| `--input` | required | Path to BBL→bounds JSON `{bbl: {min_lon, min_lat, max_lon, max_lat}}` |
| `--db-path` | `db/nyc_buildings.duckdb` | DuckDB file |
| `--year` | 2018 | Imagery year for NYC Maps tile URL |
| `--zoom` | 19 | Tile zoom level |
| `--disk-cap-bytes` | 5,000,000,000 | Reject run if projected working set exceeds this |
| `--temp-tile-dir` | `data/imagery/tmp_tiles` | Raw tile cache location |
| `--output-dir` | `data/imagery/crops` | Cropped PNG output location |
| `--log-level` | INFO | DEBUG / INFO / WARNING / ERROR |

**No `--limit` flag.** To limit rows, pre-filter the input JSON to the desired number of BBLs.

---

## DuckDB Schema (`db/imagery_schema.sql`)

Three tables under the `imagery` schema:

### `imagery.pipeline_runs`
| Column | Type | Notes |
|---|---|---|
| `run_id` | VARCHAR PK | UUID per CLI invocation |
| `run_started_at` | TIMESTAMPTZ | UTC |
| `mode` | VARCHAR | `"preflight"` or `"run"` |
| `status` | VARCHAR | `planned` → `preflight_ok` / `rejected_storage_cap` / `completed` |
| `year`, `zoom` | INT | From CLI args |
| `building_count`, `distinct_tile_count` | INT | From plan |
| `*_estimate`, `disk_cap_bytes` | INT | Storage accounting |

### `imagery.tile_manifest`
| Column | Notes |
|---|---|
| `run_id` | FK → pipeline_runs |
| `tile_key` | `"{year}/{z}/{x}/{y}"` |
| `x`, `y`, `z`, `year` | Tile coordinates |
| `planned_url` | `https://maps.nyc.gov/xyz/1.0.0/photo/...` |

### `imagery.bbl_crops`
| Column | Notes |
|---|---|
| `run_id` | FK → pipeline_runs |
| `bbl` | Building identifier (BBL or BIN — depends on input JSON key) |
| `output_path` | PNG file path |
| `center_tile_key` | Primary tile for this building |
| `tile_count` | How many tiles the building spans |
| `crop_left/top/right/bottom/width/height` | Pixel crop window |

---

## Module Responsibilities

| Module | Role |
|---|---|
| `cli.py` | Argument parsing, logging setup, orchestration |
| `config.py` | `PipelineConfig` frozen dataclass with all defaults |
| `geometry.py` | `GeometryResolver` interface + `StubGeometryResolver` (dict-backed) |
| `models.py` | `Bounds4326`, `ResolvedBuilding`, `TileRef`, `BuildingPlan`, `RunPlan`, `StorageEstimate` |
| `planner.py` | Converts buildings → tile plans, deduplicates tiles, estimates storage |
| `downloader.py` | HTTP tile fetching with retry (tenacity), on-disk cache, sequential download |
| `cropper.py` | Mosaics tiles, crops to building window, saves PNG |
| `tile_math.py` | Pure WGS84↔pixel coordinate math |
| `duckdb_store.py` | Schema init, run inserts, tile/BBL manifests, status updates |

---

## Data Flow

```
--input JSON (BBL→bounds)
    ↓
StubGeometryResolver.resolve_many()
    ↓ [list[ResolvedBuilding]]
plan_run()
    ├─ plan_building() per building → BuildingPlan (tiles + crop window)
    ├─ Deduplicate tiles (dict keyed by tile.key())
    └─ estimate_storage() → StorageEstimate
    ↓ [RunPlan]
insert_run() → run_id (UUID)
insert_tile_manifest()
insert_bbl_manifest()
    ↓
Check: peak_working_bytes < disk_cap_bytes?
    ├─ NO  → update_run_status("rejected_storage_cap"), exit 2
    └─ YES → continue
    ↓
[preflight] → update_run_status("preflight_ok"), exit 0
[run]
    ↓
TileDownloader.download_tiles()
    ├─ path.exists() check (cache hit → skip HTTP)
    └─ tenacity retry: up to 5x, exponential backoff 1–30s
    ↓ {TileRef → Path}
For each building:
    ├─ crop_building() → mosaic tiles, crop to window, save PNG
    └─ Decrement ref-count; delete tile file when ref-count hits 0
    ↓
update_run_status("completed")
```

---

## Logging

Configured in `cli.py` via `logging.basicConfig`:
```
%(asctime)s %(levelname)s %(name)s — %(message)s
```
Output goes to **stderr**.

| Component | Level | Message |
|---|---|---|
| cli | INFO | `"Starting imagery pipeline: command=X input=Y"` |
| cli | INFO | `"Planned N crops across M distinct tiles. Projected peak bytes: X."` |
| cli | INFO | `"Completed run {uuid}."` |
| cli | WARNING | `"Projected working set X bytes exceeds cap Y bytes — run rejected."` |
| downloader | DEBUG | `"Tile cache hit: {key}"` |
| downloader | DEBUG | `"Downloading tile: {key}"` |
| downloader | WARNING | `"Tile download failed (attempt N), retrying in Xs — {exception}"` |
| cropper | DEBUG | `"Cropped {bbl} → {output_path}"` |
| cropper | WARNING | `"Failed to crop BBL {bbl}"` |
| duckdb_store | DEBUG | Insert/update confirmation messages |

---

## Key Behaviors & Limitations

| Item | Detail |
|---|---|
| **No run idempotency** | Each invocation creates a new `run_id` UUID — re-running = duplicate rows in all three tables |
| **No change detection** | Pipeline does not compare crops or BBL sets across runs |
| **Tile cache is intra-run only** | `path.exists()` prevents re-fetching the same tile twice *within* one run (shared tiles across buildings). Tiles are deleted after each run (`delete_temp_tiles=True`) so run 2 re-downloads everything |
| **No `--limit` flag** | Must pre-filter the input JSON to control row count |
| **BBL/BIN field naming** | `bbl_crops.bbl` stores whatever key was used in the input JSON — if BIN is used as the key, the column name is misleading. Needs reconciliation before imagery pipeline is activated |
| **Schema file must exist on disk** | `db/imagery_schema.sql` is git-staged (`AD`) — must be restored before first run |
| **Storage cap enforced pre-download** | Preflight phase estimates peak bytes; run is rejected if cap exceeded |

---

## Input JSON Format

```json
{
  "<bbl_or_bin>": {
    "min_lon": -73.987,
    "min_lat": 40.748,
    "max_lon": -73.983,
    "max_lat": 40.751
  }
}
```

Source: NYC Building Footprints, Socrata dataset `5zhs-2jue`. Fields `mappluto_bbl` (BBL) and `bin` are both available. Bounds are computed from `the_geom` MultiPolygon coordinate extents.

---

## Outstanding Work (Before Imagery Pipeline Can Run)

1. Restore `db/imagery_schema.sql` from git index (`git restore db/imagery_schema.sql`)
2. Generate input JSON (BBL or BIN keyed) with bounds from Socrata geometry
3. Decide on BIN vs BBL as the canonical key and rename `bbl` fields in models/schema accordingly
4. Run `preflight` first to validate storage estimate before any tile downloads
