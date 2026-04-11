# NYC Geodata DB — Project Overview

A local ETL system that pulls publicly available NYC geodata from the [NYC Open Data](https://opendata.cityofnewyork.us/) Socrata platform and stores it in a local [DuckDB](https://duckdb.org/) database using a medallion architecture (Bronze → Silver; Gold layer not yet built).

---

## Pipelines

### 1. Building Pipeline (`building_pipeline/`)

Ingests NYC building footprints and metadata into the **bronze** layer.

- **Source**: Socrata dataset `5zhs-2jue` (`data.cityofnewyork.us`)
- **Destination**: `bronze.buildings_raw` (DuckDB, append-only)
- **Load mode**: Weekly incremental — only rows where `last_edited_date > last_run_at` are fetched. Falls back to a full load on first run or when the bronze table doesn't exist.
- **Change detection**: Compares Socrata's `rowsUpdatedAt` metadata against a stored watermark — skips the entire run if the dataset hasn't changed.
- **Geometry**: `the_geom` is stored as a GeoJSON string (`VARCHAR`); no spatial parsing at this layer.
- **State**: `bronze.pipeline_state` table tracks watermarks, run timestamps, row counts, and success/failure status.
- **Retention**: `purge_bronze.py` can drop old bronze data based on a configurable age threshold.

**Run:**
```bash
python -m building_pipeline.run_bronze           # full / incremental
python -m building_pipeline.run_bronze --limit N # test with N rows

python -m building_pipeline.purge_bronze --dry-run
python -m building_pipeline.purge_bronze --max-age-days 14
```

---

### 2. Imagery Pipeline (`imagery_pipeline/`)

Downloads and crops aerial photo tiles from NYC's public tile server for a given set of buildings (identified by BBL — Borough/Block/Lot).

- **Source**: `https://maps.nyc.gov/xyz/1.0.0/photo/{year}/{z}/{x}/{y}.png8` (public, no auth)
- **Destination**: PNG files under `data/imagery/crops/{bbl}.png`
- **Input**: A JSON file mapping BBL → WGS84 bounding box (must be pre-generated separately)
- **Default imagery year**: 2018, zoom level 19
- **Storage cap**: Pre-flight rejects runs exceeding 5 GB (configurable)
- **Tile caching**: Tiles are cached to `data/imagery/tmp_tiles/` within a run and deleted after use

**Workflow:**
1. Resolve BBL bounds from input JSON
2. Plan: compute required tiles + crop windows; deduplicate tiles; estimate storage
3. Insert run record, tile manifest, and BBL manifest into DuckDB (`imagery.*` tables)
4. Storage cap check — reject if peak bytes exceed cap
5. Download tiles (HTTP with exponential-backoff retry, max 5 attempts)
6. For each building: mosaic tiles → crop to bounds → save PNG
7. Update run status to `completed`

**Run:**
```bash
python -m imagery_pipeline.cli preflight --input buildings.json   # dry-run, no download
python -m imagery_pipeline.cli run       --input buildings.json   # download + crop
```

**Key flags:** `--db-path`, `--year`, `--zoom`, `--disk-cap-bytes`, `--temp-tile-dir`, `--output-dir`, `--log-level`

> **Note**: The imagery pipeline is not yet fully operational. `db/imagery_schema.sql` is missing from the repo and must be restored before the pipeline can run. The geometry resolver is a stub that only accepts pre-computed bounds.

---

### 3. Subgrade Pipeline (`subgrade_pipeline/`)

One-time full load of NYC subgrade elevation data into the **silver** layer with geometry parsing and type casting.

- **Source**: Socrata dataset `bsin-59hv` (`data.cityofnewyork.us`)
- **Destination**: `silver.subgrade` (DuckDB)
- **Geometry**: `the_geom` is parsed with `ST_GeomFromGeoJSON()` → stored as DuckDB `GEOMETRY` type (requires spatial extension)
- **Numeric casting**: `z_grade`, `z_floor`, `latitude`, `longitude` cast to `DOUBLE` via `TRY_CAST`
- **Idempotent**: drops and recreates `silver.subgrade` on each run
- **Completeness check**: validates that ingested row count matches Socrata total before committing

**Run:**
```bash
python -m subgrade_pipeline.run_silver           # full load
python -m subgrade_pipeline.run_silver --limit N # test with N rows
```

---

## Quick Start

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt  # includes pytest, ipykernel

# 3. Create .env with your Socrata credentials
cat > .env << 'EOF'
SOCRATA_APP_TOKEN=your_app_token_here
SOCRATA_USERNAME=your_username_here
SOCRATA_PASSWORD=your_password_here
EOF

# 4. Run the building pipeline
python -m building_pipeline.run_bronze --limit 500

# 5. Run the subgrade pipeline
python -m subgrade_pipeline.run_silver --limit 500

# 6. Run tests
pytest tests/
```

---

## Configuration

All secrets are loaded from a `.env` file at the project root (never committed to git).

| Variable | Required by | Purpose |
|----------|-------------|---------|
| `SOCRATA_APP_TOKEN` | Both Socrata pipelines | Increases API rate limits |
| `SOCRATA_USERNAME` | Both Socrata pipelines | Socrata account authentication |
| `SOCRATA_PASSWORD` | Both Socrata pipelines | Socrata account authentication |

The `.env` file is listed in `.gitignore` and must never be committed.

---

## Database Layout

All data lives in a single local DuckDB file at `db/nyc_buildings.duckdb`.

### `bronze` schema — Raw ingest layer

| Table | Description |
|-------|-------------|
| `bronze.buildings_raw` | Append-only raw rows from Socrata. All columns `VARCHAR`; geometry stored as GeoJSON string. Includes `run_id`, `ingested_at`, `source_dataset_id` metadata columns. |
| `bronze.pipeline_state` | One row per dataset tracking watermarks: `last_run_at`, `dataset_updated_at`, `last_run_status`, `rows_ingested`. |

### `silver` schema — Cleaned, typed layer

| Table | Description |
|-------|-------------|
| `silver.subgrade` | Subgrade elevation data. `the_geom` is a parsed `GEOMETRY` type. Numeric fields (`z_grade`, `z_floor`, `latitude`, `longitude`) are `DOUBLE`. `bin` is the primary key. |

### `imagery` schema — Imagery pipeline tracking

> Requires `db/imagery_schema.sql` to be present (currently missing).

| Table | Description |
|-------|-------------|
| `imagery.pipeline_runs` | One row per CLI invocation. Tracks mode, status, building count, tile count, storage estimates. |
| `imagery.tile_manifest` | One row per distinct tile per run. Tracks tile coordinates and planned download URL. |
| `imagery.bbl_crops` | One row per building per run. Tracks output PNG path, crop window coordinates, and tile count. |

---

## Project Structure

```
nyc_geom_db_proto/
├── building_pipeline/      # Buildings bronze ETL
│   ├── config.py           # Socrata connection settings, DB path
│   ├── extract.py          # Paginated Socrata fetch, geometry serialization
│   ├── state.py            # DuckDB-backed watermark management
│   ├── bronze.py           # Append-only bronze layer loader
│   ├── run_bronze.py       # Entry point: full/incremental ingest
│   └── purge_bronze.py     # Retention policy enforcement
├── imagery_pipeline/       # Aerial imagery crop pipeline
│   ├── config.py           # Tile URL, defaults, PipelineConfig dataclass
│   ├── models.py           # Frozen dataclasses (Bounds, TileRef, BuildingPlan, etc.)
│   ├── geometry.py         # BBL→bounds resolver interface + stub
│   ├── tile_math.py        # WGS84↔web mercator pixel math (pure functions)
│   ├── planner.py          # Tile planning, storage estimation, run planning
│   ├── downloader.py       # HTTP tile fetch with tenacity retry
│   ├── cropper.py          # Tile mosaic + PNG crop via Pillow
│   ├── duckdb_store.py     # Schema setup, run/tile/BBL manifest inserts
│   └── cli.py              # CLI entry point (preflight / run commands)
├── subgrade_pipeline/      # Subgrade elevation silver ETL
│   ├── config.py           # Dataset ID, column list
│   └── run_silver.py       # One-time full load into silver.subgrade
├── docs/
│   ├── PROJECT.md          # This file
│   └── imagery_pipeline_notes.md  # Detailed imagery pipeline design notes
├── tests/                  # pytest test suite
├── db/                     # DuckDB database files (gitignored)
├── data/imagery/           # Downloaded tiles + crops (gitignored)
├── requirements.txt        # Runtime dependencies
├── requirements-dev.txt    # Dev/test dependencies
└── .env                    # Secrets (gitignored — never commit)
```

---

## Outstanding Work

| Item | Pipeline | Notes |
|------|----------|-------|
| `db/imagery_schema.sql` missing | Imagery | Pipeline fails at startup without this file. Needs to be restored or recreated. |
| Real geometry resolver | Imagery | `StubGeometryResolver` only does dict-lookups. A real resolver should query `bronze.buildings_raw` or `silver.*` to derive BBL bounds. |
| Input JSON generation | Imagery | No script exists yet to generate the BBL→bounds JSON required by the CLI. |
| Imagery pipeline idempotency | Imagery | Re-running on the same input creates duplicate rows in all three `imagery.*` tables. |
| Gold layer | All | No aggregated/analytics layer exists yet. |
| Incremental subgrade loads | Subgrade | Currently drops and recreates the table on every run; no change detection. |
