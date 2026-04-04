<!-- Generated guidance for AI coding agents working on this repository -->
# Copilot instructions for nyc_geom_proj

Purpose
- Help an AI coding agent become productive quickly in this repo (notebook-driven ETL + local venv).

Quick entry points
- Primary workbook: `nyc_building_template.ipynb` — this is the main runnable analysis/ETL.
- Local virtualenv: `nyc_geom_db/` contains a self-contained environment. Activate on Windows PowerShell with:

  & nyc_geom_db\Scripts\Activate.ps1

Observed runtime dependencies
- `polars`, `duckdb`, `sodapy`, `requests` — packages appear inside `nyc_geom_db/Lib/site-packages`.

Big-picture architecture and patterns
- Notebook-first workflow: Most logic lives in Jupyter notebooks rather than a package. Expect data exploration, transformation, and export steps across cells.
- Data processing: Polars and DuckDB are used for in-memory DataFrame and SQL-style operations. Prefer streaming transformations in Polars where available and DuckDB for heavy SQL joins/aggregations.
- External data: `sodapy` is used to fetch NYC open data (Socrata). Treat Socrata queries as the canonical external data source; cache results locally when experiments repeat.

Developer workflows (how to run / reproduce)
- Activate the repo venv (Windows PowerShell): `& nyc_geom_db\Scripts\Activate.ps1`.
- Run the notebook interactively in VS Code or JupyterLab. To run headless (re-execute all cells):

  jupyter nbconvert --to notebook --execute nyc_building_template.ipynb --inplace

- If dependencies are missing: install into the venv (after activation):

  pip install polars duckdb sodapy requests

Project-specific conventions
- Keep analysis and transformation code inside notebooks; small helper modules may be embedded as notebook cells. If extracting code to .py, mirror cell order and preserve example inputs for tests.
- Environment stored in-repo: Do not overwrite `nyc_geom_db/` unless intentionally updating the virtualenv. Prefer creating a new venv for experiments.

Integration points and important files
- `nyc_building_template.ipynb`: analysis/ETL flow — consult this first to understand data sources and outputs.
- `nyc_geom_db/`: Python virtualenv with installed packages — use this for consistent runtime.
- External: Socrata (via `sodapy`) and local DuckDB/Parquet outputs (inferred from DuckDB and Polars usage).

Examples (what to look for)
- When searching for how Socrata is queried, look for imports of `sodapy` or `requests` in notebook cells.
- For SQL-oriented steps, search for `duckdb` usage to find persistent queries or table materialization.

When editing or adding code
- Keep changes small and notebook-forward: add a new notebook cell (and update the executed notebook) rather than large monolithic scripts unless you are extracting reusable functions.
- If you add .py modules, include a short example cell in the notebook showing intended usage.

Missing documentation to request from humans
- Canonical data locations and sample credentials for Socrata (if private); preferred output formats (Parquet/CSV); any CI/test commands.

If anything here is unclear or you want me to expand sections (run commands, export requirements.txt, or convert the notebook into a script), tell me which part to iterate on.
