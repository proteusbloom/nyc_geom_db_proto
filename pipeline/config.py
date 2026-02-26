import os

from dotenv import load_dotenv

load_dotenv()  # reads .env from the project root if present

SOCRATA_DOMAIN = "data.cityofnewyork.us"
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_API_KEY")
SOCRATA_TIMEOUT = 100

BUILDINGS_DATASET_ID = "5zhs-2jue"

DB_PATH = "db/nyc_buildings.duckdb"

# Socrata hard ceiling per request
PAGE_SIZE = 50_000
