"""
Standalone retention purge for the bronze buildings dataset.

This command reclaims local disk by removing bronze raw data and clearing the
associated watermark state when the retained bronze snapshot is older than the
configured threshold.
"""

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import duckdb

from building_pipeline.config import BUILDINGS_DATASET_ID, DB_PATH
from building_pipeline import state

_BRONZE_TABLE = "bronze.buildings_raw"


@dataclass
class PurgeDecision:
    should_purge: bool
    reason: str
    last_success_at: datetime | None
    cutoff_at: datetime


def _normalize_utc(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def get_last_success_at(conn, dataset_id):
    row = conn.execute(
        """
        SELECT last_run_at
        FROM bronze.pipeline_state
        WHERE dataset_id = ? AND last_run_status = 'success'
        """,
        [dataset_id],
    ).fetchone()
    return _normalize_utc(row[0]) if row else None


def decide_purge(last_success_at, now, max_age_days, force=False):
    cutoff_at = now - timedelta(days=max_age_days)
    if force:
        return PurgeDecision(True, "forced purge", _normalize_utc(last_success_at), cutoff_at)

    normalized_last_success = _normalize_utc(last_success_at)
    if normalized_last_success is None:
        return PurgeDecision(False, "no successful bronze load exists yet", None, cutoff_at)

    if normalized_last_success > cutoff_at:
        return PurgeDecision(
            False,
            f"latest successful bronze load is newer than {max_age_days} days",
            normalized_last_success,
            cutoff_at,
        )

    return PurgeDecision(
        True,
        f"latest successful bronze load is older than {max_age_days} days",
        normalized_last_success,
        cutoff_at,
    )


def purge_bronze(conn, dataset_id):
    table_exists = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = 'buildings_raw'
        """
    ).fetchone()[0]

    deleted_rows = 0
    if table_exists:
        deleted_rows = conn.execute(f"SELECT COUNT(*) FROM {_BRONZE_TABLE}").fetchone()[0]
        conn.execute(f"DROP TABLE {_BRONZE_TABLE}")

    deleted_state_rows = conn.execute(
        """
        DELETE FROM bronze.pipeline_state
        WHERE dataset_id = ?
        RETURNING dataset_id
        """,
        [dataset_id],
    ).fetchall()

    return {
        "deleted_rows": deleted_rows,
        "deleted_state_rows": len(deleted_state_rows),
        "table_existed": bool(table_exists),
    }


def run(max_age_days=7, force=False, dry_run=False, now=None):
    now = _normalize_utc(now or datetime.now(timezone.utc))
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = duckdb.connect(DB_PATH)

    try:
        state.ensure(conn)
        last_success_at = get_last_success_at(conn, BUILDINGS_DATASET_ID)
        decision = decide_purge(last_success_at, now, max_age_days, force=force)

        print(f"Retention threshold : {max_age_days} days")
        if decision.last_success_at is not None:
            print(f"Last successful run : {decision.last_success_at.isoformat()}")
        else:
            print("Last successful run : none")

        if not decision.should_purge:
            print(f"Skipping purge: {decision.reason}.")
            return 0

        if dry_run:
            print(f"Dry-run: would purge bronze data because {decision.reason}.")
            return 0

        conn.begin()
        try:
            result = purge_bronze(conn, BUILDINGS_DATASET_ID)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        print(f"Purged bronze data because {decision.reason}.")
        print(f"Bronze table existed : {result['table_existed']}")
        print(f"Bronze rows removed  : {result['deleted_rows']}")
        print(f"State rows removed   : {result['deleted_state_rows']}")
        return 0
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Purge bronze buildings data when the retained snapshot exceeds the age threshold."
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=7,
        metavar="N",
        help="Purge if the latest successful bronze load is older than N days.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Purge immediately regardless of bronze age.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show whether a purge would happen without modifying DuckDB.",
    )
    args = parser.parse_args()

    if args.max_age_days < 0:
        parser.error("--max-age-days must be >= 0")

    raise SystemExit(
        run(max_age_days=args.max_age_days, force=args.force, dry_run=args.dry_run)
    )


if __name__ == "__main__":
    main()
