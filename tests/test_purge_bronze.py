import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import duckdb

from building_pipeline import state
from building_pipeline.purge_bronze import decide_purge, purge_bronze, run


class PurgeDecisionTests(unittest.TestCase):
    def test_no_successful_run_skips_purge(self) -> None:
        now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
        decision = decide_purge(None, now, max_age_days=7)
        self.assertFalse(decision.should_purge)
        self.assertEqual(decision.reason, "no successful bronze load exists yet")

    def test_recent_success_skips_purge(self) -> None:
        now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
        last_success_at = now - timedelta(days=3)
        decision = decide_purge(last_success_at, now, max_age_days=7)
        self.assertFalse(decision.should_purge)
        self.assertIn("newer than 7 days", decision.reason)

    def test_old_success_triggers_purge(self) -> None:
        now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
        last_success_at = now - timedelta(days=8)
        decision = decide_purge(last_success_at, now, max_age_days=7)
        self.assertTrue(decision.should_purge)
        self.assertIn("older than 7 days", decision.reason)

    def test_force_overrides_missing_success(self) -> None:
        now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
        decision = decide_purge(None, now, max_age_days=7, force=True)
        self.assertTrue(decision.should_purge)
        self.assertEqual(decision.reason, "forced purge")


class PurgeBronzeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test.duckdb")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _connect(self):
        return duckdb.connect(self.db_path)

    def _seed_tables(self, *, success_age_days=8, include_bronze_table=True, status="success") -> None:
        conn = self._connect()
        try:
            state.ensure(conn)
            if include_bronze_table:
                conn.execute(
                    """
                    CREATE TABLE bronze.buildings_raw (
                        bin VARCHAR,
                        the_geom VARCHAR,
                        run_id VARCHAR,
                        ingested_at TIMESTAMP,
                        source_dataset_id VARCHAR
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO bronze.buildings_raw VALUES
                    ('1001', '{}', 'run-1', ?, '5zhs-2jue'),
                    ('1002', '{}', 'run-1', ?, '5zhs-2jue')
                    """,
                    [
                        datetime(2026, 4, 1, tzinfo=timezone.utc),
                        datetime(2026, 4, 1, tzinfo=timezone.utc),
                    ],
                )

            state.set_watermark(
                conn,
                "5zhs-2jue",
                datetime.now(timezone.utc) - timedelta(days=success_age_days),
                1234567890,
                status,
                2,
            )
        finally:
            conn.close()

    def test_purge_bronze_removes_table_and_state_row(self) -> None:
        self._seed_tables()

        conn = self._connect()
        try:
            result = purge_bronze(conn, "5zhs-2jue")
            remaining_tables = conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'bronze' AND table_name = 'buildings_raw'
                """
            ).fetchone()[0]
            remaining_state_rows = conn.execute(
                "SELECT COUNT(*) FROM bronze.pipeline_state WHERE dataset_id = '5zhs-2jue'"
            ).fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(result["deleted_rows"], 2)
        self.assertEqual(result["deleted_state_rows"], 1)
        self.assertTrue(result["table_existed"])
        self.assertEqual(remaining_tables, 0)
        self.assertEqual(remaining_state_rows, 0)

    def test_run_dry_run_preserves_database(self) -> None:
        self._seed_tables()
        fixed_now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)

        with mock.patch("building_pipeline.purge_bronze.DB_PATH", self.db_path):
            exit_code = run(max_age_days=7, dry_run=True, now=fixed_now)

        conn = self._connect()
        try:
            bronze_rows = conn.execute("SELECT COUNT(*) FROM bronze.buildings_raw").fetchone()[0]
            state_rows = conn.execute(
                "SELECT COUNT(*) FROM bronze.pipeline_state WHERE dataset_id = '5zhs-2jue'"
            ).fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(exit_code, 0)
        self.assertEqual(bronze_rows, 2)
        self.assertEqual(state_rows, 1)

    def test_run_purges_old_bronze_data(self) -> None:
        self._seed_tables(success_age_days=10)
        fixed_now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)

        with mock.patch("building_pipeline.purge_bronze.DB_PATH", self.db_path):
            exit_code = run(max_age_days=7, now=fixed_now)

        conn = self._connect()
        try:
            bronze_table_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'bronze' AND table_name = 'buildings_raw'
                """
            ).fetchone()[0]
            state_rows = conn.execute(
                "SELECT COUNT(*) FROM bronze.pipeline_state WHERE dataset_id = '5zhs-2jue'"
            ).fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(exit_code, 0)
        self.assertEqual(bronze_table_count, 0)
        self.assertEqual(state_rows, 0)

    def test_run_skips_recent_bronze_data(self) -> None:
        self._seed_tables(success_age_days=2)
        fixed_now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)

        with mock.patch("building_pipeline.purge_bronze.DB_PATH", self.db_path):
            exit_code = run(max_age_days=7, now=fixed_now)

        conn = self._connect()
        try:
            bronze_rows = conn.execute("SELECT COUNT(*) FROM bronze.buildings_raw").fetchone()[0]
            state_rows = conn.execute(
                "SELECT COUNT(*) FROM bronze.pipeline_state WHERE dataset_id = '5zhs-2jue'"
            ).fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(exit_code, 0)
        self.assertEqual(bronze_rows, 2)
        self.assertEqual(state_rows, 1)

    def test_run_skips_when_only_failed_state_exists(self) -> None:
        self._seed_tables(success_age_days=10, status="failed")
        fixed_now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)

        with mock.patch("building_pipeline.purge_bronze.DB_PATH", self.db_path):
            exit_code = run(max_age_days=7, now=fixed_now)

        conn = self._connect()
        try:
            bronze_rows = conn.execute("SELECT COUNT(*) FROM bronze.buildings_raw").fetchone()[0]
            state_rows = conn.execute(
                "SELECT COUNT(*) FROM bronze.pipeline_state WHERE dataset_id = '5zhs-2jue'"
            ).fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(exit_code, 0)
        self.assertEqual(bronze_rows, 2)
        self.assertEqual(state_rows, 1)


if __name__ == "__main__":
    unittest.main()
