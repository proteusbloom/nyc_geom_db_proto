import unittest
from pathlib import Path

from imagery_pipeline.config import PipelineConfig
from imagery_pipeline.models import Bounds4326, ResolvedBuilding
from imagery_pipeline.planner import plan_run


class PlannerTests(unittest.TestCase):
    def test_run_plan_deduplicates_distinct_tiles(self) -> None:
        config = PipelineConfig(output_dir=Path("test_outputs"))
        buildings = [
            ResolvedBuilding(
                bbl="1000000001",
                bounds=Bounds4326(-73.98575, 40.74840, -73.98570, 40.74845),
            ),
            ResolvedBuilding(
                bbl="1000000002",
                bounds=Bounds4326(-73.98574, 40.74841, -73.98569, 40.74846),
            ),
        ]
        plan = plan_run(buildings, config)
        self.assertEqual(len(plan.distinct_tiles), 1)
        self.assertTrue(plan.estimate.within_cap)

    def test_storage_cap_rejection_is_detected_in_planning(self) -> None:
        config = PipelineConfig(disk_cap_bytes=1, output_dir=Path("test_outputs"))
        buildings = [
            ResolvedBuilding(
                bbl="1000000001",
                bounds=Bounds4326(-73.98575, 40.74840, -73.98570, 40.74845),
            )
        ]
        plan = plan_run(buildings, config)
        self.assertFalse(plan.estimate.within_cap)


if __name__ == "__main__":
    unittest.main()
