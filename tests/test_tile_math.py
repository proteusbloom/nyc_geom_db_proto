import unittest

from imagery_pipeline.models import Bounds4326
from imagery_pipeline.tile_math import bounds_to_pixel_window, required_tiles_for_window


class TileMathTests(unittest.TestCase):
    def test_bounds_that_fit_in_one_tile_only_require_one_tile(self) -> None:
        bounds = Bounds4326(
            min_lon=-73.98575,
            min_lat=40.74840,
            max_lon=-73.98570,
            max_lat=40.74845,
        )
        window = bounds_to_pixel_window(bounds, zoom=19)
        tiles = required_tiles_for_window(window, year=2018, zoom=19)
        self.assertEqual(len(tiles), 1)

    def test_bounds_that_cross_tile_boundary_require_neighbors(self) -> None:
        bounds = Bounds4326(
            min_lon=-74.00010,
            min_lat=40.70060,
            max_lon=-73.99930,
            max_lat=40.70120,
        )
        window = bounds_to_pixel_window(bounds, zoom=19)
        tiles = required_tiles_for_window(window, year=2018, zoom=19)
        self.assertGreaterEqual(len(tiles), 2)


if __name__ == "__main__":
    unittest.main()
