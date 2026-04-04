import tempfile
import unittest
from pathlib import Path

from PIL import Image

from imagery_pipeline.cropper import crop_building
from imagery_pipeline.models import Bounds4326, BuildingPlan, CropWindow, TileRef


class CropperTests(unittest.TestCase):
    def test_cropper_stitches_neighbor_tiles_for_boundary_crossing_crop(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            left_tile = root / "left.png"
            right_tile = root / "right.png"
            output = root / "crop.png"

            Image.new("RGBA", (256, 256), (255, 0, 0, 255)).save(left_tile)
            Image.new("RGBA", (256, 256), (0, 255, 0, 255)).save(right_tile)

            tile_a = TileRef(year=2018, z=19, x=1, y=2)
            tile_b = TileRef(year=2018, z=19, x=2, y=2)
            plan = BuildingPlan(
                bbl="1",
                bounds=Bounds4326(-74.0, 40.7, -73.9, 40.8),
                center_tile=tile_a,
                tiles=(tile_a, tile_b),
                crop_window=CropWindow(left=250, top=10, right=262, bottom=20),
                output_path=output,
            )

            crop_building(plan, {tile_a: left_tile, tile_b: right_tile})
            result = Image.open(output)
            self.assertEqual(result.size, (12, 10))
            self.assertEqual(result.getpixel((0, 0)), (255, 0, 0, 255))
            self.assertEqual(result.getpixel((11, 0)), (0, 255, 0, 255))


if __name__ == "__main__":
    unittest.main()
