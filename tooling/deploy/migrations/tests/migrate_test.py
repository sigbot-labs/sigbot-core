import importlib.util
import pathlib
import tempfile
import unittest


class MigrationOrderTest(unittest.TestCase):
    def test_double_digit_sequences_follow_their_dependencies(self):
        module_path = pathlib.Path(__file__).parents[1] / "migrate.py"
        spec = importlib.util.spec_from_file_location("migrations", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            for sequence in (1, 10, 2, 16, 7):
                path = root / f"20261028-{sequence}" / "test.ddl.sql"
                path.parent.mkdir()
                path.write_text(f"-- step {sequence}\n")
            script = module.MigrationRunner(root).script()
            positions = [script.index(f"-- step {sequence}\n") for sequence in (1, 2, 7, 10, 16)]
            self.assertEqual(positions, sorted(positions))


if __name__ == "__main__":
    unittest.main()
