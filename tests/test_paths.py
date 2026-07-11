from pathlib import Path
import os
import tempfile
import unittest
from unittest.mock import patch

from fruitloops.paths import (
    default_bulk_dir,
    default_duckdb_path,
    fruitloops_cache_home,
    fruitloops_data_home,
)


class PathTests(unittest.TestCase):
    def test_macos_uses_application_support_and_caches(self) -> None:
        home = Path("/Users/gustavo")
        self.assertEqual(
            fruitloops_data_home(environ={}, platform="darwin", home=home),
            home / "Library" / "Application Support" / "fruitloops",
        )
        self.assertEqual(
            fruitloops_cache_home(environ={}, platform="darwin", home=home),
            home / "Library" / "Caches" / "fruitloops",
        )

    def test_linux_uses_xdg_roots(self) -> None:
        home = Path("/home/gustavo")
        self.assertEqual(
            fruitloops_data_home(
                environ={"XDG_DATA_HOME": "/var/data"}, platform="linux", home=home
            ),
            Path("/var/data/fruitloops"),
        )
        self.assertEqual(
            fruitloops_cache_home(
                environ={"XDG_CACHE_HOME": "/var/cache"}, platform="linux", home=home
            ),
            Path("/var/cache/fruitloops"),
        )

    def test_windows_uses_local_app_data(self) -> None:
        environ = {"LOCALAPPDATA": r"C:\Users\gustavo\AppData\Local"}
        home = Path(r"C:\Users\gustavo")
        root = Path(environ["LOCALAPPDATA"]) / "fruitloops"
        self.assertEqual(
            fruitloops_data_home(environ=environ, platform="win32", home=home), root
        )
        self.assertEqual(
            fruitloops_cache_home(environ=environ, platform="win32", home=home),
            root / "Cache",
        )

    def test_relative_xdg_roots_are_ignored(self) -> None:
        home = Path("/home/gustavo")
        self.assertEqual(
            fruitloops_data_home(
                environ={"XDG_DATA_HOME": "relative"}, platform="linux", home=home
            ),
            home / ".local" / "share" / "fruitloops",
        )
        self.assertEqual(
            fruitloops_cache_home(
                environ={"XDG_CACHE_HOME": "relative"}, platform="linux", home=home
            ),
            home / ".cache" / "fruitloops",
        )

    def test_bulk_and_duckdb_defaults_use_persistent_data_home(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            os.environ, {"FRUITLOOPS_HOME": tmp}, clear=False
        ):
            root = Path(tmp).resolve()
            self.assertEqual(default_bulk_dir(), root / "bulk")
            self.assertEqual(default_duckdb_path(), root / "bulk" / "fruitloops.duckdb")


if __name__ == "__main__":
    unittest.main()
