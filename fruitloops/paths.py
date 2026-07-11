from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from pathlib import Path, PureWindowsPath


APP_NAME = "fruitloops"


def env_path(name: str, environ: Mapping[str, str] | None = None) -> Path | None:
    environ = os.environ if environ is None else environ
    value = environ.get(name)
    if not value:
        return None
    return Path(value).expanduser().resolve()


def _env_root(environ: Mapping[str, str], name: str, *, windows: bool = False) -> Path | None:
    value = environ.get(name)
    if not value:
        return None
    if windows:
        return Path(value) if PureWindowsPath(value).is_absolute() else None
    path = Path(value).expanduser()
    return path if path.is_absolute() else None


def package_root() -> Path:
    return Path(__file__).resolve().parents[1]


def fruitloops_data_home(
    environ: Mapping[str, str] | None = None,
    platform: str | None = None,
    home: Path | None = None,
) -> Path:
    environ = os.environ if environ is None else environ
    platform = sys.platform if platform is None else platform
    home = Path.home() if home is None else home
    configured = env_path("FRUITLOOPS_HOME", environ)
    if configured:
        return configured
    if platform == "darwin":
        return home / "Library" / "Application Support" / APP_NAME
    if platform == "win32":
        for name in ("LOCALAPPDATA", "APPDATA"):
            if root := _env_root(environ, name, windows=True):
                return root / APP_NAME
        return home / "AppData" / "Local" / APP_NAME
    if root := _env_root(environ, "XDG_DATA_HOME"):
        return root / APP_NAME
    return home / ".local" / "share" / APP_NAME


def fruitloops_cache_home(
    environ: Mapping[str, str] | None = None,
    platform: str | None = None,
    home: Path | None = None,
) -> Path:
    environ = os.environ if environ is None else environ
    platform = sys.platform if platform is None else platform
    home = Path.home() if home is None else home
    if platform == "darwin":
        return home / "Library" / "Caches" / APP_NAME
    if platform == "win32":
        for name in ("LOCALAPPDATA", "APPDATA"):
            if root := _env_root(environ, name, windows=True):
                return root / APP_NAME / "Cache"
        return home / "AppData" / "Local" / APP_NAME / "Cache"
    if root := _env_root(environ, "XDG_CACHE_HOME"):
        return root / APP_NAME
    return home / ".cache" / APP_NAME


def default_data_dir() -> Path:
    configured = env_path("FRUITLOOPS_DATA_DIR")
    if configured:
        return configured

    candidates = [
        Path(__file__).resolve().parent / "data",
        package_root() / "data",
        Path(sys.prefix) / "share" / APP_NAME / "data",
        fruitloops_data_home() / "data",
    ]
    for candidate in candidates:
        if (candidate / "manifest.csv").exists():
            return candidate
    return fruitloops_data_home() / "data"


def default_bulk_dir() -> Path:
    configured = env_path("FRUITLOOPS_BULK_DIR")
    if configured:
        return configured

    return fruitloops_data_home() / "bulk"


def default_duckdb_path() -> Path:
    configured = env_path("FRUITLOOPS_DUCKDB_PATH")
    if configured:
        return configured
    return default_bulk_dir() / "fruitloops.duckdb"


def default_live_cache_dir() -> Path:
    configured = env_path("FRUITLOOPS_CACHE_DIR")
    if configured:
        return configured
    return fruitloops_cache_home() / "live"
