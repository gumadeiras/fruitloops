from __future__ import annotations

import os
import sys
from pathlib import Path


APP_NAME = "fruitloops"


def env_path(name: str) -> Path | None:
    value = os.environ.get(name)
    if not value:
        return None
    return Path(value).expanduser().resolve()


def package_root() -> Path:
    return Path(__file__).resolve().parents[1]


def fruitloops_data_home() -> Path:
    configured = env_path("FRUITLOOPS_HOME")
    if configured:
        return configured
    xdg_home = env_path("XDG_DATA_HOME")
    if xdg_home:
        return xdg_home / APP_NAME
    return Path.home() / ".local" / "share" / APP_NAME


def fruitloops_cache_home() -> Path:
    xdg_home = env_path("XDG_CACHE_HOME")
    if xdg_home:
        return xdg_home / APP_NAME
    return Path.home() / ".cache" / APP_NAME


def default_data_dir() -> Path:
    configured = env_path("FRUITLOOPS_DATA_DIR")
    if configured:
        return configured

    candidates = [
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

    source_bulk = package_root() / "bulk"
    if source_bulk.exists():
        return source_bulk
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
