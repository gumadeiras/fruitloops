from __future__ import annotations

import argparse
from pathlib import Path

from .bulk import DEFAULT_BULK_DIR, DEFAULT_DUCKDB_PATH, table_summary
from .cache import DEFAULT_CACHE_DIR, list_cache
from .cli_helpers import add_format_arg
from .data import FruitloopsData, default_data_dir
from .formatting import emit_rows
from .olfaction import olfaction_tables


def add_status_parser(subparsers) -> None:
    status = subparsers.add_parser("status", help="Show local data, cache, and storage status.")
    status.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    status.add_argument("--store", type=Path, default=DEFAULT_DUCKDB_PATH)
    status.add_argument("--details", action="store_true", help="Include offline cache and DuckDB table rows.")
    add_format_arg(status)
    status.set_defaults(func=cmd_status)


def add_locations_parser(subparsers) -> None:
    locations = subparsers.add_parser("locations", help=argparse.SUPPRESS)
    add_format_arg(locations)
    locations.set_defaults(func=cmd_locations)


def cmd_status(args: argparse.Namespace, data: FruitloopsData) -> int:
    rows = status_overview_rows(data, args.cache_dir, args.store)
    if args.details:
        rows.extend(status_detail_rows(args.cache_dir, args.store))
    emit_rows(rows, ["section", "name", "value", "path", "exists"], args.format)
    return 0


def status_overview_rows(data: FruitloopsData, cache_dir: Path, store: Path) -> list[dict[str, str]]:
    rows = [
        status_row("location", "data_dir", "configured", data.data_dir),
        status_row("location", "bulk_dir", "configured", DEFAULT_BULK_DIR),
        status_row("location", "duckdb", "configured", store),
        status_row("location", "live_cache", f"{len(list_cache(cache_dir))} entries", cache_dir),
    ]
    rows.extend(
        {
            "section": "dataset",
            "name": dataset,
            "value": f"{count} tables",
            "path": str(data.data_dir),
            "exists": str(data.data_dir.exists()).lower(),
        }
        for dataset, count in data.datasets().items()
    )
    return rows


def status_detail_rows(cache_dir: Path, store: Path) -> list[dict[str, str]]:
    rows = [
        {
            "section": "offline_cache",
            "name": f"{entry.dataset}:{entry.action}",
            "value": f"{entry.rows} rows",
            "path": str(entry.path),
            "exists": str(entry.path.exists()).lower(),
        }
        for entry in list_cache(cache_dir)
    ]
    rows.extend(duckdb_status_rows("bulk_table", store, table_summary))
    rows.extend(duckdb_status_rows("olfaction_table", store, olfaction_tables))
    return rows


def duckdb_status_rows(section: str, store: Path, loader) -> list[dict[str, str]]:
    try:
        table_rows = loader(store)
    except SystemExit as error:
        return [status_row(section, "unavailable", str(error), store)]
    return [
        status_row(section, row["table"], f"{row['rows']} rows", Path(row["store"]))
        for row in table_rows
    ]


def status_row(section: str, name: str, value: str, path: Path) -> dict[str, str]:
    return {
        "section": section,
        "name": name,
        "value": value,
        "path": str(path),
        "exists": str(path.exists()).lower(),
    }


def cmd_locations(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    rows = [
        location_row("data_dir", default_data_dir(), "FRUITLOOPS_DATA_DIR"),
        location_row("bulk_dir", DEFAULT_BULK_DIR, "FRUITLOOPS_BULK_DIR"),
        location_row("duckdb", DEFAULT_DUCKDB_PATH, "FRUITLOOPS_DUCKDB_PATH"),
        location_row("live_cache", DEFAULT_CACHE_DIR, "FRUITLOOPS_CACHE_DIR"),
    ]
    emit_rows(rows, ["name", "path", "exists", "env"], args.format)
    return 0


def location_row(name: str, path: Path, env: str) -> dict[str, str]:
    return {
        "name": name,
        "path": str(path),
        "exists": str(path.exists()).lower(),
        "env": env,
    }
