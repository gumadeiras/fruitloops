from __future__ import annotations

import argparse
from pathlib import Path

from .bulk import DEFAULT_BULK_DIR, DEFAULT_DUCKDB_PATH, setup_practical_bulk
from .cache import DEFAULT_CACHE_DIR
from .cli_helpers import add_dataset_filter_arg, add_format_arg, unique_values
from .data import FruitloopsData
from .formatting import emit_rows
from .olfaction import build_olfaction_cache
from .olfaction_live import cache_olfaction_annotations


def add_setup_parser(subparsers) -> None:
    setup = subparsers.add_parser("setup", help="Download and build local offline data stores.")
    setup.add_argument("--bulk-dir", type=Path, default=DEFAULT_BULK_DIR)
    setup.add_argument("--store", type=Path, default=DEFAULT_DUCKDB_PATH)
    setup.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    add_dataset_filter_arg(setup, ("hemibrain", "flywire"))
    setup.add_argument("--replace", action=argparse.BooleanOptionalAction, default=True)
    setup.add_argument(
        "--cache-annotations",
        action="store_true",
        help="Also fetch live olfaction annotations into DuckDB; requires credentials.",
    )
    setup.add_argument("--chunk-size", type=int, default=2000)
    add_format_arg(setup)
    setup.set_defaults(func=cmd_setup)


def cmd_setup(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    datasets = unique_values(args.dataset)
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        setup_status_row(
            dataset="all",
            action="cache",
            target="live_cache",
            status="ready",
            path=args.cache_dir,
            store=args.store,
        )
    ]
    bulk_rows = setup_practical_bulk(
        bulk_dir=args.bulk_dir,
        store=args.store,
        datasets=datasets,
        replace=args.replace,
    )
    rows.extend(normalize_bulk_setup_rows(bulk_rows))
    olfaction_rows = build_olfaction_cache(
        store=args.store,
        datasets=datasets,
        replace=args.replace,
    )
    rows.extend(normalize_olfaction_setup_rows(olfaction_rows, action="olfaction-build"))
    if args.cache_annotations:
        annotation_rows = cache_olfaction_annotations(
            store=args.store,
            datasets=datasets,
            chunk_size=args.chunk_size,
            rebuild=False,
        )
        rows.extend(normalize_olfaction_setup_rows(annotation_rows, action="annotation-cache"))
        rebuilt_rows = build_olfaction_cache(
            store=args.store,
            datasets=datasets,
            replace=True,
        )
        rows.extend(normalize_olfaction_setup_rows(rebuilt_rows, action="olfaction-rebuild"))
    emit_rows(rows, ["dataset", "action", "target", "status", "path", "store"], args.format)
    return 0


def normalize_bulk_setup_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        setup_status_row(
            dataset=row["dataset"],
            action=row["action"],
            target=row["target"],
            status=row["status"],
            path=row["path"],
            store=row["store"],
        )
        for row in rows
    ]


def normalize_olfaction_setup_rows(rows: list[dict[str, str]], action: str) -> list[dict[str, str]]:
    return [
        setup_status_row(
            dataset=row["dataset"],
            action=action,
            target=row["table"],
            status=f"{row['status']}:{row['rows']}",
            path="",
            store=row["store"],
        )
        for row in rows
    ]


def setup_status_row(
    *,
    dataset: str,
    action: str,
    target: str,
    status: str,
    path: Path | str,
    store: Path | str,
) -> dict[str, str]:
    return {
        "dataset": dataset,
        "action": action,
        "target": target,
        "status": status,
        "path": str(path),
        "store": str(store),
    }
