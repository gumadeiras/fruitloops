from __future__ import annotations

import csv
import json
import sys
from typing import Iterable, Sequence


def emit_rows(rows: Iterable[dict[str, str]], columns: Sequence[str], fmt: str) -> None:
    materialized = list(rows)
    if fmt == "json":
        print(json.dumps(materialized, indent=2))
    elif fmt == "jsonl":
        for row in materialized:
            print(json.dumps(row, separators=(",", ":")))
    elif fmt == "csv":
        writer = csv.DictWriter(sys.stdout, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized)
    else:
        print_table(materialized, columns)


def print_table(rows: Sequence[dict[str, str]], columns: Sequence[str]) -> None:
    if not columns:
        return
    widths = {column: len(column) for column in columns}
    for row in rows:
        for column in columns:
            widths[column] = max(widths[column], len(str(row.get(column, ""))))
    header = "  ".join(column.ljust(widths[column]) for column in columns)
    rule = "  ".join("-" * widths[column] for column in columns)
    print(header)
    print(rule)
    for row in rows:
        print("  ".join(str(row.get(column, "")).ljust(widths[column]) for column in columns))


def parse_columns(select: str | None, fallback: Sequence[str]) -> list[str]:
    if not select:
        return list(fallback)
    return [item.strip() for item in select.split(",") if item.strip()]
