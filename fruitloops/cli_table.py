from __future__ import annotations

import argparse

from .aggregate import aggregate_rows
from .cli_helpers import add_dataset_arg, add_format_arg
from .data import FruitloopsData
from .filters import matches, parse_filters, project, split_csv
from .formatting import emit_rows, parse_columns


def add_table_parser(subparsers) -> None:
    table = subparsers.add_parser("table", help="List, inspect, filter, or aggregate CSV snapshot tables.")
    table.add_argument("table_ref", nargs="?", help="Table reference or file_id.")
    table.add_argument("--table", dest="table", help="Table reference or file_id.")
    add_dataset_arg(table, ("hemibrain", "flywire", "comparison"))
    table.add_argument("--list", action="store_true", help="List tables from the manifest.")
    table.add_argument("--schema", action="store_true", help="Show table columns.")
    table.add_argument("--path", action="store_true", help="Print the CSV path.")
    table.add_argument("--head", action="store_true", help="Show first rows instead of filtering.")
    table.add_argument("--where", action="append", default=[], help="Exact filter: column=value.")
    table.add_argument("--contains", action="append", default=[], help="Substring filter: column=text.")
    table.add_argument("--select")
    table.add_argument("--by", help="Comma-separated group columns.")
    table.add_argument("--sum", dest="sum_columns", default="", help="Comma-separated numeric columns to sum.")
    table.add_argument("--mean", dest="mean_columns", default="", help="Comma-separated numeric columns to average.")
    table.add_argument("--limit", type=int)
    add_format_arg(table)
    table.set_defaults(func=cmd_table)


def add_legacy_table_parsers(subparsers) -> None:
    files = subparsers.add_parser("files", help=argparse.SUPPRESS)
    add_dataset_arg(files, ("hemibrain", "flywire", "comparison"))
    files.add_argument("--contains", help="Case-insensitive filter on path/id.")
    add_format_arg(files)
    files.set_defaults(func=cmd_files)

    schema = subparsers.add_parser("schema", help=argparse.SUPPRESS)
    schema.add_argument("--table", required=True)
    add_format_arg(schema)
    schema.set_defaults(func=cmd_schema)

    path = subparsers.add_parser("path", help=argparse.SUPPRESS)
    path.add_argument("--table", required=True)
    path.set_defaults(func=cmd_path)

    head = subparsers.add_parser("head", help=argparse.SUPPRESS)
    head.add_argument("--table", required=True)
    head.add_argument("--limit", type=int, default=10)
    head.add_argument("--select")
    add_format_arg(head)
    head.set_defaults(func=cmd_head)

    query = subparsers.add_parser("query", help=argparse.SUPPRESS)
    query.add_argument("--table", required=True)
    query.add_argument("--where", action="append", default=[], help="Exact filter: column=value.")
    query.add_argument("--contains", action="append", default=[], help="Substring filter: column=text.")
    query.add_argument("--select")
    query.add_argument("--limit", type=int, default=100)
    add_format_arg(query)
    query.set_defaults(func=cmd_query)

    aggregate = subparsers.add_parser("aggregate", help=argparse.SUPPRESS)
    aggregate.add_argument("--table", required=True)
    aggregate.add_argument("--by", required=True, help="Comma-separated group columns.")
    aggregate.add_argument("--sum", dest="sum_columns", default="", help="Comma-separated numeric columns to sum.")
    aggregate.add_argument("--mean", dest="mean_columns", default="", help="Comma-separated numeric columns to average.")
    aggregate.add_argument("--where", action="append", default=[], help="Exact filter: column=value.")
    aggregate.add_argument("--contains", action="append", default=[], help="Substring filter: column=text.")
    aggregate.add_argument("--limit", type=int, default=100)
    add_format_arg(aggregate)
    aggregate.set_defaults(func=cmd_aggregate)


def cmd_files(args: argparse.Namespace, data: FruitloopsData) -> int:
    return emit_manifest_tables(data, args.dataset, args.contains, args.format)


def cmd_schema(args: argparse.Namespace, data: FruitloopsData) -> int:
    return emit_table_schema(data, args.table, args.format)


def cmd_path(args: argparse.Namespace, data: FruitloopsData) -> int:
    table = data.resolve(args.table)
    print(data.table_path(table))
    return 0


def cmd_table(args: argparse.Namespace, data: FruitloopsData) -> int:
    table_ref = args.table_ref or args.table
    needs_table = args.schema or args.path or args.head or args.by or args.sum_columns or args.mean_columns
    if needs_table and not table_ref:
        raise SystemExit("table action requires a table reference")
    if (args.sum_columns or args.mean_columns) and not args.by:
        raise SystemExit("table aggregation requires --by")
    if args.list or not table_ref:
        return emit_manifest_tables(data, args.dataset, args.contains, args.format)
    if args.schema:
        return emit_table_schema(data, table_ref, args.format)
    if args.path:
        print(data.table_path(data.resolve(table_ref)))
        return 0
    if args.by:
        return emit_table_aggregate(
            data,
            table_ref,
            by=args.by,
            sum_columns=args.sum_columns,
            mean_columns=args.mean_columns,
            where=args.where,
            contains=args.contains,
            limit=args.limit or 100,
            fmt=args.format,
        )
    if args.head:
        return emit_table_head(data, table_ref, limit=args.limit or 10, select=args.select, fmt=args.format)
    return emit_table_query(
        data,
        table_ref,
        where=args.where,
        contains=args.contains,
        select=args.select,
        limit=args.limit or 100,
        fmt=args.format,
    )


def cmd_head(args: argparse.Namespace, data: FruitloopsData) -> int:
    return emit_table_head(data, args.table, limit=args.limit, select=args.select, fmt=args.format)


def cmd_query(args: argparse.Namespace, data: FruitloopsData) -> int:
    return emit_table_query(
        data,
        args.table,
        where=args.where,
        contains=args.contains,
        select=args.select,
        limit=args.limit,
        fmt=args.format,
    )


def cmd_aggregate(args: argparse.Namespace, data: FruitloopsData) -> int:
    return emit_table_aggregate(
        data,
        args.table,
        by=args.by,
        sum_columns=args.sum_columns,
        mean_columns=args.mean_columns,
        where=args.where,
        contains=args.contains,
        limit=args.limit,
        fmt=args.format,
    )


def emit_manifest_tables(data: FruitloopsData, dataset: str | None, contains: list[str] | str | None, fmt: str) -> int:
    needle = contains[0] if isinstance(contains, list) and contains else contains
    rows = [
        {
            "dataset": table.dataset,
            "collection": table.collection,
            "file_id": table.file_id,
            "rows": str(table.rows),
            "columns": str(len(table.columns)),
            "relative_path": table.relative_path,
        }
        for table in data.tables(dataset, needle)
    ]
    emit_rows(rows, ["dataset", "collection", "file_id", "rows", "columns", "relative_path"], fmt)
    return 0


def emit_table_schema(data: FruitloopsData, table_ref: str, fmt: str) -> int:
    table = data.resolve(table_ref)
    rows = [
        {"index": str(index), "column": column, "table": table.file_id}
        for index, column in enumerate(table.columns, start=1)
    ]
    emit_rows(rows, ["index", "column", "table"], fmt)
    return 0


def emit_table_head(data: FruitloopsData, table_ref: str, limit: int, select: str | None, fmt: str) -> int:
    table = data.resolve(table_ref)
    columns = parse_columns(select, table.columns)
    rows = []
    for row in data.open_table(table):
        rows.append(project(row, columns))
        if len(rows) >= limit:
            break
    emit_rows(rows, columns, fmt)
    return 0


def emit_table_query(
    data: FruitloopsData,
    table_ref: str,
    *,
    where: list[str],
    contains: list[str],
    select: str | None,
    limit: int,
    fmt: str,
) -> int:
    table = data.resolve(table_ref)
    exact = parse_filters(where)
    substring_filters = parse_filters(contains)
    columns = parse_columns(select, table.columns)
    rows = []
    for row in data.open_table(table):
        if not matches(row, exact, substring_filters):
            continue
        rows.append(project(row, columns))
        if len(rows) >= limit:
            break
    emit_rows(rows, columns, fmt)
    return 0


def emit_table_aggregate(
    data: FruitloopsData,
    table_ref: str,
    *,
    by: str,
    sum_columns: str,
    mean_columns: str,
    where: list[str],
    contains: list[str],
    limit: int,
    fmt: str,
) -> int:
    table = data.resolve(table_ref)
    by_columns = split_csv(by)
    sums = split_csv(sum_columns)
    means = split_csv(mean_columns)
    rows = list(data.open_table(table))
    out = aggregate_rows(
        rows,
        by_columns,
        sums,
        means,
        parse_filters(where),
        parse_filters(contains),
    )
    columns = by_columns + ["count"] + [f"sum_{column}" for column in sums] + [
        f"mean_{column}" for column in means
    ]
    emit_rows(out[:limit], columns, fmt)
    return 0
