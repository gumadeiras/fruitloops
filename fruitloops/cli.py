from __future__ import annotations

import argparse
from pathlib import Path

from .aggregate import aggregate_rows
from .connectome import comparison_rows, partner_rows
from .data import FruitloopsData, TableInfo, default_data_dir
from .filters import matches, parse_filters, project, split_csv
from .formatting import emit_rows, parse_columns, print_table


FORMATS = ("table", "csv", "json", "jsonl")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="fruitloops",
        description="Query local connectome analysis CSVs.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Data directory. Defaults to FRUITLOOPS_DATA_DIR or ./data.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    datasets = subparsers.add_parser("datasets", help="List available datasets.")
    datasets.set_defaults(func=cmd_datasets)

    files = subparsers.add_parser("files", help="List tables from the manifest.")
    files.add_argument("--dataset", choices=("hemibrain", "flywire", "comparison"))
    files.add_argument("--contains", help="Case-insensitive filter on path/id.")
    files.add_argument("--format", choices=FORMATS, default="table")
    files.set_defaults(func=cmd_files)

    schema = subparsers.add_parser("schema", help="Show table columns.")
    schema.add_argument("--table", required=True)
    schema.add_argument("--format", choices=FORMATS, default="table")
    schema.set_defaults(func=cmd_schema)

    path = subparsers.add_parser("path", help="Print the CSV path for a table.")
    path.add_argument("--table", required=True)
    path.set_defaults(func=cmd_path)

    head = subparsers.add_parser("head", help="Show first rows from a table.")
    head.add_argument("--table", required=True)
    head.add_argument("--limit", type=int, default=10)
    head.add_argument("--select")
    head.add_argument("--format", choices=FORMATS, default="table")
    head.set_defaults(func=cmd_head)

    query = subparsers.add_parser("query", help="Filter a table.")
    query.add_argument("--table", required=True)
    query.add_argument("--where", action="append", default=[], help="Exact filter: column=value.")
    query.add_argument("--contains", action="append", default=[], help="Substring filter: column=text.")
    query.add_argument("--select")
    query.add_argument("--limit", type=int, default=100)
    query.add_argument("--format", choices=FORMATS, default="table")
    query.set_defaults(func=cmd_query)

    aggregate = subparsers.add_parser("aggregate", help="Group and aggregate a table.")
    aggregate.add_argument("--table", required=True)
    aggregate.add_argument("--by", required=True, help="Comma-separated group columns.")
    aggregate.add_argument("--sum", dest="sum_columns", default="", help="Comma-separated numeric columns to sum.")
    aggregate.add_argument("--mean", dest="mean_columns", default="", help="Comma-separated numeric columns to average.")
    aggregate.add_argument("--where", action="append", default=[], help="Exact filter: column=value.")
    aggregate.add_argument("--contains", action="append", default=[], help="Substring filter: column=text.")
    aggregate.add_argument("--limit", type=int, default=100)
    aggregate.add_argument("--format", choices=FORMATS, default="table")
    aggregate.set_defaults(func=cmd_aggregate)

    ln = subparsers.add_parser("ln", help="Find LN rows across common summary tables.")
    ln.add_argument("name", help="LN class/name/root id/body id search term.")
    ln.add_argument("--dataset", choices=("hemibrain", "flywire", "comparison"))
    ln.add_argument("--limit", type=int, default=200)
    ln.add_argument("--format", choices=FORMATS, default="table")
    ln.set_defaults(func=cmd_ln)

    partners = subparsers.add_parser("partners", help="Summarize ORN or PN partners for an LN.")
    partners.add_argument("name", help="LN class/name/root id/body id search term.")
    partners.add_argument("--dataset", choices=("hemibrain", "flywire"), required=True)
    partners.add_argument("--kind", choices=("orn", "pn"), required=True)
    partners.add_argument("--limit", type=int, default=100)
    partners.add_argument("--format", choices=FORMATS, default="table")
    partners.set_defaults(func=cmd_partners)

    compare = subparsers.add_parser("compare", help="Show cross-dataset comparison rows for an LN.")
    compare.add_argument("name", help="LN class/name search term.")
    compare.add_argument("--format", choices=FORMATS, default="table")
    compare.set_defaults(func=cmd_compare)

    args = parser.parse_args(argv)
    data = FruitloopsData(args.data_dir or default_data_dir())
    return args.func(args, data)


def cmd_datasets(args: argparse.Namespace, data: FruitloopsData) -> int:
    rows = [
        {"dataset": dataset, "tables": str(count), "data_dir": str(data.data_dir)}
        for dataset, count in data.datasets().items()
    ]
    print_table(rows, ["dataset", "tables", "data_dir"])
    return 0


def cmd_files(args: argparse.Namespace, data: FruitloopsData) -> int:
    rows = [
        {
            "dataset": table.dataset,
            "collection": table.collection,
            "file_id": table.file_id,
            "rows": str(table.rows),
            "columns": str(len(table.columns)),
            "relative_path": table.relative_path,
        }
        for table in data.tables(args.dataset, args.contains)
    ]
    emit_rows(rows, ["dataset", "collection", "file_id", "rows", "columns", "relative_path"], args.format)
    return 0


def cmd_schema(args: argparse.Namespace, data: FruitloopsData) -> int:
    table = data.resolve(args.table)
    rows = [
        {"index": str(index), "column": column, "table": table.file_id}
        for index, column in enumerate(table.columns, start=1)
    ]
    emit_rows(rows, ["index", "column", "table"], args.format)
    return 0


def cmd_path(args: argparse.Namespace, data: FruitloopsData) -> int:
    table = data.resolve(args.table)
    print(data.table_path(table))
    return 0


def cmd_head(args: argparse.Namespace, data: FruitloopsData) -> int:
    table = data.resolve(args.table)
    columns = parse_columns(args.select, table.columns)
    rows = []
    for row in data.open_table(table):
        rows.append(project(row, columns))
        if len(rows) >= args.limit:
            break
    emit_rows(rows, columns, args.format)
    return 0


def cmd_query(args: argparse.Namespace, data: FruitloopsData) -> int:
    table = data.resolve(args.table)
    exact = parse_filters(args.where)
    contains = parse_filters(args.contains)
    columns = parse_columns(args.select, table.columns)
    rows = []
    for row in data.open_table(table):
        if not matches(row, exact, contains):
            continue
        rows.append(project(row, columns))
        if len(rows) >= args.limit:
            break
    emit_rows(rows, columns, args.format)
    return 0


def cmd_aggregate(args: argparse.Namespace, data: FruitloopsData) -> int:
    table = data.resolve(args.table)
    by = split_csv(args.by)
    sum_columns = split_csv(args.sum_columns)
    mean_columns = split_csv(args.mean_columns)
    rows = list(data.open_table(table))
    out = aggregate_rows(
        rows,
        by,
        sum_columns,
        mean_columns,
        parse_filters(args.where),
        parse_filters(args.contains),
    )
    columns = by + ["count"] + [f"sum_{column}" for column in sum_columns] + [
        f"mean_{column}" for column in mean_columns
    ]
    emit_rows(out[: args.limit], columns, args.format)
    return 0


def cmd_ln(args: argparse.Namespace, data: FruitloopsData) -> int:
    tables = ln_candidate_tables(data, args.dataset)
    needle = args.name.lower()
    rows = []
    columns = [
        "dataset",
        "file_id",
        "matched_column",
        "matched_value",
        "LN_type",
        "combined_LN_type",
        "comparison_name",
        "recommended_match_group",
        "bodyId",
        "root_id",
        "hemisphere",
        "input_preference",
        "top_nt",
        "top_nt_conf",
    ]
    for table in tables:
        for row in data.open_table(table):
            match_column, match_value = row_matches_ln(row, needle)
            if not match_column:
                continue
            out = {column: row.get(column, "") for column in columns}
            out["dataset"] = table.dataset
            out["file_id"] = table.file_id
            out["matched_column"] = match_column
            out["matched_value"] = match_value
            rows.append(out)
            if len(rows) >= args.limit:
                emit_rows(rows, columns, args.format)
                return 0
    emit_rows(rows, columns, args.format)
    return 0


def cmd_partners(args: argparse.Namespace, data: FruitloopsData) -> int:
    rows, columns = partner_rows(data, args.dataset, args.name, args.kind)
    emit_rows(rows[: args.limit], columns, args.format)
    return 0


def cmd_compare(args: argparse.Namespace, data: FruitloopsData) -> int:
    rows = comparison_rows(data, args.name)
    columns = list(rows[0].keys()) if rows else data.resolve("comparison:matched_ln_class_similarity").columns
    emit_rows(rows, columns, args.format)
    return 0


def ln_candidate_tables(data: FruitloopsData, dataset: str | None) -> list[TableInfo]:
    priority = (
        "full_summary",
        "ln_observations_by_hemisphere",
        "flywire_ln_aliases",
        "flywire_ln_nt_top3_by_hemisphere",
        "matched_ln_class_similarity",
        "ln_class_inventory",
        "shared_type_comparison",
    )
    tables = data.tables(dataset)
    ranked = []
    for table in tables:
        path = table.relative_path.lower()
        score = next((index for index, term in enumerate(priority) if term in path), len(priority))
        if score < len(priority):
            ranked.append((score, table.relative_path, table))
    return [table for _, _, table in sorted(ranked)]


def row_matches_ln(row: dict[str, str], needle: str) -> tuple[str, str]:
    preferred_columns = (
        "LN_type",
        "combined_LN_type",
        "comparison_name",
        "recommended_match_group",
        "primary_flywire_name",
        "hemibrain_type_names",
        "all_names",
        "bodyId",
        "root_id",
        "source_root_id",
    )
    for column in preferred_columns:
        value = row.get(column, "")
        if needle in value.lower():
            return column, value
    return "", ""
