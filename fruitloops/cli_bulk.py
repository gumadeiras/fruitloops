from __future__ import annotations

import argparse
from pathlib import Path

from .bulk import (
    DEFAULT_BULK_DIR,
    DEFAULT_DUCKDB_PATH,
    archive_stem,
    connection_rows,
    create_common_views,
    download_source,
    extract_archive_csvs,
    import_to_duckdb,
    list_sources,
    optimize_connection_table,
    partner_rows as bulk_partner_rows,
    query_duckdb,
    schema_duckdb,
    table_summary,
)
from .filters import parse_filters, split_csv
from .formatting import emit_rows


def add_bulk_parser(subparsers, formats: tuple[str, ...]) -> None:
    bulk = subparsers.add_parser("bulk", help="Download/import/query bulk offline releases.")
    bulk.add_argument("--bulk-dir", type=Path, default=DEFAULT_BULK_DIR)
    bulk.add_argument("--store", type=Path, default=DEFAULT_DUCKDB_PATH)
    bulk_subparsers = bulk.add_subparsers(dest="bulk_action", required=True)

    bulk_sources = bulk_subparsers.add_parser("sources", help="List known bulk data sources.")
    bulk_sources.add_argument("--format", choices=formats, default="table")
    bulk_sources.set_defaults(func=cmd_bulk_sources)

    bulk_download = bulk_subparsers.add_parser("download", help="Download a known bulk source.")
    bulk_download.add_argument("--dataset", choices=("hemibrain", "flywire"), required=True)
    bulk_download.add_argument("--kind", required=True)
    bulk_download.add_argument("--force", action="store_true")
    bulk_download.set_defaults(func=cmd_bulk_download)

    bulk_import = bulk_subparsers.add_parser("import", help="Import a CSV/Parquet/Feather file into DuckDB.")
    bulk_import.add_argument("--path", type=Path, required=True)
    bulk_import.add_argument("--table", required=True)
    bulk_import.add_argument("--replace", action="store_true")
    bulk_import.set_defaults(func=cmd_bulk_import)

    bulk_extract = bulk_subparsers.add_parser("extract", help="Extract CSVs from a downloaded archive.")
    bulk_extract.add_argument("--path", type=Path, required=True)
    bulk_extract.add_argument("--output-dir", type=Path)
    bulk_extract.add_argument("--force", action="store_true")
    bulk_extract.add_argument("--format", choices=formats, default="table")
    bulk_extract.set_defaults(func=cmd_bulk_extract)

    bulk_tables = bulk_subparsers.add_parser("tables", help="List imported DuckDB tables.")
    bulk_tables.add_argument("--format", choices=formats, default="table")
    bulk_tables.set_defaults(func=cmd_bulk_tables)

    bulk_schema = bulk_subparsers.add_parser("schema", help="Show imported DuckDB table schema.")
    bulk_schema.add_argument("--table", required=True)
    bulk_schema.add_argument("--format", choices=formats, default="table")
    bulk_schema.set_defaults(func=cmd_bulk_schema)

    bulk_query = bulk_subparsers.add_parser("query", help="Query imported DuckDB tables.")
    bulk_query.add_argument("--table", required=True)
    bulk_query.add_argument("--where", action="append", default=[], help="Exact filter: column=value.")
    bulk_query.add_argument("--select")
    bulk_query.add_argument("--limit", type=int, default=50)
    bulk_query.add_argument("--format", choices=formats, default="table")
    bulk_query.set_defaults(func=cmd_bulk_query)

    bulk_connections = bulk_subparsers.add_parser("connections", help="Query inferred connection table rows.")
    bulk_connections.add_argument("--table", required=True)
    bulk_connections.add_argument("--pre-id")
    bulk_connections.add_argument("--post-id")
    bulk_connections.add_argument("--min-weight", type=int, default=1)
    bulk_connections.add_argument("--limit", type=int, default=50)
    bulk_connections.add_argument("--format", choices=formats, default="table")
    bulk_connections.set_defaults(func=cmd_bulk_connections)

    bulk_inputs = bulk_subparsers.add_parser("inputs", help="Query upstream partners for a body id.")
    add_bulk_partner_args(bulk_inputs, formats)
    bulk_inputs.set_defaults(func=cmd_bulk_inputs)

    bulk_outputs = bulk_subparsers.add_parser("outputs", help="Query downstream partners for a body id.")
    add_bulk_partner_args(bulk_outputs, formats)
    bulk_outputs.set_defaults(func=cmd_bulk_outputs)

    bulk_partners = bulk_subparsers.add_parser("partners", help="Query input and output partners for a body id.")
    add_bulk_partner_args(bulk_partners, formats)
    bulk_partners.set_defaults(func=cmd_bulk_partners)

    bulk_views = bulk_subparsers.add_parser("views", help="Create normalized edge/partner views.")
    bulk_views.add_argument("--table", required=True)
    bulk_views.add_argument("--prefix")
    bulk_views.add_argument("--format", choices=formats, default="table")
    bulk_views.set_defaults(func=cmd_bulk_views)

    bulk_optimize = bulk_subparsers.add_parser("optimize", help="Add indexes/statistics for a connection table.")
    bulk_optimize.add_argument("--table", required=True)
    bulk_optimize.add_argument("--prefix")
    bulk_optimize.add_argument("--format", choices=formats, default="table")
    bulk_optimize.set_defaults(func=cmd_bulk_optimize)


def add_bulk_partner_args(parser: argparse.ArgumentParser, formats: tuple[str, ...]) -> None:
    parser.add_argument("--table", required=True)
    parser.add_argument("--body-id", required=True)
    parser.add_argument("--min-weight", type=int, default=1)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--format", choices=formats, default="table")


def cmd_bulk_sources(args: argparse.Namespace, data) -> int:
    rows = list_sources()
    columns = ["dataset", "kind", "format", "filename", "table_name", "description", "url"]
    emit_rows(rows, columns, args.format)
    return 0


def cmd_bulk_download(args: argparse.Namespace, data) -> int:
    path = download_source(
        dataset=args.dataset,
        kind=args.kind,
        output_dir=args.bulk_dir / "raw",
        force=args.force,
    )
    print(path)
    return 0


def cmd_bulk_import(args: argparse.Namespace, data) -> int:
    row = import_to_duckdb(
        path=args.path,
        table_name=args.table,
        store=args.store,
        replace=args.replace,
    )
    emit_rows([row], ["store", "table", "rows"], "table")
    return 0


def cmd_bulk_extract(args: argparse.Namespace, data) -> int:
    output_dir = args.output_dir or args.bulk_dir / "extracted" / archive_stem(args.path)
    paths = extract_archive_csvs(args.path, output_dir=output_dir, force=args.force)
    rows = [{"path": str(path)} for path in paths]
    emit_rows(rows, ["path"], args.format)
    return 0


def cmd_bulk_tables(args: argparse.Namespace, data) -> int:
    rows = table_summary(args.store)
    emit_rows(rows, ["table", "rows", "store"], args.format)
    return 0


def cmd_bulk_schema(args: argparse.Namespace, data) -> int:
    rows = schema_duckdb(args.store, args.table)
    emit_rows(rows, ["column", "type", "nullable"], args.format)
    return 0


def cmd_bulk_query(args: argparse.Namespace, data) -> int:
    rows = query_duckdb(
        store=args.store,
        table=args.table,
        select=split_csv(args.select),
        where=parse_filters(args.where),
        limit=args.limit,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_bulk_connections(args: argparse.Namespace, data) -> int:
    rows = connection_rows(
        store=args.store,
        table=args.table,
        pre_id=args.pre_id,
        post_id=args.post_id,
        min_weight=args.min_weight,
        limit=args.limit,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_bulk_inputs(args: argparse.Namespace, data) -> int:
    return emit_bulk_partner_direction(args, "inputs")


def cmd_bulk_outputs(args: argparse.Namespace, data) -> int:
    return emit_bulk_partner_direction(args, "outputs")


def cmd_bulk_partners(args: argparse.Namespace, data) -> int:
    inputs = fetch_bulk_partner_rows(args, "inputs")
    outputs = fetch_bulk_partner_rows(args, "outputs")
    rows = [{"direction": "input", **row} for row in inputs] + [
        {"direction": "output", **row} for row in outputs
    ]
    emit_dynamic_rows(rows, args.format)
    return 0


def emit_bulk_partner_direction(args: argparse.Namespace, direction: str) -> int:
    rows = fetch_bulk_partner_rows(args, direction)
    emit_dynamic_rows(rows, args.format)
    return 0


def fetch_bulk_partner_rows(args: argparse.Namespace, direction: str) -> list[dict[str, str]]:
    return bulk_partner_rows(
        store=args.store,
        table=args.table,
        body_id=args.body_id,
        direction=direction,
        min_weight=args.min_weight,
        limit=args.limit,
    )


def cmd_bulk_views(args: argparse.Namespace, data) -> int:
    rows = create_common_views(args.store, args.table, prefix=args.prefix)
    emit_rows(rows, ["view", "store"], args.format)
    return 0


def cmd_bulk_optimize(args: argparse.Namespace, data) -> int:
    rows = optimize_connection_table(args.store, args.table, prefix=args.prefix)
    emit_rows(rows, ["action", "name", "column", "store"], args.format)
    return 0


def emit_dynamic_rows(rows: list[dict[str, str]], fmt: str) -> None:
    columns = list(rows[0].keys()) if rows else []
    emit_rows(rows, columns, fmt)
