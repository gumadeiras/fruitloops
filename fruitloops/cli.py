from __future__ import annotations

import argparse
import csv
from pathlib import Path

from . import __version__
from .cache import DEFAULT_CACHE_DIR, get_or_fetch
from .cli_helpers import (
    add_dataset_arg,
    add_format_arg,
    add_partner_kind_arg,
    require_dataset,
    require_partner_kind,
)
from .cli_bulk import add_bulk_parser
from .cli_setup import add_setup_parser
from .cli_status import add_locations_parser, add_status_parser
from .cli_table import add_legacy_table_parsers, add_table_parser
from .connectome import comparison_rows, partner_rows
from .data import FruitloopsData, TableInfo, default_data_dir
from .env import load_env_file
from .filters import matches, parse_filters, split_csv
from .formatting import emit_rows, print_table
from .live import (
    flywire_synapses,
    flywire_table,
    flywire_tables,
    hemibrain_custom,
    hemibrain_fetch_connections,
    hemibrain_fetch_neurons,
    parse_ints,
)
from .cli_olfaction import add_olfaction_parser
from .plotting import PlotSpec, render_plot


class HelpOnMissingArgsParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        if is_missing_argument_error(message):
            self.print_help()
            self.exit(0)
        super().error(message)


def is_missing_argument_error(message: str) -> bool:
    return (
        "the following arguments are required:" in message
        or ("one of the arguments" in message and "is required" in message)
        or "expected one argument" in message
        or "expected at least one argument" in message
    )


def main(argv: list[str] | None = None) -> int:
    parser = HelpOnMissingArgsParser(
        prog="fruitloops",
        description="Query local connectome analysis CSVs.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Data directory. Defaults to FRUITLOOPS_DATA_DIR or bundled/user data.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional env file for live database credentials. Defaults to .env.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    primary_commands = ("status", "setup", "olf", "table", "find", "partners", "examples", "admin")
    subparsers = parser.add_subparsers(dest="command", metavar="{" + ",".join(primary_commands) + "}")

    add_status_parser(subparsers)
    add_setup_parser(subparsers)
    add_olfaction_parser(subparsers, name="olf")
    add_table_parser(subparsers)

    find = subparsers.add_parser("find", help="Find LN rows across common summary tables.")
    add_find_args(find)
    find.set_defaults(func=cmd_ln)

    partners = subparsers.add_parser("partners", help="Summarize ORN or PN partners for an LN.")
    partners.add_argument("name", help="LN class/name/root id/body id search term.")
    add_dataset_arg(partners, ("hemibrain", "flywire"), required=True)
    add_partner_kind_arg(partners, required=True)
    partners.add_argument("--limit", type=int, default=100)
    add_format_arg(partners)
    partners.set_defaults(func=cmd_partners)

    examples = subparsers.add_parser("examples", help="Show common task recipes.")
    examples.set_defaults(func=cmd_examples)

    admin = subparsers.add_parser("admin", help="Run advanced commands: bulk, live, offline, olfaction, plot.")
    admin.add_argument("args", nargs=argparse.REMAINDER)
    admin.set_defaults(func=cmd_admin)

    add_locations_parser(subparsers)

    datasets = subparsers.add_parser("datasets", help=argparse.SUPPRESS)
    datasets.set_defaults(func=cmd_datasets)

    add_legacy_table_parsers(subparsers)

    ln = subparsers.add_parser("ln", help=argparse.SUPPRESS)
    add_find_args(ln)
    ln.set_defaults(func=cmd_ln)

    compare = subparsers.add_parser("compare", help=argparse.SUPPRESS)
    compare.add_argument("name", help="LN class/name search term.")
    add_format_arg(compare)
    compare.set_defaults(func=cmd_compare)

    plot = subparsers.add_parser("plot", help=argparse.SUPPRESS)
    plot_source = plot.add_mutually_exclusive_group(required=True)
    plot_source.add_argument("--table", help="Table reference or file_id.")
    plot_source.add_argument("--csv", type=Path, help="Path to any CSV file.")
    plot.add_argument(
        "--kind",
        choices=("scatter", "line", "bar", "hist", "violin", "bubble", "heatmap"),
        required=True,
    )
    plot.add_argument("--x")
    plot.add_argument("--y")
    plot.add_argument("--value")
    plot.add_argument("--color")
    plot.add_argument("--size")
    plot.add_argument("--label")
    plot.add_argument("--top-labels", type=int, default=0)
    plot.add_argument("--where", action="append", default=[], help="Exact filter: column=value.")
    plot.add_argument("--contains", action="append", default=[], help="Substring filter: column=text.")
    plot.add_argument("--limit", type=int)
    plot.add_argument("--title")
    plot.add_argument("--xlabel")
    plot.add_argument("--ylabel")
    plot.add_argument("--log-x", action="store_true")
    plot.add_argument("--log-y", action="store_true")
    plot.add_argument("--width", type=float, default=7.0)
    plot.add_argument("--height", type=float, default=5.0)
    plot.add_argument("--dpi", type=int, default=300)
    plot.add_argument("--output", type=Path, required=True, help="Output path without suffix.")
    plot.add_argument("--formats", default="png", help="Comma-separated: png,pdf,svg.")
    plot.set_defaults(func=cmd_plot)

    live = subparsers.add_parser("live", help=argparse.SUPPRESS)
    live_subparsers = live.add_subparsers(dest="live_dataset", required=True)

    hemibrain = live_subparsers.add_parser("hemibrain", help="Fetch from neuprint hemibrain.")
    hemibrain_subparsers = hemibrain.add_subparsers(dest="live_action", required=True)

    hb_neurons = hemibrain_subparsers.add_parser("neurons", help="Fetch hemibrain neuron metadata.")
    hb_neurons.add_argument("--body-id", action="append", default=[], help="Body id, repeatable or comma-separated.")
    hb_neurons.add_argument("--type-contains")
    hb_neurons.add_argument("--instance-contains")
    hb_neurons.add_argument("--limit", type=int, default=20)
    add_format_arg(hb_neurons)
    hb_neurons.set_defaults(func=cmd_live_hemibrain_neurons)

    hb_connections = hemibrain_subparsers.add_parser("connections", help="Fetch hemibrain weighted connections.")
    hb_connections.add_argument("--upstream-body-id", action="append", default=[], help="Upstream body id.")
    hb_connections.add_argument("--downstream-body-id", action="append", default=[], help="Downstream body id.")
    hb_connections.add_argument("--min-weight", type=int, default=1)
    hb_connections.add_argument("--limit", type=int, default=50)
    add_format_arg(hb_connections)
    hb_connections.set_defaults(func=cmd_live_hemibrain_connections)

    hb_cypher = hemibrain_subparsers.add_parser("cypher", help="Run a hemibrain Cypher query.")
    hb_cypher.add_argument("--query", required=True)
    hb_cypher.add_argument("--limit", type=int)
    add_format_arg(hb_cypher)
    hb_cypher.set_defaults(func=cmd_live_hemibrain_cypher)

    flywire = live_subparsers.add_parser("flywire", help="Fetch from CAVE FlyWire.")
    flywire_subparsers = flywire.add_subparsers(dest="live_action", required=True)

    fw_tables = flywire_subparsers.add_parser("tables", help="List FlyWire materialized tables.")
    add_format_arg(fw_tables)
    fw_tables.set_defaults(func=cmd_live_flywire_tables)

    fw_table = flywire_subparsers.add_parser("table", help="Query a FlyWire materialized table.")
    fw_table.add_argument("--table", required=True)
    fw_table.add_argument("--where", action="append", default=[], help="Exact filter: column=value.")
    fw_table.add_argument("--in", dest="in_filters", action="append", default=[], help="Membership filter: column=a,b,c.")
    fw_table.add_argument("--select", help="Comma-separated columns.")
    fw_table.add_argument("--limit", type=int, default=50)
    fw_table.add_argument("--materialization-version", type=int)
    add_format_arg(fw_table)
    fw_table.set_defaults(func=cmd_live_flywire_table)

    fw_synapses = flywire_subparsers.add_parser("synapses", help="Query FlyWire synapses_nt_v1.")
    fw_synapses.add_argument("--pre-root-id", action="append", default=[], help="Pre root id.")
    fw_synapses.add_argument("--post-root-id", action="append", default=[], help="Post root id.")
    fw_synapses.add_argument("--limit", type=int, default=50)
    fw_synapses.add_argument("--materialization-version", type=int)
    add_format_arg(fw_synapses)
    fw_synapses.set_defaults(func=cmd_live_flywire_synapses)

    offline = subparsers.add_parser("offline", help=argparse.SUPPRESS)
    offline.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    offline_subparsers = offline.add_subparsers(dest="offline_action", required=True)

    offline_list = offline_subparsers.add_parser("list", help="List cached live queries.")
    add_format_arg(offline_list)
    offline_list.set_defaults(func=cmd_offline_list)

    offline_fetch = offline_subparsers.add_parser(
        "fetch",
        help="Read cached result first; fetch live and save on miss.",
    )
    add_dataset_arg(offline_fetch, ("hemibrain", "flywire"), required=True)
    offline_fetch.add_argument(
        "--action",
        choices=("neurons", "connections", "cypher", "tables", "table", "synapses"),
        required=True,
    )
    offline_fetch.add_argument("--body-id", action="append", default=[])
    offline_fetch.add_argument("--type-contains")
    offline_fetch.add_argument("--instance-contains")
    offline_fetch.add_argument("--upstream-body-id", action="append", default=[])
    offline_fetch.add_argument("--downstream-body-id", action="append", default=[])
    offline_fetch.add_argument("--min-weight", type=int, default=1)
    offline_fetch.add_argument("--query")
    offline_fetch.add_argument("--table")
    offline_fetch.add_argument("--where", action="append", default=[])
    offline_fetch.add_argument("--in", dest="in_filters", action="append", default=[])
    offline_fetch.add_argument("--select")
    offline_fetch.add_argument("--pre-root-id", action="append", default=[])
    offline_fetch.add_argument("--post-root-id", action="append", default=[])
    offline_fetch.add_argument("--limit", type=int, default=50)
    offline_fetch.add_argument("--materialization-version", type=int)
    offline_fetch.add_argument("--refresh", action="store_true", help="Bypass cache and fetch live.")
    offline_fetch.add_argument("--offline-only", action="store_true", help="Fail closed on cache miss.")
    add_format_arg(offline_fetch)
    offline_fetch.set_defaults(func=cmd_offline_fetch)

    add_bulk_parser(subparsers, hidden=True)
    add_olfaction_parser(subparsers, hidden=True)
    hide_subcommands(
        subparsers,
        (
            "locations",
            "datasets",
            "files",
            "schema",
            "path",
            "head",
            "query",
            "aggregate",
            "ln",
            "compare",
            "plot",
            "live",
            "offline",
            "bulk",
            "olfaction",
        ),
    )

    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    validate_required_aliases(args)
    load_env_file(args.env_file)
    data = None if command_uses_no_manifest(args) else FruitloopsData(args.data_dir or default_data_dir())
    return args.func(args, data)


def add_find_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("name", help="LN class/name/root id/body id search term.")
    add_dataset_arg(parser, ("hemibrain", "flywire", "comparison"))
    parser.add_argument("--limit", type=int, default=200)
    add_format_arg(parser)


def hide_subcommands(subparsers, names: tuple[str, ...]) -> None:
    hidden = set(names)
    subparsers._choices_actions = [action for action in subparsers._choices_actions if action.dest not in hidden]


def validate_required_aliases(args: argparse.Namespace) -> None:
    if getattr(args, "_required_dataset", False):
        require_dataset(args)
    if getattr(args, "_required_kind", False):
        require_partner_kind(args)


def cmd_datasets(args: argparse.Namespace, data: FruitloopsData) -> int:
    rows = [
        {"dataset": dataset, "tables": str(count), "data_dir": str(data.data_dir)}
        for dataset, count in data.datasets().items()
    ]
    print_table(rows, ["dataset", "tables", "data_dir"])
    return 0


def cmd_admin(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    if not args.args:
        print(ADMIN_HELP)
        return 0
    forwarded = []
    if args.data_dir:
        forwarded.extend(["--data-dir", str(args.data_dir)])
    if args.env_file:
        forwarded.extend(["--env-file", str(args.env_file)])
    return main(forwarded + args.args)


def cmd_examples(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    print(EXAMPLES)
    return 0


ADMIN_HELP = """usage: fruitloops admin <command> ...

Advanced commands:
  bulk      Download/import/query bulk offline releases.
  live      Fetch data from live connectome APIs.
  offline   Use the offline-first live-query cache.
  olfaction Build/query derived AL/LH/MB tables.
  plot      Render plots from CSV tables.
"""


EXAMPLES = """fruitloops status --csv
fruitloops setup --csv
fruitloops table --flywire --contains full_summary --csv
fruitloops table comparison:matched_ln_class_similarity --schema --csv
fruitloops table comparison:matched_ln_class_similarity --contains LN_class=il3LN6 --json
fruitloops find il3LN6 --flywire --csv
fruitloops partners il3LN6 --flywire --orn --csv
fruitloops admin offline fetch --dataset flywire --action synapses --pre-root-id ROOT --limit 10 --csv
fruitloops setup --flywire --csv
"""


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
    rows, columns = partner_rows(data, require_dataset(args), args.name, require_partner_kind(args))
    emit_rows(rows[: args.limit], columns, args.format)
    return 0


def cmd_compare(args: argparse.Namespace, data: FruitloopsData) -> int:
    rows = comparison_rows(data, args.name)
    columns = list(rows[0].keys()) if rows else data.resolve("comparison:matched_ln_class_similarity").columns
    emit_rows(rows, columns, args.format)
    return 0


def cmd_plot(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    exact = parse_filters(args.where)
    contains = parse_filters(args.contains)
    if args.csv:
        source_rows = load_csv_rows(args.csv)
    else:
        if data is None:
            raise ValueError("--table requires a data manifest")
        source_rows = data.open_table(data.resolve(args.table))
    rows = [row for row in source_rows if matches(row, exact, contains)]
    spec = PlotSpec(
        kind=args.kind,
        x=args.x,
        y=args.y,
        value=args.value,
        color=args.color,
        size=args.size,
        label=args.label,
        top_labels=args.top_labels,
        title=args.title,
        xlabel=args.xlabel,
        ylabel=args.ylabel,
        log_x=args.log_x,
        log_y=args.log_y,
        width=args.width,
        height=args.height,
        dpi=args.dpi,
        output=args.output,
        formats=tuple(split_csv(args.formats)),
        limit=args.limit,
    )
    paths = render_plot(rows, spec)
    for path in paths:
        print(path)
    return 0


def cmd_live_hemibrain_neurons(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    rows = hemibrain_fetch_neurons(
        body_ids=parse_ints(args.body_id),
        type_contains=args.type_contains,
        instance_contains=args.instance_contains,
        limit=args.limit,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_live_hemibrain_connections(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    rows = hemibrain_fetch_connections(
        upstream_body_ids=parse_ints(args.upstream_body_id),
        downstream_body_ids=parse_ints(args.downstream_body_id),
        min_weight=args.min_weight,
        limit=args.limit,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_live_hemibrain_cypher(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    rows = hemibrain_custom(args.query, limit=args.limit)
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_live_flywire_tables(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    rows = flywire_tables()
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_live_flywire_table(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    rows = flywire_table(
        table=args.table,
        exact_filters=parse_filters(args.where),
        in_filters=parse_filters(args.in_filters),
        select_columns=split_csv(args.select),
        limit=args.limit,
        materialization_version=args.materialization_version,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_live_flywire_synapses(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    rows = flywire_synapses(
        pre_root_ids=parse_ints(args.pre_root_id),
        post_root_ids=parse_ints(args.post_root_id),
        limit=args.limit,
        materialization_version=args.materialization_version,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_offline_list(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    rows = [
        {
            "dataset": entry.dataset,
            "action": entry.action,
            "key": entry.key,
            "rows": str(entry.rows),
            "created_at": entry.created_at,
            "path": str(entry.path),
        }
        for entry in list_cache(args.cache_dir)
    ]
    emit_rows(rows, ["dataset", "action", "key", "rows", "created_at", "path"], args.format)
    return 0


def cmd_offline_fetch(args: argparse.Namespace, data: FruitloopsData | None) -> int:
    dataset, action, query, fetcher = build_offline_fetch(args)
    rows, entry, source = get_or_fetch(
        cache_dir=args.cache_dir,
        dataset=dataset,
        action=action,
        query=query,
        fetch=fetcher,
        refresh=args.refresh,
        offline_only=args.offline_only,
    )
    if source == "miss":
        raise SystemExit("offline cache miss; rerun without --offline-only to fetch live")
    emit_dynamic_rows(rows, args.format)
    if args.format == "table" and entry is not None:
        print(f"# source={source} cache={entry.path}")
    return 0


def build_offline_fetch(args: argparse.Namespace):
    dataset = require_dataset(args)
    action = args.action
    query = offline_query_payload(args)

    if dataset == "hemibrain" and action == "neurons":
        return dataset, action, query, lambda: hemibrain_fetch_neurons(
            body_ids=parse_ints(args.body_id),
            type_contains=args.type_contains,
            instance_contains=args.instance_contains,
            limit=args.limit,
        )
    if dataset == "hemibrain" and action == "connections":
        return dataset, action, query, lambda: hemibrain_fetch_connections(
            upstream_body_ids=parse_ints(args.upstream_body_id),
            downstream_body_ids=parse_ints(args.downstream_body_id),
            min_weight=args.min_weight,
            limit=args.limit,
        )
    if dataset == "hemibrain" and action == "cypher":
        if not args.query:
            raise ValueError("hemibrain cypher requires --query")
        return dataset, action, query, lambda: hemibrain_custom(args.query, limit=args.limit)
    if dataset == "flywire" and action == "tables":
        return dataset, action, query, flywire_tables
    if dataset == "flywire" and action == "table":
        if not args.table:
            raise ValueError("flywire table requires --table")
        return dataset, action, query, lambda: flywire_table(
            table=args.table,
            exact_filters=parse_filters(args.where),
            in_filters=parse_filters(args.in_filters),
            select_columns=split_csv(args.select),
            limit=args.limit,
            materialization_version=args.materialization_version,
        )
    if dataset == "flywire" and action == "synapses":
        return dataset, action, query, lambda: flywire_synapses(
            pre_root_ids=parse_ints(args.pre_root_id),
            post_root_ids=parse_ints(args.post_root_id),
            limit=args.limit,
            materialization_version=args.materialization_version,
        )
    raise ValueError(f"unsupported offline fetch: {dataset} {action}")


def offline_query_payload(args: argparse.Namespace) -> dict[str, object]:
    keys = (
        "body_id",
        "type_contains",
        "instance_contains",
        "upstream_body_id",
        "downstream_body_id",
        "min_weight",
        "query",
        "table",
        "where",
        "in_filters",
        "select",
        "pre_root_id",
        "post_root_id",
        "limit",
        "materialization_version",
    )
    payload = {}
    for key in keys:
        value = getattr(args, key)
        if value not in (None, [], ""):
            payload[key] = value
    return payload


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.expanduser().open(newline="") as handle:
        return list(csv.DictReader(handle))


def emit_dynamic_rows(rows: list[dict[str, str]], fmt: str) -> None:
    columns = list(rows[0].keys()) if rows else []
    emit_rows(rows, columns, fmt)


def command_uses_no_manifest(args: argparse.Namespace) -> bool:
    return (args.command == "plot" and args.csv) or args.command in {
        "admin",
        "examples",
        "locations",
        "setup",
        "live",
        "offline",
        "bulk",
        "olfaction",
    }


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
