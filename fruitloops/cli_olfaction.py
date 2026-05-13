from __future__ import annotations

import argparse
from pathlib import Path

from .bulk import DEFAULT_DUCKDB_PATH
from .cli_helpers import add_dataset_arg, add_format_arg, unique_values
from .formatting import emit_rows
from .olfaction import (
    build_olfaction_cache,
    olfaction_edges,
    olfaction_neurons,
    olfaction_orn_inputs,
    olfaction_pns,
    olfaction_tables,
)
from .olfaction_live import cache_olfaction_annotations


def add_olfaction_parser(subparsers, *, name: str = "olfaction", hidden: bool = False) -> None:
    help_text = argparse.SUPPRESS if hidden else "Build and query offline AL/LH/MB olfaction tables."
    olfaction = subparsers.add_parser(
        name,
        help=help_text,
    )
    olfaction.add_argument("--store", type=Path, default=DEFAULT_DUCKDB_PATH)
    olfaction_subparsers = olfaction.add_subparsers(dest="olfaction_action", required=True)

    olf_build = olfaction_subparsers.add_parser(
        "build",
        help="Build derived olfaction tables from imported bulk connectivity.",
    )
    olf_build.add_argument("--dataset", choices=("hemibrain", "flywire"), action="append")
    olf_build.add_argument("--keep-existing", action="store_true")
    add_format_arg(olf_build)
    olf_build.set_defaults(func=cmd_olfaction_build)

    olf_cache = olfaction_subparsers.add_parser(
        "cache-annotations",
        help="Fetch AL/LH/MB neuron annotations once and save them into DuckDB.",
    )
    olf_cache.add_argument("--dataset", choices=("hemibrain", "flywire"), action="append")
    olf_cache.add_argument("--chunk-size", type=int, default=2000)
    olf_cache.add_argument("--no-rebuild", action="store_true")
    add_format_arg(olf_cache)
    olf_cache.set_defaults(func=cmd_olfaction_cache_annotations)

    olf_tables = olfaction_subparsers.add_parser("tables", help="List derived olfaction tables.")
    add_format_arg(olf_tables)
    olf_tables.set_defaults(func=cmd_olfaction_tables)

    olf_neurons = olfaction_subparsers.add_parser("neurons", help="Query AL/LH/MB neurons.")
    add_olfaction_neuron_args(olf_neurons)
    olf_neurons.set_defaults(func=cmd_olfaction_neurons)

    olf_edges = olfaction_subparsers.add_parser("edges", help="Query AL/LH/MB connection rows.")
    add_dataset_arg(olf_edges, ("hemibrain", "flywire"))
    olf_edges.add_argument("--region", choices=("AL", "LH", "MB"))
    olf_edges.add_argument("--pre-id")
    olf_edges.add_argument("--post-id")
    olf_edges.add_argument("--min-synapses", type=int, default=1)
    olf_edges.add_argument("--limit", type=int, default=50)
    add_format_arg(olf_edges)
    olf_edges.set_defaults(func=cmd_olfaction_edges)

    olf_pns = olfaction_subparsers.add_parser("pns", help="Query projection neurons by glomerulus.")
    add_dataset_arg(olf_pns, ("hemibrain", "flywire"))
    olf_pns.add_argument("--glomerulus")
    olf_pns.add_argument("--limit", type=int, default=50)
    add_format_arg(olf_pns)
    olf_pns.set_defaults(func=cmd_olfaction_pns)

    olf_orn_inputs = olfaction_subparsers.add_parser(
        "orn-inputs",
        help="Summarize ORN inputs onto PNs, optionally by side.",
    )
    add_dataset_arg(olf_orn_inputs, ("hemibrain", "flywire"))
    olf_orn_inputs.add_argument("--glomerulus")
    olf_orn_inputs.add_argument("--pn-type")
    olf_orn_inputs.add_argument("--by-side", action="store_true")
    olf_orn_inputs.add_argument("--limit", type=int, default=50)
    add_format_arg(olf_orn_inputs)
    olf_orn_inputs.set_defaults(func=cmd_olfaction_orn_inputs)


def add_olfaction_neuron_args(parser: argparse.ArgumentParser) -> None:
    add_dataset_arg(parser, ("hemibrain", "flywire"))
    parser.add_argument("--region", choices=("AL", "LH", "MB"))
    parser.add_argument("--class", dest="cell_class", choices=("ORN", "PN", "LN", "KC", "MBON", "APL", "DAN"))
    parser.add_argument("--glomerulus")
    parser.add_argument("--contains")
    parser.add_argument("--limit", type=int, default=50)
    add_format_arg(parser)


def cmd_olfaction_build(args: argparse.Namespace, data) -> int:
    rows = build_olfaction_cache(
        store=args.store,
        datasets=unique_values(args.dataset),
        replace=not args.keep_existing,
    )
    emit_rows(rows, ["dataset", "table", "rows", "status", "store"], args.format)
    return 0


def cmd_olfaction_cache_annotations(args: argparse.Namespace, data) -> int:
    rows = cache_olfaction_annotations(
        store=args.store,
        datasets=unique_values(args.dataset),
        chunk_size=args.chunk_size,
        rebuild=not args.no_rebuild,
    )
    emit_rows(rows, ["dataset", "table", "rows", "status", "store"], args.format)
    return 0


def cmd_olfaction_tables(args: argparse.Namespace, data) -> int:
    rows = olfaction_tables(store=args.store)
    emit_rows(rows, ["table", "rows", "store"], args.format)
    return 0


def cmd_olfaction_neurons(args: argparse.Namespace, data) -> int:
    rows = olfaction_neurons(
        store=args.store,
        dataset=args.dataset,
        region=args.region,
        cell_class=args.cell_class,
        glomerulus=args.glomerulus,
        contains=args.contains,
        limit=args.limit,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_olfaction_edges(args: argparse.Namespace, data) -> int:
    rows = olfaction_edges(
        store=args.store,
        dataset=args.dataset,
        region=args.region,
        pre_id=args.pre_id,
        post_id=args.post_id,
        min_synapses=args.min_synapses,
        limit=args.limit,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_olfaction_pns(args: argparse.Namespace, data) -> int:
    rows = olfaction_pns(
        store=args.store,
        dataset=args.dataset,
        glomerulus=args.glomerulus,
        limit=args.limit,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_olfaction_orn_inputs(args: argparse.Namespace, data) -> int:
    rows = olfaction_orn_inputs(
        store=args.store,
        dataset=args.dataset,
        glomerulus=args.glomerulus,
        pn_type=args.pn_type,
        by_side=args.by_side,
        limit=args.limit,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def emit_dynamic_rows(rows: list[dict[str, str]], fmt: str) -> None:
    columns = list(rows[0].keys()) if rows else []
    emit_rows(rows, columns, fmt)
