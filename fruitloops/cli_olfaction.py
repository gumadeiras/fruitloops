from __future__ import annotations

import argparse
from pathlib import Path

from .bulk import DEFAULT_DUCKDB_PATH
from .cli_helpers import add_dataset_arg, add_format_arg, unique_values
from .formatting import emit_rows
from .olfaction import (
    build_olfaction_cache,
    olfaction_class_summary,
    olfaction_edges,
    olfaction_glomerulus_summary,
    olfaction_input_summary,
    olfaction_neurons,
    olfaction_orn_inputs,
    olfaction_pathway_summary,
    olfaction_pns,
    olfaction_tables,
)
from .olfaction_live import cache_olfaction_annotations


CELL_CLASS_CHOICES = ("ORN", "PN", "LN", "KC", "MBON", "APL", "DAN")


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

    olf_classes = olfaction_subparsers.add_parser("classes", help="Summarize olfactory neuron classes.")
    add_dataset_arg(olf_classes, ("hemibrain", "flywire"))
    olf_classes.add_argument("--region", choices=("AL", "LH", "MB"))
    olf_classes.add_argument("--class", dest="cell_class", choices=CELL_CLASS_CHOICES)
    olf_classes.add_argument("--glomerulus")
    olf_classes.add_argument("--limit", type=int, default=100)
    add_format_arg(olf_classes)
    olf_classes.set_defaults(func=cmd_olfaction_classes)

    olf_glomerulus = olfaction_subparsers.add_parser("glomerulus", help="Summarize glomerulus inventory and ORN->PN input.")
    olf_glomerulus.add_argument("name", nargs="?")
    add_dataset_arg(olf_glomerulus, ("hemibrain", "flywire"))
    olf_glomerulus.add_argument("--limit", type=int, default=100)
    add_format_arg(olf_glomerulus)
    olf_glomerulus.set_defaults(func=cmd_olfaction_glomerulus)

    olf_pathway = olfaction_subparsers.add_parser("pathway", help="Summarize source-class to target-class pathways.")
    olf_pathway.add_argument("source_class", choices=CELL_CLASS_CHOICES)
    olf_pathway.add_argument("target_class", choices=CELL_CLASS_CHOICES)
    add_dataset_arg(olf_pathway, ("hemibrain", "flywire"))
    olf_pathway.add_argument("--region", choices=("AL", "LH", "MB"))
    olf_pathway.add_argument("--glomerulus")
    olf_pathway.add_argument("--source-glomerulus")
    olf_pathway.add_argument("--target-glomerulus")
    olf_pathway.add_argument("--by-side", action="store_true")
    olf_pathway.add_argument("--limit", type=int, default=100)
    add_format_arg(olf_pathway)
    olf_pathway.set_defaults(func=cmd_olfaction_pathway)

    olf_inputs = olfaction_subparsers.add_parser("inputs", help="Summarize inputs onto target neurons.")
    add_dataset_arg(olf_inputs, ("hemibrain", "flywire"))
    olf_inputs.add_argument("--target-class", choices=CELL_CLASS_CHOICES)
    olf_inputs.add_argument("--source-class", choices=CELL_CLASS_CHOICES)
    olf_inputs.add_argument("--target-id")
    olf_inputs.add_argument("--glomerulus")
    olf_inputs.add_argument("--region", choices=("AL", "LH", "MB"))
    olf_inputs.add_argument("--by-side", action="store_true")
    olf_inputs.add_argument("--limit", type=int, default=100)
    add_format_arg(olf_inputs)
    olf_inputs.set_defaults(func=cmd_olfaction_inputs)

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
    parser.add_argument("--class", dest="cell_class", choices=CELL_CLASS_CHOICES)
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


def cmd_olfaction_classes(args: argparse.Namespace, data) -> int:
    rows = olfaction_class_summary(
        store=args.store,
        dataset=args.dataset,
        region=args.region,
        cell_class=args.cell_class,
        glomerulus=args.glomerulus,
        limit=args.limit,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_olfaction_glomerulus(args: argparse.Namespace, data) -> int:
    rows = olfaction_glomerulus_summary(
        store=args.store,
        dataset=args.dataset,
        glomerulus=args.name,
        limit=args.limit,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_olfaction_pathway(args: argparse.Namespace, data) -> int:
    rows = olfaction_pathway_summary(
        store=args.store,
        dataset=args.dataset,
        source_class=args.source_class,
        target_class=args.target_class,
        region=args.region,
        glomerulus=args.glomerulus,
        source_glomerulus=args.source_glomerulus,
        target_glomerulus=args.target_glomerulus,
        by_side=args.by_side,
        limit=args.limit,
    )
    emit_dynamic_rows(rows, args.format)
    return 0


def cmd_olfaction_inputs(args: argparse.Namespace, data) -> int:
    rows = olfaction_input_summary(
        store=args.store,
        dataset=args.dataset,
        target_class=args.target_class,
        source_class=args.source_class,
        target_id=args.target_id,
        glomerulus=args.glomerulus,
        region=args.region,
        by_side=args.by_side,
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
