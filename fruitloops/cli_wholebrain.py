from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .cli_helpers import add_dataset_arg, add_format_arg, require_dataset
from .curated import receptor_families
from .formatting import emit_rows
from .graph_analysis import ROUTES
from .neuron_labels import load_neuron_labels
from .selectors import add_selector_args, select_neurons, selector_values
from .wholebrain_query import OrnSeedSpec, path_rows, prepare_query, reach_rows, type_path_rows

PATH_COLUMNS = [
    "dataset",
    "route",
    "target_id",
    "target_type",
    "target_side",
    "rank",
    "source_id",
    "source_type",
    "source_side",
    "relation",
    "hops",
    "shortest_hops",
    "strength",
    "seed_weight",
    "sign",
    "signed_strength",
    "path_types",
    "path_ids",
    "path_sides",
    "step_synapses",
    "step_weights",
    "transmitters",
    "sign_conflict_types",
]
TYPE_PATH_COLUMNS = [
    "dataset",
    "route",
    "target_type",
    "rank",
    "hops",
    "path_types",
    "strength",
    "share",
    "ipsi_share",
    "signed_strength",
    "step_synapses",
]
REACH_TYPE_COLUMNS = ["dataset", "route", "hop", "target_type", "neurons", "reach", "rank", "rank_of"]
REACH_NEURON_COLUMNS = [
    "dataset",
    "route",
    "hop",
    "target_id",
    "target_type",
    "target_side",
    "reach",
    "rank",
    "rank_of",
]
SIDE_COLUMNS = ["ipsi", "contra", "ai", "signed_ipsi", "signed_contra", "signed_net"]
MAX_HOPS_LIMIT = 12
DOCS_URL = "https://github.com/gumadeiras/fruitloops/blob/main/docs/paths.md"
NEURON_COLUMNS = [
    "dataset",
    "id",
    "type",
    "side",
    "super_class",
    "cell_class",
    "cell_sub_class",
    "hemibrain_type",
    "top_nt",
    "transmitter",
    "sign",
    "type_sign_conflict",
]


def add_wholebrain_parsers(subparsers) -> None:
    neurons = subparsers.add_parser(
        "neurons",
        help="Look up whole-brain neurons by type, class, super class, or id.",
        description="Look up whole-brain neurons. Selector values inside one flag are OR'ed; flags are AND'ed.",
    )
    add_dataset_arg(neurons, ("hemibrain", "flywire"), required=True)
    neurons.add_argument("--store", type=Path)
    add_selector_args(neurons)
    neurons.add_argument(
        "--sign-conflicts",
        action="store_true",
        help="Only neurons of types whose neurons have different transmitter signs.",
    )
    neurons.add_argument("--limit", type=int, default=200)
    add_format_arg(neurons)
    neurons.set_defaults(func=cmd_neurons)

    paths = subparsers.add_parser(
        "paths",
        help="Rank strongest paths and fewest hops from source to target neurons.",
        description=(
            "Strongest path = maximum product of input-fraction weights; rank 1 is the strongest path, "
            "later ranks reach the target through a different last presynaptic neuron. "
            f"Definitions: {DOCS_URL}"
        ),
    )
    add_query_args(paths)
    paths.add_argument(
        "--via",
        action="append",
        choices=ROUTES,
        help="First-hop route; repeat for several. Default: all.",
    )
    paths.add_argument("--max-hops", type=int, default=6, help="Longest path to search (default 6).")
    paths.add_argument("--top", type=int, default=3, help="Paths per target neuron and route (default 3).")
    paths.add_argument(
        "--signed",
        action="store_true",
        help="Search only paths whose presynaptic neurons have a known fast-transmitter sign.",
    )
    paths.add_argument(
        "--by-type",
        action="store_true",
        help="Group paths by cell-type sequence and rank these type routes per target type.",
    )
    add_format_arg(paths)
    paths.set_defaults(func=cmd_paths)

    reach = subparsers.add_parser(
        "reach",
        help="Rank target neurons or types by hop-k reach from source neurons.",
        description=(
            "Reach at hop k = sum over all length-k paths of the product of weights. "
            "Type values are means over the neurons of the type. "
            f"Definitions: {DOCS_URL}"
        ),
    )
    add_query_args(reach)
    reach.add_argument("--hops", default="1,2,3", help="Comma-separated hop counts (default 1,2,3).")
    reach.add_argument("--by-route", action="store_true", help=f"Split by first-hop route: {', '.join(ROUTES)}.")
    reach.add_argument(
        "--by-side",
        action="store_true",
        help="Add ipsi/contra reach, laterality index, and signed net ipsi - contra.",
    )
    reach.add_argument("--per-neuron", action="store_true", help="One row per target neuron instead of per type.")
    add_format_arg(reach)
    reach.set_defaults(func=cmd_reach)


def add_query_args(parser: argparse.ArgumentParser) -> None:
    add_dataset_arg(parser, ("hemibrain", "flywire"), required=True)
    parser.add_argument("--store", type=Path)
    add_selector_args(parser, "source-", "source")
    add_selector_args(parser, "target-", "target")
    parser.add_argument(
        "--min-synapses",
        type=int,
        default=5,
        help="Keep a directed edge when its pair synapses (summed over neuropils) reach this value (default 5).",
    )
    orn = parser.add_argument_group("ORN seed weighting (FlyWire)")
    choice = orn.add_mutually_exclusive_group()
    choice.add_argument(
        "--orn-family",
        choices=receptor_families(),
        help="Seed each source by its input fraction from verified glomeruli of this receptor family.",
    )
    choice.add_argument("--orn-glomerulus", help="Seed each source by its input fraction from one glomerulus.")
    orn.add_argument("--orn-side", choices=("left", "right"), help="Use only ORNs from this antenna side.")


def orn_spec(args: argparse.Namespace) -> OrnSeedSpec:
    spec = OrnSeedSpec(family=args.orn_family, glomerulus=args.orn_glomerulus, side=args.orn_side)
    if spec.side and not spec.active:
        raise SystemExit("--orn-side needs --orn-family or --orn-glomerulus")
    return spec


def require_flywire_option(dataset: str, option: str, reason: str) -> None:
    if dataset != "flywire":
        raise SystemExit(f"{option} is not supported for {dataset}: {reason}; use --flywire")


def build_query(args: argparse.Namespace, command: str):
    dataset = require_dataset(args)
    spec = orn_spec(args)
    if spec.active:
        require_flywire_option(
            dataset,
            "ORN weighting",
            "the compact adjacency export lacks most ORNs and their glomerulus labels",
        )
    return prepare_query(
        command=command,
        store=args.store,
        dataset=dataset,
        source_values=selector_values(args, "source-"),
        target_values=selector_values(args, "target-"),
        min_synapses=args.min_synapses,
        orn=spec,
    )


def cmd_paths(args: argparse.Namespace, data) -> int:
    if not 1 <= args.max_hops <= MAX_HOPS_LIMIT:
        raise SystemExit(f"--max-hops must be between 1 and {MAX_HOPS_LIMIT}")
    if args.top < 1:
        raise SystemExit("--top must be at least 1")
    context = build_query(args, "fruitloops paths")
    routes = list(dict.fromkeys(args.via or ["all"]))
    if args.by_type:
        rows = type_path_rows(context, routes, args.max_hops, args.top, args.signed)
        emit_rows(rows, TYPE_PATH_COLUMNS, args.format)
        return 0
    rows = path_rows(context, routes, args.max_hops, args.top, args.signed)
    emit_rows(rows, PATH_COLUMNS, args.format)
    return 0


def cmd_reach(args: argparse.Namespace, data) -> int:
    dataset = require_dataset(args)
    hops = parse_hops(args.hops)
    if args.by_side and args.orn_side:
        raise SystemExit("--by-side compares left and right seeds; drop --orn-side")
    context = build_query(args, "fruitloops reach")
    routes = list(ROUTES) if args.by_route else ["all"]
    rows = reach_rows(context, routes, hops, per_neuron=args.per_neuron, by_side=args.by_side)
    columns = list(REACH_NEURON_COLUMNS if args.per_neuron else REACH_TYPE_COLUMNS)
    if args.by_side:
        columns += ([] if args.per_neuron else ["left_neurons", "right_neurons"]) + SIDE_COLUMNS
        print(
            f"fruitloops reach: signed values use per-neuron transmitter signs; "
            f"{len(context.labels.sign_conflict_types())} {dataset} types have neurons with conflicting signs "
            f"(list them with `fruitloops neurons --{dataset} --sign-conflicts`)",
            file=sys.stderr,
        )
    emit_rows(rows, columns, args.format)
    return 0


def parse_hops(value: str) -> list[int]:
    try:
        hops = sorted({int(item) for item in value.split(",") if item.strip()})
    except ValueError as exc:
        raise SystemExit(f"--hops expects comma-separated integers, got '{value}'") from exc
    if not hops or hops[0] < 1 or hops[-1] > MAX_HOPS_LIMIT:
        raise SystemExit(f"--hops values must be between 1 and {MAX_HOPS_LIMIT}")
    return hops


def cmd_neurons(args: argparse.Namespace, data) -> int:
    dataset = require_dataset(args)
    if args.limit < 1:
        raise SystemExit("--limit must be at least 1")
    labels = load_neuron_labels(args.store, dataset)
    conflicts = labels.sign_conflict_types()
    values = selector_values(args)
    if any(values.values()) or not args.sign_conflicts:
        rows = select_neurons(labels, values).rows.tolist()
    else:
        rows = list(range(len(labels)))
    if args.sign_conflicts:
        rows = [row for row in rows if labels.field("type")[row] in conflicts]
        if not rows:
            print("fruitloops neurons: no selected neurons belong to sign-conflict types", file=sys.stderr)
    out = [neuron_row(labels, row, conflicts) for row in rows]
    if len(out) > args.limit:
        print(f"fruitloops neurons: showing {args.limit} of {len(out)} neurons; use --limit", file=sys.stderr)
    emit_rows(out[: args.limit], NEURON_COLUMNS, args.format)
    return 0


def neuron_row(labels, row: int, conflicts: set[str]) -> dict[str, str]:
    out = {"dataset": labels.dataset, "id": str(int(labels.ids[row]))}
    for name in NEURON_COLUMNS[2:-2]:
        out[name] = str(labels.field(name)[row])
    out["sign"] = str(int(labels.sign[row]))
    out["type_sign_conflict"] = str(labels.field("type")[row] in conflicts).lower()
    return out
