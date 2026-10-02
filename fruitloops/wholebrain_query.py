"""Shared query setup and row building for the ``paths`` and ``reach`` commands."""

from __future__ import annotations

import difflib
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .curated import family_glomeruli, hemibrain_glomerulus_names
from .graph_analysis import (
    ROUTES,
    EdgeTable,
    QueryGraph,
    competition_ranks,
    first_hop_edges,
    input_fraction_seeds,
    propagate,
    ranked_paths,
    relay_edges,
    search_paths,
    shortest_hops,
    threshold_graph,
)
from .graph_cache import ConnectomeGraph, load_graph
from .hemibrain_orns import HemibrainOrnInputs, hemibrain_orn_seeds, load_hemibrain_orn_inputs
from .neuron_labels import NeuronLabels, load_neuron_labels
from .selectors import Selection, select_neurons
from .type_routes import LEFT, OTHER, RIGHT, RouteGraph, route_synapses, strongest_type_routes

SIDES = ("left", "right")
# How errors name the ORNs that --orn-glomerulus and all --orn-* options can match.
FLYWIRE_ORNS = ("FlyWire ORN/TRN/HRN neurons", "FlyWire sensory neurons")
HEMIBRAIN_ORNS = ("hemibrain ORNs", "hemibrain ORNs")


@dataclass(frozen=True)
class OrnSeedSpec:
    family: str | None = None
    glomerulus: str | None = None
    side: str | None = None

    @property
    def active(self) -> bool:
        return bool(self.family or self.glomerulus)


@dataclass
class QueryContext:
    command: str
    labels: NeuronLabels
    graph: ConnectomeGraph
    query: QueryGraph
    node_rows: np.ndarray
    row_display: np.ndarray
    display: np.ndarray
    sides: np.ndarray
    transmitters: np.ndarray
    signs: np.ndarray
    kenyon: np.ndarray
    sources: np.ndarray
    seeds: np.ndarray
    orn: OrnSeedSpec
    targets: Selection
    target_nodes: np.ndarray
    orn_inputs: HemibrainOrnInputs | None = None

    @property
    def dataset(self) -> str:
        return self.labels.dataset


def prepare_query(
    *,
    command: str,
    store: Path,
    dataset: str,
    source_values: dict[str, list[str]],
    target_values: dict[str, list[str]],
    min_synapses: int,
    orn: OrnSeedSpec,
) -> QueryContext:
    if min_synapses < 1:
        raise SystemExit("--min-synapses must be at least 1")
    labels = load_neuron_labels(store, dataset)
    source_selection = select_neurons(labels, source_values, "source-")
    targets = select_neurons(labels, target_values, "target-")
    graph = load_graph(store, dataset, command=command)
    node_rows = labels.rows_for_ids(graph.ids)
    known = node_rows >= 0
    rows = node_rows.clip(min=0)

    def node_field(values: np.ndarray, missing: object) -> np.ndarray:
        return np.where(known, values[rows], missing)

    display_by_row = labels.display_types()
    sources = np.zeros(graph.size, dtype=bool)
    source_nodes = graph.nodes_for_ids(labels.ids[source_selection.rows])
    sources[source_nodes[source_nodes >= 0]] = True
    if not sources.any():
        raise SystemExit(f"{source_selection.description} selects neurons without connections in the {dataset} graph")
    context = QueryContext(
        command=command,
        labels=labels,
        graph=graph,
        query=threshold_graph(graph, min_synapses),
        node_rows=node_rows,
        row_display=display_by_row,
        display=node_field(display_by_row, "[unlabeled]"),
        sides=node_field(labels.field("side"), ""),
        transmitters=node_field(labels.field("transmitter"), ""),
        signs=node_field(labels.sign, 0).astype(np.int8),
        kenyon=node_field(labels.kenyon_cells(), False).astype(bool),
        sources=sources,
        seeds=np.zeros(graph.size),
        orn=orn,
        targets=targets,
        target_nodes=graph.nodes_for_ids(labels.ids[targets.rows]),
        orn_inputs=load_hemibrain_orn_inputs(store) if orn.active and dataset == "hemibrain" else None,
    )
    context.seeds = seed_vector(context, orn)
    report_seeds(context, source_selection)
    return context


def seed_vector(context: QueryContext, orn: OrnSeedSpec, side: str | None = None) -> np.ndarray:
    """Seed weights over graph nodes; ``side`` limits seeds to one side."""
    if not orn.active:
        sources = context.sources & (context.sides == side) if side else context.sources
        return sources.astype(np.float64)
    if context.orn_inputs is not None:
        chosen = hemibrain_orn_rows(context.orn_inputs, orn, side or orn.side)
        return hemibrain_orn_seeds(context.graph, context.sources, context.orn_inputs, chosen)
    inputs = orn_input_nodes(context, orn, side or orn.side)
    return input_fraction_seeds(context.graph, context.sources, inputs)


def orn_input_nodes(context: QueryContext, orn: OrnSeedSpec, side: str | None) -> np.ndarray:
    """FlyWire graph nodes of the chosen ORNs."""
    glomeruli_by_row = context.labels.sensory_glomeruli()
    glomeruli = np.where(context.node_rows >= 0, glomeruli_by_row[context.node_rows.clip(min=0)], "")
    present = sorted({value for value in glomeruli_by_row.tolist() if value})
    return select_orns(glomeruli, context.sides, present, orn, side, FLYWIRE_ORNS)


def hemibrain_orn_rows(inputs: HemibrainOrnInputs, orn: OrnSeedSpec, side: str | None) -> np.ndarray:
    """ORN table rows of the chosen ORNs."""
    present = sorted(set(inputs.glomeruli.tolist()))
    renamed = hemibrain_glomerulus_names()
    if orn.glomerulus in renamed and orn.glomerulus not in present:
        raise SystemExit(
            f"--orn-glomerulus '{orn.glomerulus}': hemibrain v1.2 type ORN_{orn.glomerulus} is glomerulus "
            f"{renamed[orn.glomerulus]} (Schlegel et al. 2021); use --orn-glomerulus {renamed[orn.glomerulus]}"
        )
    return select_orns(inputs.glomeruli, inputs.sides, present, orn, side, HEMIBRAIN_ORNS)


def select_orns(
    glomeruli: np.ndarray,
    sides: np.ndarray,
    present: list[str],
    orn: OrnSeedSpec,
    side: str | None,
    names: tuple[str, str],
) -> np.ndarray:
    """Mask of the ORNs of the chosen glomeruli and side; ``names`` words the errors."""
    if orn.glomerulus:
        if orn.glomerulus not in present:
            close = difflib.get_close_matches(orn.glomerulus, present, n=5, cutoff=0.5)
            hint = f"; close matches: {', '.join(close)}" if close else ""
            raise SystemExit(f"--orn-glomerulus '{orn.glomerulus}' has no {names[0]}{hint}")
        chosen = {orn.glomerulus}
    else:
        chosen = family_glomeruli(orn.family or "")
    inputs = np.isin(glomeruli, sorted(chosen))
    if side:
        inputs &= sides == side
    if not inputs.any():
        # A side other than --orn-side comes from --by-side, which needs ORNs on both antennae.
        where = f" on the {side} antenna side (needed by --by-side)" if side and side != orn.side else ""
        raise SystemExit(f"no {names[1]} match {orn_description(orn)}{where}")
    return inputs


def orn_description(orn: OrnSeedSpec) -> str:
    parts = [f"--orn-family {orn.family}" if orn.family else f"--orn-glomerulus {orn.glomerulus}"]
    if orn.side:
        parts.append(f"--orn-side {orn.side}")
    return " ".join(parts)


def report_seeds(context: QueryContext, selection: Selection) -> None:
    positive = int((context.seeds > 0).sum())
    seed_text = (
        f"seed = input fraction from {orn_description(context.orn)} ORNs"
        if context.orn.active
        else "seed = 1 per source neuron"
    )
    print(
        f"{context.command}: {int(context.sources.sum())} source neurons ({selection.description}); "
        f"{positive} with seed > 0; {seed_text}; {len(context.targets.rows)} target neurons",
        file=sys.stderr,
    )
    renamed = {new: old for old, new in hemibrain_glomerulus_names().items()}
    if context.orn_inputs is not None and context.orn.glomerulus in renamed:
        print(
            f"{context.command}: hemibrain glomerulus {context.orn.glomerulus} is v1.2 type "
            f"ORN_{renamed[context.orn.glomerulus]} (renamed by Schlegel et al. 2021)",
            file=sys.stderr,
        )
    if positive == 0:
        raise SystemExit("no source neuron has a positive seed weight; check --orn-* options")


def route_edges(context: QueryContext, route: str, keep_pre: np.ndarray | None = None) -> EdgeTable:
    return first_hop_edges(context.query, route, context.sources, context.kenyon, keep_pre)


def signed_filter(context: QueryContext, signed: bool) -> np.ndarray | None:
    """Presynaptic neurons that a ``--signed`` search may use, or None without ``--signed``."""
    if not signed:
        return None
    unsigned = int((context.sources & (context.signs == 0)).sum())
    if unsigned:
        print(
            f"{context.command}: --signed excludes {unsigned} source neurons without a fast-transmitter sign",
            file=sys.stderr,
        )
    return context.signs != 0


def report_missing(context: QueryContext, missing: int, max_hops: int, unit: str) -> None:
    notes = []
    if missing:
        notes.append(f"no path within {max_hops} hops for {missing} {unit}")
    absent = int((context.target_nodes < 0).sum())
    if absent:
        notes.append(f"{absent} target neurons have no connections in the {context.dataset} graph")
    if notes:
        print(f"{context.command}: {'; '.join(notes)}", file=sys.stderr)


def path_rows(context: QueryContext, routes: list[str], max_hops: int, top: int, signed: bool) -> list[dict[str, str]]:
    keep_pre = signed_filter(context, signed)
    relay = relay_edges(context.query, context.sources, keep_pre)
    conflicts = context.labels.sign_conflict_types()
    targets = sorted(
        {int(node) for node in context.target_nodes if node >= 0},
        key=lambda node: (context.display[node], context.sides[node], int(context.graph.ids[node])),
    )
    rows, missing = [], 0
    for route in routes:
        first = route_edges(context, route, keep_pre)
        search = search_paths(first, relay, context.seeds, max_hops)
        for target in targets:
            found = ranked_paths(search, target, max_hops, top)
            if not found:
                missing += 1
            hops = shortest_hops(search, target)
            for rank, (cost, path) in enumerate(found, start=1):
                rows.append(path_row(context, route, rank, cost, path, hops, first, relay, conflicts))
    report_missing(context, missing, max_hops, "target/route combinations")
    return rows


def type_path_rows(
    context: QueryContext, routes: list[str], max_hops: int, top: int, signed: bool
) -> list[dict[str, str]]:
    """Strongest cell-type routes into each target type (``paths --by-type``)."""
    keep_pre = signed_filter(context, signed)
    relay = relay_edges(context.query, context.sources, keep_pre)
    names, type_ids = np.unique(context.display.astype(str), return_inverse=True)
    side_codes = np.select([context.sides == "left", context.sides == "right"], [LEFT, RIGHT], OTHER)
    signs = context.signs.astype(np.float64)
    target_nodes = np.unique(context.target_nodes[context.target_nodes >= 0])
    rows, missing = [], 0
    for route in routes:
        first = route_edges(context, route, keep_pre)
        graph = RouteGraph.build(first, relay, context.graph.size)
        for target_type in np.unique(type_ids[target_nodes]):
            targets = np.zeros(context.graph.size, dtype=bool)
            targets[target_nodes[type_ids[target_nodes] == target_type]] = True
            found, total = strongest_type_routes(
                graph, context.seeds, type_ids, side_codes, signs, targets, max_hops, top
            )
            if not found:
                missing += 1
            for rank, item in enumerate(found, start=1):
                sided = item.ipsi + item.contra
                synapses = route_synapses(item.types, graph, context.seeds, type_ids, targets)
                rows.append({
                    "dataset": context.dataset,
                    "route": route,
                    "target_type": str(names[target_type]),
                    "rank": str(rank),
                    "hops": str(len(item.types) - 1),
                    "path_types": " > ".join(str(names[type_id]) for type_id in item.types),
                    "strength": f"{item.strength:.6g}",
                    "share": f"{item.strength / total:.6g}",
                    "ipsi_share": f"{item.ipsi / sided:.6g}" if sided > 0 else "",
                    "signed_strength": f"{item.signed:.6g}",
                    "step_synapses": " > ".join(str(count) for count in synapses),
                })
    report_missing(context, missing, max_hops, "target type/route combinations")
    return rows


def path_row(context, route, rank, cost, path, hops, first: EdgeTable, relay: EdgeTable, conflicts) -> dict[str, str]:
    ids = context.graph.ids
    steps = []
    for index, (pre, post) in enumerate(zip(path[:-1], path[1:])):
        table = first if index == 0 else relay
        edge = table.find(pre, post)
        steps.append((int(table.synapses[edge]), float(table.weight[edge])))
    source, target = path[0], path[-1]
    strength = float(np.exp(-cost))
    sign = int(np.prod([int(context.signs[node]) for node in path[:-1]]))
    return {
        "dataset": context.dataset,
        "route": route,
        "target_id": str(int(ids[target])),
        "target_type": context.display[target],
        "target_side": context.sides[target],
        "rank": str(rank),
        "source_id": str(int(ids[source])),
        "source_type": context.display[source],
        "source_side": context.sides[source],
        "relation": side_relation(context.sides[source], context.sides[target]),
        "hops": str(len(path) - 1),
        "shortest_hops": "" if hops is None else str(hops),
        "strength": f"{strength:.6g}",
        "seed_weight": f"{context.seeds[source]:.6g}",
        "path_types": " > ".join(context.display[node] for node in path),
        "path_ids": " > ".join(str(int(ids[node])) for node in path),
        "path_sides": " > ".join(context.sides[node] or "?" for node in path),
        "step_synapses": " > ".join(str(synapses) for synapses, _ in steps),
        "step_weights": " > ".join(f"{weight:.6g}" for _, weight in steps),
        "sign": str(sign),
        "signed_strength": f"{sign * strength:.6g}",
        "transmitters": " > ".join(context.transmitters[node] or "?" for node in path),
        "sign_conflict_types": ";".join(
            sorted({context.display[node] for node in path[:-1] if context.display[node] in conflicts})
        ),
    }


def side_relation(source: str, target: str) -> str:
    if source not in SIDES or target not in SIDES:
        return "unknown"
    return "ipsi" if source == target else "contra"


def reach_rows(
    context: QueryContext,
    routes: list[str],
    hops: list[int],
    *,
    per_neuron: bool,
    by_side: bool,
) -> list[dict[str, str]]:
    absent = int((context.target_nodes < 0).sum())
    if absent:
        print(
            f"{context.command}: {absent} target neurons have no connections in the {context.dataset} graph; "
            "their reach is 0",
            file=sys.stderr,
        )
    relay = relay_edges(context.query, context.sources)
    side_seeds = {side: seed_vector(context, context.orn, side) for side in SIDES} if by_side else {}
    rows = []
    for route in routes:
        first = route_edges(context, route)
        vectors = propagate(first, relay, context.seeds, max(hops))
        sided = {
            (side, signed): propagate(first, relay, seeds, max(hops), context.signs if signed else None)
            for side, seeds in side_seeds.items()
            for signed in (False, True)
        }
        for hop in hops:
            values = target_values(context, vectors[hop - 1])
            side_values = {key: target_values(context, vector[hop - 1]) for key, vector in sided.items()}
            builder = neuron_reach_rows if per_neuron else type_reach_rows
            rows.extend(builder(context, route, hop, values, side_values))
    order = {route: index for index, route in enumerate(ROUTES)}
    rows.sort(key=lambda row: (order[row["route"]], int(row["hop"]), int(row["rank"]), row["target_type"]))
    return rows


def target_values(context: QueryContext, vector: np.ndarray) -> np.ndarray:
    """Reach value per selected target neuron; neurons without connections get 0."""
    nodes = context.target_nodes
    return np.where(nodes >= 0, vector[nodes.clip(min=0)], 0.0)


def neuron_reach_rows(context, route, hop, values, side_values) -> list[dict[str, str]]:
    labels, rows = context.labels, context.targets.rows
    ranks = competition_ranks(values)
    out = []
    for index, row in enumerate(rows):
        item = {
            "dataset": context.dataset,
            "route": route,
            "hop": str(hop),
            "target_id": str(int(labels.ids[row])),
            "target_type": context.row_display[row],
            "target_side": labels.field("side")[row],
            "reach": f"{values[index]:.6g}",
            "rank": str(int(ranks[index])),
            "rank_of": str(len(rows)),
        }
        if side_values:
            side = labels.field("side")[row]
            other = {"left": "right", "right": "left"}.get(side)
            ipsi = side_values[(side, False)][index] if other else np.nan
            contra = side_values[(other, False)][index] if other else np.nan
            signed_ipsi = side_values[(side, True)][index] if other else np.nan
            signed_contra = side_values[(other, True)][index] if other else np.nan
            item.update(side_columns(ipsi, contra, signed_ipsi, signed_contra))
        out.append(item)
    return out


def type_reach_rows(context, route, hop, values, side_values) -> list[dict[str, str]]:
    labels, rows = context.labels, context.targets.rows
    types = context.row_display[rows]
    sides = labels.field("side")[rows]
    names, inverse = np.unique(types.astype(str), return_inverse=True)
    size = len(names)
    counts = np.bincount(inverse, minlength=size)
    means = np.bincount(inverse, weights=values, minlength=size) / counts
    ranks = competition_ranks(means)
    side_counts = {side: np.bincount(inverse[sides == side], minlength=size) for side in SIDES}

    def side_mean(key: tuple[str, bool], side: str) -> np.ndarray:
        """Mean over the type's neurons on ``side`` of reach from ``key`` seeds; NaN without neurons."""
        mask = sides == side
        total = np.bincount(inverse[mask], weights=side_values[key][mask], minlength=size)
        return np.divide(total, side_counts[side], out=np.full(size, np.nan), where=side_counts[side] > 0)

    if side_values:
        ipsi = side_mean(("left", False), "left") + side_mean(("right", False), "right")
        contra = side_mean(("left", False), "right") + side_mean(("right", False), "left")
        signed_ipsi = side_mean(("left", True), "left") + side_mean(("right", True), "right")
        signed_contra = side_mean(("left", True), "right") + side_mean(("right", True), "left")
    out = []
    for index, name in enumerate(names.tolist()):
        item = {
            "dataset": context.dataset,
            "route": route,
            "hop": str(hop),
            "target_type": name,
            "neurons": str(int(counts[index])),
            "reach": f"{means[index]:.6g}",
            "rank": str(int(ranks[index])),
            "rank_of": str(size),
        }
        if side_values:
            item["left_neurons"] = str(int(side_counts["left"][index]))
            item["right_neurons"] = str(int(side_counts["right"][index]))
            item.update(side_columns(ipsi[index], contra[index], signed_ipsi[index], signed_contra[index]))
        out.append(item)
    return out


def side_columns(ipsi: float, contra: float, signed_ipsi: float, signed_contra: float) -> dict[str, str]:
    def fmt(value: float) -> str:
        return "" if not np.isfinite(value) else f"{value:.6g}"

    total = ipsi + contra
    ai = (ipsi - contra) / total if np.isfinite(total) and total > 0 else np.nan
    return {
        "ipsi": fmt(ipsi),
        "contra": fmt(contra),
        "ai": fmt(ai),
        "signed_ipsi": fmt(signed_ipsi),
        "signed_contra": fmt(signed_contra),
        "signed_net": fmt(signed_ipsi - signed_contra),
    }
