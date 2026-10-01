"""Paths grouped by cell-type sequence (``paths --by-type``).

A type route is a sequence of cell types from a source to a target. Its
strength is the sum, over every length-k path whose neurons have those types,
of the product of edge weights times the source's seed weight. Paths may revisit
neurons, as in ``reach``, so the strengths of all type routes of length k add up
to the hop-k reach into the targets.

The strongest routes are found exactly by a best-first search. A partial route
holds the weight of its paths at each neuron of its last type. Backward reach
vectors give the summed strength of all its completions exactly, so a partial
route is expanded only while it can still beat the routes already found.
"""

from __future__ import annotations

import heapq
import itertools
from dataclasses import dataclass

import numpy as np
from scipy import sparse

from .graph_analysis import EdgeTable

LEFT, RIGHT, OTHER = 0, 1, 2
SIGNED = 3


@dataclass(frozen=True)
class TypeRoute:
    types: tuple[int, ...]
    strength: float
    ipsi: float
    contra: float
    signed: float


@dataclass(frozen=True)
class RouteGraph:
    """First-hop and relay edges of one route, as matrices the search reuses for every target type."""

    first: EdgeTable
    relay: EdgeTable
    first_weights: sparse.csr_matrix
    relay_weights: sparse.csr_matrix
    edge_index: tuple[sparse.csr_matrix, sparse.csr_matrix]

    @classmethod
    def build(cls, first: EdgeTable, relay: EdgeTable, size: int) -> "RouteGraph":
        return cls(
            first=first,
            relay=relay,
            first_weights=first.matrix(size),
            relay_weights=relay.matrix(size),
            edge_index=(edge_index_matrix(first, size), edge_index_matrix(relay, size)),
        )


@dataclass(frozen=True)
class PartialRoute:
    types: tuple[int, ...]
    nodes: np.ndarray
    # Per node: path weight from left, right, and other-side sources, then the signed weight.
    weight: np.ndarray


def strongest_type_routes(
    graph: RouteGraph,
    seeds: np.ndarray,
    type_ids: np.ndarray,
    side_codes: np.ndarray,
    signs: np.ndarray,
    targets: np.ndarray,
    max_hops: int,
    top: int,
) -> tuple[list[TypeRoute], float]:
    """Strongest type routes into ``targets`` within ``max_hops``, and the strength of all routes.

    ``side_codes`` holds LEFT, RIGHT, or OTHER per node; ``targets`` marks the
    target neurons, which share one type.
    """
    size = len(seeds)
    tables = (graph.first, graph.relay)
    # remaining[r][n]: summed weight of every path of 1..r relay edges from n into the targets.
    arriving = targets.astype(np.float64)
    remaining = [np.zeros(size)]
    for _ in range(max_hops):
        arriving = graph.relay_weights @ arriving
        remaining.append(remaining[-1] + arriving)
    start = graph.first_weights @ (targets + remaining[max_hops - 1])
    total = float(seeds @ start)

    order = itertools.count()
    queue: list[tuple[float, int, TypeRoute | PartialRoute]] = []
    best: list[float] = []

    def threshold() -> float:
        return best[0] if len(best) >= top else 0.0

    def push(priority: float, item: TypeRoute | PartialRoute) -> None:
        if priority > 0 and priority >= threshold():
            heapq.heappush(queue, (-priority, next(order), item))

    sources = np.flatnonzero(seeds > 0)
    for type_id in np.unique(type_ids[sources]):
        nodes = sources[type_ids[sources] == type_id]
        weight = np.zeros((len(nodes), 4))
        weight[np.arange(len(nodes)), side_codes[nodes]] = seeds[nodes]
        weight[:, SIGNED] = seeds[nodes]
        push(float(seeds[nodes] @ start[nodes]), PartialRoute((int(type_id),), nodes, weight))

    routes: list[TypeRoute] = []
    while queue and len(routes) < top:
        _, _, item = heapq.heappop(queue)
        if isinstance(item, TypeRoute):
            routes.append(item)
            continue
        step = 0 if len(item.types) == 1 else 1
        nodes, weight = advance(item, tables[step], graph.edge_index[step], signs)
        node_types = type_ids[nodes]
        reached = targets[nodes]
        if reached.any():
            route = completed_route(item.types + (int(node_types[reached][0]),), weight[reached], side_codes[nodes[reached]])
            push(route.strength, route)
            if route.strength > 0:
                heapq.heappush(best, route.strength)
                if len(best) > top:
                    heapq.heappop(best)
        hops_left = max_hops - len(item.types)
        if hops_left <= 0:
            continue
        ahead = remaining[hops_left][nodes]
        onward = ahead > 0
        bounds = np.bincount(node_types[onward], weights=weight[onward, :SIGNED].sum(axis=1) * ahead[onward])
        for type_id in np.flatnonzero(bounds > 0):
            chosen = onward & (node_types == type_id)
            push(float(bounds[type_id]), PartialRoute(item.types + (int(type_id),), nodes[chosen], weight[chosen]))
    return routes, total


def edge_index_matrix(table: EdgeTable, size: int) -> sparse.csr_matrix:
    """Sparse matrix whose entry (pre, post) is 1 + the edge's index in ``table``."""
    index = np.arange(1, len(table.src) + 1, dtype=np.float64)
    return sparse.csr_matrix((index, (table.src, table.dst)), shape=(size, size))


def advance(item: PartialRoute, table: EdgeTable, edge_index: sparse.csr_matrix, signs: np.ndarray):
    """Move the partial route's path weights one edge forward; return the reached nodes and weights."""
    rows = edge_index[item.nodes].tocoo()
    edges = rows.data.astype(np.int64) - 1
    moving = item.weight[rows.row] * table.weight[edges, None]
    moving[:, SIGNED] *= signs[item.nodes[rows.row]]
    nodes, slot = np.unique(table.dst[edges], return_inverse=True)
    weight = np.column_stack([np.bincount(slot, weights=moving[:, column], minlength=len(nodes)) for column in range(4)])
    return nodes, weight


def completed_route(types: tuple[int, ...], weight: np.ndarray, target_sides: np.ndarray) -> TypeRoute:
    left, right = target_sides == LEFT, target_sides == RIGHT
    return TypeRoute(
        types=types,
        strength=float(weight[:, :SIGNED].sum()),
        ipsi=float(weight[left, LEFT].sum() + weight[right, RIGHT].sum()),
        contra=float(weight[left, RIGHT].sum() + weight[right, LEFT].sum()),
        signed=float(weight[:, SIGNED].sum()),
    )


def route_synapses(
    types: tuple[int, ...],
    graph: RouteGraph,
    seeds: np.ndarray,
    type_ids: np.ndarray,
    targets: np.ndarray,
) -> list[int]:
    """Synapses at each step between the neurons that the route's paths pass through."""
    tables = (graph.first, graph.relay)
    nodes = np.flatnonzero((seeds > 0) & (type_ids == types[0]))
    steps = []
    for position, type_id in enumerate(types[1:]):
        step = 0 if position == 0 else 1
        table = tables[step]
        edges = graph.edge_index[step][nodes].data.astype(np.int64) - 1
        edges = edges[type_ids[table.dst[edges]] == type_id]
        steps.append((table, edges))
        nodes = np.unique(table.dst[edges])
    # Walk back from the targets, keeping only edges whose paths reach them.
    on_route = targets
    counts = []
    for table, edges in reversed(steps):
        edges = edges[on_route[table.dst[edges]]]
        counts.append(int(table.synapses[edges].sum()))
        on_route = np.zeros(len(seeds), dtype=bool)
        on_route[table.src[edges]] = True
    return counts[::-1]
