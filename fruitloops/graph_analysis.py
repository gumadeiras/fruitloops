"""Strongest paths, fewest-hop paths, and hop-k reach on a cached graph.

Definitions (see docs/paths.md):

- Pair synapses are summed over neuropils. A directed edge is kept when its pair
  synapses reach ``min_synapses``.
- Edge weight = pair synapses / all input synapses of the postsynaptic neuron.
  The denominator counts every presynaptic partner, with no threshold.
- Source neurons contribute out-edges only on the first hop, so no path passes
  through a source neuron after the first hop.
- A route restricts the first hop. ``AL``, ``LH``, ``MB`` and ``other`` use only
  the first-hop synapses in that neuropil class for the weight; ``kc`` keeps
  first-hop edges whose postsynaptic neuron is a Kenyon cell; ``all`` uses every
  first-hop synapse.
- Strongest path = maximum product of weights (times the seed weight), found as
  the minimum sum of -log(weight) within ``max_hops`` hops.
- Reach at hop k = sum over all length-k paths of the product of weights:
  ``v_1 = F^T v_0`` with the route-restricted first hop ``F``, then
  ``v_k = W0^T v_(k-1)`` with source out-edges removed from ``W0``.
"""

from __future__ import annotations

import heapq
import itertools
from dataclasses import dataclass

import numpy as np
from scipy import sparse

from .graph_cache import GRAPH_REGIONS, ConnectomeGraph

ROUTES = ("all", "AL", "LH", "MB", "other", "kc")


@dataclass
class EdgeTable:
    """Directed edges sorted by postsynaptic node."""

    src: np.ndarray
    dst: np.ndarray
    weight: np.ndarray
    synapses: np.ndarray
    cost: np.ndarray
    starts: np.ndarray
    targets: np.ndarray

    @classmethod
    def build(cls, src: np.ndarray, dst: np.ndarray, weight: np.ndarray, synapses: np.ndarray) -> "EdgeTable":
        order = np.lexsort((src, dst))
        src = np.asarray(src, dtype=np.int64)[order]
        dst = np.asarray(dst, dtype=np.int64)[order]
        weight = np.asarray(weight, dtype=np.float64)[order]
        targets, starts = np.unique(dst, return_index=True)
        return cls(
            src=src,
            dst=dst,
            weight=weight,
            synapses=np.asarray(synapses, dtype=np.int64)[order],
            cost=-np.log(weight),
            starts=starts,
            targets=targets,
        )

    def into(self, node: int) -> slice:
        return slice(
            int(np.searchsorted(self.dst, node, side="left")),
            int(np.searchsorted(self.dst, node, side="right")),
        )

    def find(self, src: int, dst: int) -> int:
        block = self.into(dst)
        hits = np.flatnonzero(self.src[block] == src)
        return block.start + int(hits[0]) if len(hits) else -1

    def without_source(self, node: int) -> "EdgeTable":
        """The same edges minus those whose presynaptic neuron is ``node``."""
        keep = self.src != node
        return EdgeTable.build(self.src[keep], self.dst[keep], self.weight[keep], self.synapses[keep])

    def matrix(self, size: int, sign: np.ndarray | None = None) -> sparse.csr_matrix:
        weight = self.weight if sign is None else self.weight * sign[self.src]
        return sparse.csr_matrix((weight, (self.src, self.dst)), shape=(size, size))


@dataclass
class QueryGraph:
    graph: ConnectomeGraph
    kept: np.ndarray
    weight: np.ndarray


def threshold_graph(graph: ConnectomeGraph, min_synapses: int) -> QueryGraph:
    kept = np.flatnonzero(graph.synapses >= min_synapses)
    weight = graph.synapses[kept] / graph.input_synapses[graph.post[kept]]
    return QueryGraph(graph=graph, kept=kept, weight=weight)


def relay_edges(query: QueryGraph, sources: np.ndarray, keep_pre: np.ndarray | None = None) -> EdgeTable:
    """Kept edges whose presynaptic neuron is not a source (``W0``)."""
    graph = query.graph
    pre = graph.pre[query.kept]
    mask = ~sources[pre]
    if keep_pre is not None:
        mask &= keep_pre[pre]
    pairs = query.kept[mask]
    return EdgeTable.build(graph.pre[pairs], graph.post[pairs], query.weight[mask], graph.synapses[pairs])


def first_hop_edges(
    query: QueryGraph,
    route: str,
    sources: np.ndarray,
    kenyon_cells: np.ndarray,
    keep_pre: np.ndarray | None = None,
) -> EdgeTable:
    """Kept source out-edges, with synapses restricted to the route's neuropils."""
    graph = query.graph
    mask = sources[graph.pre[query.kept]]
    if keep_pre is not None:
        mask &= keep_pre[graph.pre[query.kept]]
    pairs = query.kept[mask]
    synapses = route_synapses(graph, pairs, route)
    if route == "kc":
        synapses = np.where(kenyon_cells[graph.post[pairs]], synapses, 0)
    keep = synapses > 0
    pairs, synapses = pairs[keep], synapses[keep]
    weight = synapses / graph.input_synapses[graph.post[pairs]]
    return EdgeTable.build(graph.pre[pairs], graph.post[pairs], weight, synapses)


def route_synapses(graph: ConnectomeGraph, pairs: np.ndarray, route: str) -> np.ndarray:
    if route in ("all", "kc"):
        return graph.synapses[pairs].astype(np.int64)
    if route in GRAPH_REGIONS:
        return region_synapses(graph, pairs, route)
    if route == "other":
        other = graph.synapses[pairs].astype(np.int64)
        for region in GRAPH_REGIONS:
            other -= region_synapses(graph, pairs, region)
        return other
    raise ValueError(f"unknown route {route}; choose from {', '.join(ROUTES)}")


def region_synapses(graph: ConnectomeGraph, pairs: np.ndarray, region: str) -> np.ndarray:
    region_pairs = graph.region_pairs[region]
    out = np.zeros(len(pairs), dtype=np.int64)
    if not len(region_pairs):
        return out
    slots = np.searchsorted(region_pairs, pairs).clip(max=len(region_pairs) - 1)
    hit = region_pairs[slots] == pairs
    out[hit] = graph.region_synapses[region][slots[hit]]
    return out


def input_fraction_seeds(graph: ConnectomeGraph, sources: np.ndarray, inputs: np.ndarray) -> np.ndarray:
    """Seed(source) = synapses from ``inputs`` onto it / all its input synapses (no threshold)."""
    pairs = inputs[graph.pre] & sources[graph.post]
    synapses = np.bincount(graph.post[pairs], weights=graph.synapses[pairs], minlength=graph.size)
    totals = graph.input_synapses.astype(np.float64)
    return np.divide(synapses, totals, out=np.zeros(graph.size), where=totals > 0)


@dataclass
class PathSearch:
    seeds: np.ndarray
    seed_cost: np.ndarray
    first: EdgeTable
    relay: EdgeTable
    costs: list[np.ndarray]
    preds: list[np.ndarray]

    @property
    def levels(self) -> int:
        return len(self.costs) - 1


def relax(previous: np.ndarray, edges: EdgeTable, size: int) -> tuple[np.ndarray, np.ndarray]:
    best = np.full(size, np.inf)
    pred = np.full(size, -1, dtype=np.int64)
    if not len(edges.src):
        return best, pred
    candidate = previous[edges.src] + edges.cost
    best[edges.targets] = np.minimum.reduceat(candidate, edges.starts)
    winners = np.flatnonzero((candidate == best[edges.dst]) & np.isfinite(candidate))
    nodes, first = np.unique(edges.dst[winners], return_index=True)
    pred[nodes] = edges.src[winners[first]]
    return best, pred


def search_paths(first: EdgeTable, relay: EdgeTable, seeds: np.ndarray, max_hops: int) -> PathSearch:
    """Hop-limited min-sum search; ``costs[k]`` is the best cost within k hops."""
    size = len(seeds)
    seed_cost = np.full(size, np.inf)
    positive = seeds > 0
    seed_cost[positive] = -np.log(seeds[positive])
    best, pred = relax(seed_cost, first, size)
    costs, preds = [np.full(size, np.inf), best], [np.full(size, -1, dtype=np.int64), pred]
    for _ in range(2, max_hops + 1):
        candidate, candidate_pred = relax(costs[-1], relay, size)
        improved = candidate < costs[-1]
        if not improved.any():
            break
        costs.append(np.where(improved, candidate, costs[-1]))
        preds.append(np.where(improved, candidate_pred, -1))
    return PathSearch(seeds=seeds, seed_cost=seed_cost, first=first, relay=relay, costs=costs, preds=preds)


def trace(search: PathSearch, node: int, level: int) -> list[int]:
    level = min(level, search.levels)
    path = [node]
    while level >= 1:
        pred = int(search.preds[level][node])
        if pred >= 0:
            path.append(pred)
            node = pred
        level -= 1
    return path[::-1]


def shortest_hops(search: PathSearch, node: int) -> int | None:
    for level in range(1, search.levels + 1):
        if np.isfinite(search.costs[level][node]):
            return level
    return None


def ranked_paths(search: PathSearch, target: int, max_hops: int, top: int) -> list[tuple[float, list[int]]]:
    """Strongest path, then the strongest path through each other last presynaptic neuron.

    Rank 1 is the strongest path within ``max_hops``. Each later rank is the
    strongest path that reaches the target through a different presynaptic
    neuron. No path passes through the target before its last step: when the
    best path to a presynaptic neuron does, that neuron's best path that avoids
    the target is used instead.
    """
    level = min(max_hops - 1, search.levels)
    order = itertools.count()
    queue: list[tuple[float, int, int, float, list[int] | None]] = []
    block = search.first.into(target)
    for edge in range(block.start, block.stop):
        node = int(search.first.src[edge])
        cost = float(search.seed_cost[node] + search.first.cost[edge])
        if np.isfinite(cost) and node != target:
            queue.append((cost, node, next(order), 0.0, [node]))
    if max_hops >= 2:
        block = search.relay.into(target)
        for node, edge_cost in zip(search.relay.src[block].tolist(), search.relay.cost[block].tolist()):
            cost = float(search.costs[level][node] + edge_cost)
            if np.isfinite(cost) and node != target:
                queue.append((cost, node, next(order), edge_cost, None))
    heapq.heapify(queue)
    avoiding: PathSearch | None = None
    out = []
    while queue and len(out) < top:
        cost, node, _, edge_cost, prefix = heapq.heappop(queue)
        if prefix is None:
            prefix = trace(search, node, level)
            if target in prefix:
                # Avoiding the target can only cost more, so the entry goes back in order.
                avoiding = avoiding or search_avoiding(search, target, level)
                cost = float(avoiding.costs[-1][node] + edge_cost)
                if np.isfinite(cost):
                    heapq.heappush(queue, (cost, node, next(order), edge_cost, trace(avoiding, node, level)))
                continue
        out.append((cost, prefix + [target]))
    return out


def search_avoiding(search: PathSearch, target: int, max_hops: int) -> PathSearch:
    """The same search with ``target`` removed as a source and as a relay."""
    seeds = search.seeds.copy()
    seeds[target] = 0.0
    return search_paths(search.first, search.relay.without_source(target), seeds, max_hops)


def propagate(
    first: EdgeTable,
    relay: EdgeTable,
    seeds: np.ndarray,
    max_hop: int,
    sign: np.ndarray | None = None,
) -> list[np.ndarray]:
    """Reach vectors ``[v_1, ..., v_max_hop]``."""
    size = len(seeds)
    vector = first.matrix(size, sign).T @ seeds
    out = [vector]
    relay_t = relay.matrix(size, sign).T.tocsr()
    for _ in range(2, max_hop + 1):
        vector = relay_t @ vector
        out.append(vector)
    return out


def competition_ranks(values: np.ndarray) -> np.ndarray:
    """Rank 1 = largest value; ties share the best rank."""
    descending = -np.sort(-values)
    return np.searchsorted(-descending, -values, side="left") + 1
