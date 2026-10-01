"""Cached sparse connectivity graph per dataset.

The cache stores every connected neuron pair with its synapses summed over
neuropils, the synapses of each pair in the AL, LH, and MB neuropil classes,
and the total input synapses of every neuron (all partners, no threshold).
Synapse thresholds are applied at query time, so one cache serves every
``--min-synapses`` value.
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from .bulk import setup_row
from .duckdb_store import require_duckdb, safe_identifier, table_exists
from .olfaction import CONNECTION_SPECS, roi_region_sql
from .setup_state import setup_state_matches, sha256_json, table_fingerprint, write_setup_state

GRAPH_SCHEMA_VERSION = "1"
GRAPH_REGIONS = ("AL", "LH", "MB")
GRAPH_DATASETS = tuple(CONNECTION_SPECS)


@dataclass
class ConnectomeGraph:
    dataset: str
    ids: np.ndarray
    pre: np.ndarray
    post: np.ndarray
    synapses: np.ndarray
    region_pairs: dict[str, np.ndarray]
    region_synapses: dict[str, np.ndarray]
    input_synapses: np.ndarray
    meta: dict

    @property
    def size(self) -> int:
        return len(self.ids)

    def nodes_for_ids(self, ids: np.ndarray) -> np.ndarray:
        """Graph node for each id, or -1 when the neuron has no connections."""
        ids = np.asarray(ids, dtype=np.int64)
        if not len(self.ids):
            return np.full(len(ids), -1, dtype=np.int64)
        nodes = np.searchsorted(self.ids, ids).clip(max=len(self.ids) - 1)
        return np.where(self.ids[nodes] == ids, nodes, -1)


def graph_cache_path(store: Path, dataset: str) -> Path:
    return store.parent / f"{store.stem}.graphs" / f"{dataset}.npz"


def graph_source_fingerprint(connection, dataset: str) -> str:
    spec = CONNECTION_SPECS[dataset]
    return sha256_json(
        {
            "schema_version": GRAPH_SCHEMA_VERSION,
            "dataset": dataset,
            "table": spec.table,
            "source": table_fingerprint(connection, spec.table),
        }
    )


def build_graph_cache(store: Path, dataset: str, *, skip_current: bool = False) -> dict[str, str]:
    duckdb = require_duckdb("graph build")
    spec = CONNECTION_SPECS[dataset]
    path = graph_cache_path(store, dataset)
    stage_key = f"graph:{dataset}"
    store.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(store)) as connection:
        if not table_exists(connection, spec.table):
            return setup_row(dataset, "graph", dataset, f"missing:{spec.table}", path, store)
        fingerprint = graph_source_fingerprint(connection, dataset)
        meta = read_graph_meta(path)
        if (
            skip_current
            and meta.get("fingerprint") == fingerprint
            and setup_state_matches(connection, stage_key, fingerprint)
        ):
            return setup_row(dataset, "graph", dataset, f"current:{meta.get('pairs', '')}", path, store)
        started = time.perf_counter()
        arrays = aggregate_pairs(connection, dataset)
        meta = {
            "schema_version": GRAPH_SCHEMA_VERSION,
            "dataset": dataset,
            "source_table": spec.table,
            "fingerprint": fingerprint,
            "nodes": int(len(arrays["ids"])),
            "pairs": int(len(arrays["pre"])),
            "built_at": datetime.now(timezone.utc).isoformat(),
        }
        meta["build_seconds"] = round(time.perf_counter() - started, 3)
        write_graph_file(path, arrays, meta)
        write_setup_state(connection, stage_key, fingerprint, str(meta["pairs"]))
    return setup_row(dataset, "graph", dataset, str(meta["pairs"]), path, store)


def aggregate_pairs(connection, dataset: str) -> dict[str, np.ndarray]:
    spec = CONNECTION_SPECS[dataset]
    syn = f"CAST({safe_identifier(spec.weight_column)} AS BIGINT)"
    region_columns = ",\n".join(
        f"sum(CASE WHEN region = '{region}' THEN syn ELSE 0 END) AS syn_{region}" for region in GRAPH_REGIONS
    )
    result = connection.execute(
        f"""
        WITH rows AS (
            SELECT CAST({safe_identifier(spec.pre_column)} AS BIGINT) AS pre,
                   CAST({safe_identifier(spec.post_column)} AS BIGINT) AS post,
                   {syn} AS syn,
                   {roi_region_sql(safe_identifier(spec.roi_column))} AS region
            FROM {safe_identifier(spec.table)}
            WHERE {syn} > 0
        )
        SELECT pre, post, sum(syn) AS synapses,
               {region_columns}
        FROM rows
        GROUP BY pre, post
        """
    ).fetchnumpy()
    pre_ids = np.asarray(result["pre"], dtype=np.int64)
    post_ids = np.asarray(result["post"], dtype=np.int64)
    ids = np.unique(np.concatenate([pre_ids, post_ids]))
    pre = np.searchsorted(ids, pre_ids).astype(np.int32)
    post = np.searchsorted(ids, post_ids).astype(np.int32)
    synapses = np.asarray(result["synapses"], dtype=np.int64)
    arrays = {
        "ids": ids,
        "pre": pre,
        "post": post,
        "synapses": synapses.astype(np.int32),
        "input_synapses": np.bincount(post, weights=synapses, minlength=len(ids)).astype(np.int64),
    }
    for region in GRAPH_REGIONS:
        region_syn = np.asarray(result[f"syn_{region}"], dtype=np.int64)
        pairs = np.flatnonzero(region_syn > 0)
        arrays[f"{region}_pairs"] = pairs.astype(np.int64)
        arrays[f"{region}_synapses"] = region_syn[pairs].astype(np.int32)
    return arrays


def write_graph_file(path: Path, arrays: dict[str, np.ndarray], meta: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.stem}.tmp.npz")
    np.savez(tmp, meta=np.array(json.dumps(meta, sort_keys=True)), **arrays)
    os.replace(tmp, path)


def read_graph_meta(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with np.load(path, allow_pickle=False) as data:
            return json.loads(str(data["meta"]))
    except (OSError, KeyError, ValueError):
        return {}


def current_source_fingerprint(store: Path, dataset: str) -> str:
    """Fingerprint of the dataset's connection table, or '' when it is missing."""
    if not store.exists():
        return ""
    duckdb = require_duckdb("graph status")
    with duckdb.connect(str(store), read_only=True) as connection:
        if not table_exists(connection, CONNECTION_SPECS[dataset].table):
            return ""
        return graph_source_fingerprint(connection, dataset)


def load_graph(store: Path, dataset: str, *, command: str = "fruitloops") -> ConnectomeGraph:
    """Load the graph cache, building it first when it is missing or stale."""
    spec = CONNECTION_SPECS[dataset]
    fingerprint = current_source_fingerprint(store, dataset)
    if not fingerprint:
        raise SystemExit(f"missing {spec.table} in {store}; run `fruitloops setup --{dataset}`")
    path = graph_cache_path(store, dataset)
    meta = read_graph_meta(path)
    if meta.get("fingerprint") != fingerprint:
        reason = "stale" if meta else "missing"
        print(f"{command}: {dataset} graph cache is {reason}; building {path}", file=sys.stderr)
        build_graph_cache(store, dataset)
    with np.load(path, allow_pickle=False) as data:
        meta = json.loads(str(data["meta"]))
        return ConnectomeGraph(
            dataset=dataset,
            ids=data["ids"],
            pre=data["pre"],
            post=data["post"],
            synapses=data["synapses"],
            region_pairs={region: data[f"{region}_pairs"] for region in GRAPH_REGIONS},
            region_synapses={region: data[f"{region}_synapses"] for region in GRAPH_REGIONS},
            input_synapses=data["input_synapses"],
            meta=meta,
        )


def graph_status(store: Path, dataset: str) -> dict[str, str]:
    path = graph_cache_path(store, dataset)
    meta = read_graph_meta(path)
    if not meta:
        return {"name": dataset, "value": "missing", "path": str(path)}
    state = "current" if meta.get("fingerprint") == current_source_fingerprint(store, dataset) else "stale"
    return {
        "name": dataset,
        "value": f"{state}: {meta.get('nodes')} neurons, {meta.get('pairs')} pairs, built {meta.get('built_at', '')}",
        "path": str(path),
    }
