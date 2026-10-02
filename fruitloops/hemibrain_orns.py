"""Hemibrain ORN input onto PNs, for ORN-weighted seeds of ``paths`` and ``reach``.

The compact traced export holds 2 of the 2,574 hemibrain ORNs with PN partners, so
seeds use ``hemibrain_olfaction_orn_pn_connections``, which setup builds from the
pinned neo4j bundle. Glomeruli come from the ``ORN_<glomerulus>`` types, with the
hemibrain v1.2 names that Schlegel et al. (2021) changed mapped to the names of the
receptor-family table. The antenna side comes from the instance suffix: in the RN
table of Schlegel et al. (2021), ``_R`` ORNs enter the (right) antennal lobe from the
ipsilateral antenna and ``_L`` ORNs from the contralateral one; ORNs without a
suffix have no known side.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .curated import hemibrain_glomerulus_names
from .duckdb_store import connect_read_only, safe_identifier, table_exists
from .graph_cache import ConnectomeGraph
from .olfaction import HEMIBRAIN_OLFACTION_ORN_PN_TABLE

ORN_TYPE_PREFIX = "ORN_"
INSTANCE_SIDES = {"_R": "right", "_L": "left"}


@dataclass(frozen=True)
class HemibrainOrnInputs:
    """One row per ORN->PN pair of the ORN table."""

    orn_ids: np.ndarray
    pn_ids: np.ndarray
    synapses: np.ndarray
    glomeruli: np.ndarray
    sides: np.ndarray


def load_hemibrain_orn_inputs(store: Path) -> HemibrainOrnInputs:
    table = safe_identifier(HEMIBRAIN_OLFACTION_ORN_PN_TABLE)
    with connect_read_only(store, "hemibrain ORN inputs") as connection:
        if not table_exists(connection, table):
            raise SystemExit(
                f"missing {table}; run `fruitloops setup --hemibrain` "
                "to build the hemibrain ORN->PN table from the pinned neo4j bundle"
            )
        result = connection.execute(
            f"""
            SELECT CAST(bodyId_pre AS BIGINT) AS orn, CAST(bodyId_post AS BIGINT) AS pn,
                   CAST(weight AS BIGINT) AS synapses, pre_type, coalesce(pre_instance, '') AS pre_instance
            FROM {table}
            WHERE starts_with(pre_type, '{ORN_TYPE_PREFIX}') AND CAST(weight AS BIGINT) > 0
            ORDER BY orn, pn
            """
        ).fetchnumpy()
    renamed = hemibrain_glomerulus_names()
    glomeruli = [cell_type.removeprefix(ORN_TYPE_PREFIX) for cell_type in result["pre_type"]]
    return HemibrainOrnInputs(
        orn_ids=np.asarray(result["orn"], dtype=np.int64),
        pn_ids=np.asarray(result["pn"], dtype=np.int64),
        synapses=np.asarray(result["synapses"], dtype=np.float64),
        glomeruli=np.asarray([renamed.get(name, name) for name in glomeruli], dtype=object),
        sides=np.asarray([INSTANCE_SIDES.get(instance[-2:], "") for instance in result["pre_instance"]], dtype=object),
    )


def hemibrain_orn_seeds(
    graph: ConnectomeGraph, sources: np.ndarray, inputs: HemibrainOrnInputs, chosen: np.ndarray
) -> np.ndarray:
    """Seed(PN) = synapses from ``chosen`` ORN rows / (graph input from other partners + all ORN synapses).

    ORN synapses come from the ORN table, every other input from the graph, so the
    synapses of traced ORNs, which both hold, count once.
    """
    pn_nodes = graph.nodes_for_ids(inputs.pn_ids)
    known = pn_nodes >= 0
    chosen_synapses = np.bincount(pn_nodes[known & chosen], weights=inputs.synapses[known & chosen], minlength=graph.size)
    orn_synapses = np.bincount(pn_nodes[known], weights=inputs.synapses[known], minlength=graph.size)
    orn_nodes = graph.nodes_for_ids(np.unique(inputs.orn_ids))
    traced_orn = np.zeros(graph.size, dtype=bool)
    traced_orn[orn_nodes[orn_nodes >= 0]] = True
    from_orn = traced_orn[graph.pre]
    graph_orn = np.bincount(graph.post[from_orn], weights=graph.synapses[from_orn], minlength=graph.size)
    totals = graph.input_synapses - graph_orn + orn_synapses
    seeds = np.divide(chosen_synapses, totals, out=np.zeros(graph.size), where=totals > 0)
    return np.where(sources, seeds, 0.0)
