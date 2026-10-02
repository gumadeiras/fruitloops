from __future__ import annotations

import csv
import importlib.util
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

from fruitloops.cli import main
from fruitloops.olfaction import build_olfaction_cache

HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None
RELATIONS = ("pre_to_post_relation", "pre_to_neuropil_relation", "post_to_neuropil_relation")

# One DM1 PN on the right gets 6 synapses in each antennal lobe from a right, a left, and an
# unsided ORN, so many rows tie on synapses and differ only in side columns or neuropil.
CONNECTIONS = [
    (orn, 2001, neuropil, 6) for orn in (1001, 1002, 1003) for neuropil in ("AL_R", "AL_L")
]
ANNOTATIONS = [
    (1001, "cell_type", "ORN_DM1_R"),
    (1002, "cell_type", "ORN_DM1_L"),
    (1003, "cell_type", "ORN_DM1"),
    (2001, "cell_type", "DM1_lPN_R"),
]

# Each query's total order as (column, descending) pairs. The first LEADING[name] keys rank
# the rows; the rest break ties between rows that differ only in side columns or neuropil.
LEADING = {"inputs": 4, "outputs": 4, "pathway": 4, "classes": 6, "edges": 4, "orn-inputs": 3}
ORDERS = {
    "inputs": (
        [("synapses", True), ("dataset", False), ("target_id", False), ("source_class", False),
         ("source_glomerulus", False), ("target_side", False), ("source_side", False), ("neuropil_side", False)]
        + [(column, False) for column in RELATIONS]
    ),
    "outputs": (
        [("synapses", True), ("dataset", False), ("source_id", False), ("target_class", False),
         ("target_glomerulus", False), ("source_side", False), ("target_side", False), ("neuropil_side", False)]
        + [(column, False) for column in RELATIONS]
    ),
    "pathway": (
        [("synapses", True), ("dataset", False), ("source_class", False), ("target_class", False),
         ("source_glomerulus", False), ("target_glomerulus", False), ("region", False), ("neuropil_side", False)]
        + [(column, False) for column in RELATIONS]
    ),
    "classes": [
        ("total_synapses", True), ("neurons", True), ("dataset", False), ("region", False), ("cell_class", False),
        ("glomerulus", False), ("neuropil_side", False), ("cell_body_side", False),
    ],
    "edges": [
        ("synapses", True), ("dataset", False), ("pre_id", False), ("post_id", False), ("neuropil", False),
        ("source_table", False),
    ],
    "orn-inputs": [("synapses", True), ("dataset", False), ("pn_id", False), ("orn_side", False)],
}
COMMANDS = {
    "inputs": ("inputs", "--target-class", "PN", "--source-class", "ORN", "--by-side"),
    "outputs": ("outputs", "--source-class", "ORN", "--target-class", "PN", "--by-side"),
    "pathway": ("pathway", "ORN", "PN", "--by-side"),
    "classes": ("classes", "--class", "ORN"),
    "edges": ("edges",),
    "orn-inputs": ("orn-inputs", "--by-side"),
}


def write_store(store: Path, reverse: bool) -> None:
    """Write the fixture with its rows in forward or reverse order, then build the olf tables."""
    import duckdb

    step = -1 if reverse else 1
    with duckdb.connect(str(store)) as connection:
        connection.execute(
            "CREATE TABLE flywire_proofread_connections"
            "(pre_pt_root_id BIGINT, post_pt_root_id BIGINT, neuropil VARCHAR, syn_count BIGINT)"
        )
        connection.executemany("INSERT INTO flywire_proofread_connections VALUES (?, ?, ?, ?)", CONNECTIONS[::step])
        connection.execute(
            "CREATE TABLE flywire_hierarchical_neuron_annotations"
            "(pt_root_id BIGINT, classification_system VARCHAR, cell_type VARCHAR)"
        )
        connection.executemany(
            "INSERT INTO flywire_hierarchical_neuron_annotations VALUES (?, ?, ?)", ANNOTATIONS[::step]
        )
    build_olfaction_cache(store=store, datasets=["flywire"], replace=True)


def olf_csv(store: Path, command: tuple[str, ...]) -> str:
    output = StringIO()
    with redirect_stdout(output), redirect_stderr(StringIO()):
        result = main(["olf", "--store", str(store), *command, "--dataset", "flywire", "--format", "csv"])
    if result != 0:
        raise AssertionError(f"olf {command[0]} exited with {result}")
    return output.getvalue()


def sort_key(order: list[tuple[str, bool]]):
    def key(row: dict[str, str]) -> tuple:
        return tuple(-int(row[column]) if descending else row[column] for column, descending in order)

    return key


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
class OlfactionOrderTest(unittest.TestCase):
    def test_tied_rows_keep_one_total_order_across_runs_and_stores(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            stores = [Path(tmp) / "forward.duckdb", Path(tmp) / "reverse.duckdb"]
            for store, reverse in zip(stores, (False, True)):
                write_store(store, reverse)
            outputs = {
                name: [olf_csv(store, command) for store in stores for _ in range(2)]
                for name, command in COMMANDS.items()
            }

        for name, runs in outputs.items():
            with self.subTest(name):
                rows = list(csv.DictReader(StringIO(runs[0])))
                ordered = sorted(rows, key=sort_key(ORDERS[name]))
                ties = len(rows) - len({sort_key(ORDERS[name][: LEADING[name]])(row) for row in rows})
                self.assertGreater(ties, 0, "the fixture must tie on the leading keys")
                self.assertEqual(runs, [runs[0]] * len(runs))
                self.assertEqual(rows, ordered)


if __name__ == "__main__":
    unittest.main()
