from __future__ import annotations

import csv
import os
import stat
import subprocess
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from fruitloops.bulk import setup_flywire_bulk
from fruitloops.graph_cache import graph_cache_path
from fruitloops.table_import import import_to_duckdb
from test_wholebrain import EDGES, HAS_DUCKDB, W_P2B, cli_error, run_cli_rows, write_flywire_fixture

# Holds a read-write connection, so other processes cannot open the store.
LOCK_SCRIPT = """
import sys, duckdb
connection = duckdb.connect(sys.argv[1])
print("locked", flush=True)
sys.stdin.read()
connection.close()
"""


def write_edges(path: Path, edges: list[tuple[int, int, str, int]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["pre_pt_root_id", "post_pt_root_id", "neuropil", "syn_count"])
        writer.writerows(edges)


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
class GraphCacheFreshnessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = self.root / "fixture.duckdb"
        write_flywire_fixture(self.store)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def strongest(self) -> tuple[dict[str, str], str]:
        rows, _, errors = run_cli_rows(
            "paths", "--flywire", "--store", str(self.store), "--source-class", "ALPN",
            "--target-id", "31", "--top", "1", "--csv",
        )
        return rows[0], errors

    def test_query_rebuilds_cache_after_reimport_with_same_row_count(self) -> None:
        first_file, second_file = self.root / "a.csv", self.root / "b.csv"
        write_edges(first_file, EDGES)
        # Same rows and columns; only the LHB -> DNx01 weight changes.
        write_edges(second_file, [(pre, post, roi, 500 if (pre, post) == (22, 31) else syn) for pre, post, roi, syn in EDGES])
        import_to_duckdb(first_file, "flywire_proofread_connections", store=self.store, replace=True)
        before, _ = self.strongest()
        import_to_duckdb(second_file, "flywire_proofread_connections", store=self.store, replace=True)
        after, errors = self.strongest()

        self.assertEqual(before["path_types"], "DA1_lPN > KCg > MBON01 > DNx01")
        self.assertIn("flywire graph cache is stale", errors)
        self.assertEqual(after["path_types"], "DA1_lPN > LHB > DNx01")
        self.assertEqual(after["step_synapses"], "20 > 500")
        in_t1 = 10 + 500 + 15 + 4 * 3
        self.assertAlmostEqual(float(after["strength"]), W_P2B * 500 / in_t1, places=6)

    def test_unreadable_cache_is_rebuilt(self) -> None:
        expected, _ = self.strongest()
        graph_cache_path(self.store, "flywire").write_bytes(b"not a cache")
        status, _, _ = run_cli_rows("status", "--store", str(self.store), "--csv")
        rebuilt, errors = self.strongest()

        self.assertIn(("graph", "flywire", "unreadable"), {(r["section"], r["name"], r["value"]) for r in status})
        self.assertIn("flywire graph cache is unreadable", errors)
        self.assertEqual(rebuilt, expected)

    def test_cache_builds_from_a_read_only_store(self) -> None:
        mode = self.store.stat().st_mode
        os.chmod(self.store, stat.S_IRUSR)
        try:
            row, errors = self.strongest()
        finally:
            os.chmod(self.store, mode)

        self.assertIn("flywire graph cache is missing", errors)
        self.assertEqual(row["path_types"], "DA1_lPN > KCg > MBON01 > DNx01")

    def test_store_locked_by_another_process(self) -> None:
        self.strongest()
        holder = subprocess.Popen(
            [sys.executable, "-c", LOCK_SCRIPT, str(self.store)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            self.assertEqual(holder.stdout.readline().strip(), "locked")
            status, _, _ = run_cli_rows("status", "--store", str(self.store), "--csv")
            error = cli_error(
                "paths", "--flywire", "--store", str(self.store), "--source-class", "ALPN", "--target-id", "31"
            )
        finally:
            holder.communicate("")

        graph = next(row for row in status if row["section"] == "graph" and row["name"] == "flywire")
        self.assertTrue(graph["value"].startswith("unavailable"))
        self.assertIn("another fruitloops process may be writing to it", error)


class AnnotationFailureTest(unittest.TestCase):
    @unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
    def test_setup_reports_annotation_download_failure_and_continues(self) -> None:
        connections = Path("tests/fixtures/bulk/flywire_olf_connections.csv").resolve()

        def fake_download(dataset, kind, output_dir):
            if kind == "neuron-annotations":
                raise urllib.error.URLError("offline")
            return connections

        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "store.duckdb"
            with patch("fruitloops.bulk.download_source", side_effect=fake_download):
                rows = setup_flywire_bulk(Path(tmp) / "bulk", store, replace=True, skip_current=True)

        statuses = {(row["action"], row["target"]): row["status"] for row in rows}
        self.assertEqual(statuses[("import", "flywire_proofread_connections")], "9")
        self.assertTrue(statuses[("download", "neuron-annotations")].startswith("error: "))
        self.assertIn("offline", statuses[("download", "neuron-annotations")])


if __name__ == "__main__":
    unittest.main()
