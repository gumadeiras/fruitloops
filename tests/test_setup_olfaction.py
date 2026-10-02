from __future__ import annotations

import csv
import importlib.util
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from fruitloops.cli import main
from fruitloops.olfaction import olfaction_provenance_datasets
from fruitloops.olfaction_live import cache_olfaction_annotations
from fruitloops.setup_state import setup_row
from fruitloops.table_import import import_to_duckdb

HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None
FIXTURES = Path(__file__).parent / "fixtures" / "bulk"


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
class SharedOlfactionTablesTest(unittest.TestCase):
    """The olf tables are one set for both datasets: no single-dataset run may narrow them."""

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.store = self.tmp / "fixture.duckdb"
        hemibrain = self.tmp / "hemibrain_roi_connections.csv"
        with hemibrain.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["bodyId_pre", "bodyId_post", "roi", "weight"])
            writer.writerow(["501", "601", "AL(R)", "11"])
        self.sources = {
            "flywire": [(FIXTURES / "flywire_olf_connections.csv", "flywire_proofread_connections")],
            "hemibrain": [
                (hemibrain, "hemibrain_traced_roi_connections"),
                (FIXTURES / "hemibrain_neurons.csv", "hemibrain_traced_neurons"),
            ],
        }

    def import_sources(self, dataset: str) -> None:
        for path, table in self.sources[dataset]:
            import_to_duckdb(path, table, store=self.store, replace=True, skip_current=True)

    def olf_datasets(self) -> set[str]:
        import duckdb

        with duckdb.connect(str(self.store), read_only=True) as connection:
            return olfaction_provenance_datasets(connection, "olf")

    def setup(self, *flags: str, bundle: list[dict[str, str]] | None = None) -> list[dict[str, str]]:
        def fake_bulk(bulk_dir, store, datasets, replace, skip_current):
            self.import_sources(datasets[0])
            return []

        graph = setup_row("", "graph", "", "current", "", self.store)
        output = StringIO()
        with patch("fruitloops.cli_setup.setup_practical_bulk", side_effect=fake_bulk), patch(
            "fruitloops.cli_setup.build_graph_cache", return_value=graph
        ), patch("fruitloops.cli_setup.setup_hemibrain_bundle", return_value=bundle or []), patch(
            "fruitloops.cli_setup.cache_olfaction_annotations", return_value=[]
        ), redirect_stdout(output), redirect_stderr(StringIO()):
            result = main(
                [
                    "setup", *flags, "--no-progress", "--csv", "--store", str(self.store),
                    "--cache-dir", str(self.tmp / "cache"), "--bulk-dir", str(self.tmp / "bulk"),
                ]
            )
        self.assertEqual(result, 0)
        return list(csv.DictReader(StringIO(output.getvalue())))

    def test_setup_for_one_dataset_keeps_the_other_in_olf_tables(self) -> None:
        first = self.setup("--flywire")
        self.assertEqual(self.olf_datasets(), {"flywire"})
        # Hemibrain was not selected, so its missing tables are not reported.
        self.assertEqual([row for row in first if row["status"].startswith("missing:")], [])

        # Each run below ends with a different olf build: the main build, the build after
        # live annotations, and the build after the hemibrain bundle tables.
        self.setup("--hemibrain")
        self.assertEqual(self.olf_datasets(), {"flywire", "hemibrain"})
        self.setup("--flywire", "--cache-annotations")
        self.assertEqual(self.olf_datasets(), {"flywire", "hemibrain"})
        bundle = [setup_row("hemibrain", "import", "hemibrain_olfaction_orn_pn_connections", "1", "", self.store)]
        last = self.setup("--hemibrain", bundle=bundle)
        self.assertEqual(self.olf_datasets(), {"flywire", "hemibrain"})

        olf_rows = [row for row in last if row["action"].startswith("olfaction-")]
        self.assertEqual({row["action"] for row in olf_rows}, {"olfaction-build", "olfaction-rebuild"})
        self.assertTrue(all(row["status"].startswith("current:") for row in olf_rows), olf_rows)

    def test_annotation_cache_for_one_dataset_builds_missing_olf_tables_for_both(self) -> None:
        self.import_sources("flywire")
        self.import_sources("hemibrain")

        with patch("fruitloops.olfaction_live.cache_flywire_annotations", return_value=[]):
            cache_olfaction_annotations(store=self.store, datasets=["flywire"], rebuild=False)

        self.assertEqual(self.olf_datasets(), {"flywire", "hemibrain"})


if __name__ == "__main__":
    unittest.main()
