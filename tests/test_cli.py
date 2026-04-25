from __future__ import annotations

import csv
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from fruitloops.cli import main
from fruitloops.cache import get_or_fetch, list_cache
from fruitloops.env import load_env_file
from fruitloops.live import parse_in_filters, parse_ints
from fruitloops.plotting import PlotSpec


class CliTest(unittest.TestCase):
    def test_datasets_uses_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            write_csv(
                data / "manifest.csv",
                [
                    {
                        "dataset": "flywire",
                        "collection": "analysis_outputs",
                        "file_id": "flywire_analysis_outputs_full_summary",
                        "relative_path": "flywire/analysis_outputs/full_summary.csv",
                        "rows": "1",
                        "columns": "LN_type|n_synapses",
                        "size_bytes": "1",
                        "sha256": "x",
                    }
                ],
            )
            write_csv(data / "flywire/analysis_outputs/full_summary.csv", [{"LN_type": "il3LN6", "n_synapses": "7"}])

            output = run_cli("--data-dir", str(data), "datasets")

        self.assertIn("flywire", output)

    def test_aggregate_sums_filtered_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            write_csv(
                data / "manifest.csv",
                [
                    {
                        "dataset": "flywire",
                        "collection": "source_audit",
                        "file_id": "audit",
                        "relative_path": "flywire/source_audit/orn.csv",
                        "rows": "3",
                        "columns": "LN_type|analysis_hemisphere|input_relation|n_synapses",
                        "size_bytes": "1",
                        "sha256": "x",
                    }
                ],
            )
            write_csv(
                data / "flywire/source_audit/orn.csv",
                [
                    {"LN_type": "il3LN6", "analysis_hemisphere": "L", "input_relation": "ipsi", "n_synapses": "2"},
                    {"LN_type": "il3LN6", "analysis_hemisphere": "L", "input_relation": "ipsi", "n_synapses": "3"},
                    {"LN_type": "lLN2T", "analysis_hemisphere": "L", "input_relation": "ipsi", "n_synapses": "100"},
                ],
            )

            output = run_cli(
                "--data-dir",
                str(data),
                "aggregate",
                "--table",
                "audit",
                "--where",
                "LN_type=il3LN6",
                "--by",
                "analysis_hemisphere,input_relation",
                "--sum",
                "n_synapses",
                "--format",
                "csv",
            )

        self.assertIn("L,ipsi,2,5", output)

    def test_plot_spec_defaults_to_png(self) -> None:
        spec = PlotSpec(kind="scatter", x="x", y="y")

        self.assertEqual(spec.formats, ("png",))

    def test_plot_accepts_csv_path_without_data_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "table.csv"
            write_csv(path, [{"score": "0.1"}, {"score": "0.2"}])

            with patch("fruitloops.cli.render_plot", return_value=[]) as render:
                output = run_cli(
                    "--data-dir",
                    str(Path(tmp) / "missing-data"),
                    "plot",
                    "--csv",
                    str(path),
                    "--kind",
                    "hist",
                    "--value",
                    "score",
                    "--output",
                    str(Path(tmp) / "hist"),
                )

        self.assertEqual(output, "")
        self.assertEqual(len(render.call_args.args[0]), 2)

    def test_env_file_loads_without_overriding_existing_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".env"
            path.write_text("FRUITLOOPS_TEST_VALUE=from-file\nFRUITLOOPS_KEEP=from-file\n")

            with patch.dict(
                "os.environ",
                {"FRUITLOOPS_KEEP": "existing"},
                clear=False,
            ):
                load_env_file(path)
                import os

                self.assertEqual(os.environ["FRUITLOOPS_TEST_VALUE"], "from-file")
                self.assertEqual(os.environ["FRUITLOOPS_KEEP"], "existing")

    def test_live_id_and_filter_parsing(self) -> None:
        self.assertEqual(parse_ints(["1,2", "3"]), [1, 2, 3])
        self.assertEqual(parse_in_filters([("pre_pt_root_id", "1,2")]), {"pre_pt_root_id": [1, 2]})

    def test_cache_fetches_once_then_reads_offline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            calls = 0

            def fetch() -> list[dict[str, str]]:
                nonlocal calls
                calls += 1
                return [{"id": "1", "weight": "2"}]

            cache_dir = Path(tmp) / "cache"
            query = {"body_id": [1], "limit": 1}
            rows, entry, source = get_or_fetch(cache_dir, "hemibrain", "neurons", query, fetch)
            self.assertEqual(source, "live")
            self.assertEqual(rows, [{"id": "1", "weight": "2"}])
            self.assertIsNotNone(entry)

            rows, _, source = get_or_fetch(cache_dir, "hemibrain", "neurons", query, fetch)
            self.assertEqual(source, "cache")
            self.assertEqual(rows, [{"id": "1", "weight": "2"}])
            self.assertEqual(calls, 1)
            self.assertEqual(len(list_cache(cache_dir)), 1)

    def test_offline_only_misses_without_fetching(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            rows, entry, source = get_or_fetch(
                Path(tmp),
                "flywire",
                "tables",
                {},
                lambda: [{"table": "synapses_nt_v1"}],
                offline_only=True,
            )

        self.assertEqual(rows, [])
        self.assertIsNone(entry)
        self.assertEqual(source, "miss")


def run_cli(*args: str) -> str:
    output = StringIO()
    with redirect_stdout(output):
        result = main(list(args))
    if result != 0:
        raise AssertionError(f"CLI exited with {result}")
    return output.getvalue()


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
