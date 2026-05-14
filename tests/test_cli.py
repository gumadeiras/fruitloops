from __future__ import annotations

import csv
import importlib.util
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from fruitloops.cli import main
from fruitloops.bulk import (
    DEFAULT_DUCKDB_PATH,
    archive_stem,
    default_bulk_dir,
    import_to_duckdb,
    list_sources,
    safe_identifier,
    setup_flywire_bulk,
    table_summary,
    where_clause,
)
from fruitloops.cache import DEFAULT_CACHE_DIR, get_or_fetch, list_cache
from fruitloops.env import load_env_file
from fruitloops.live import parse_in_filters, parse_ints
from fruitloops.olfaction import build_olfaction_cache
from fruitloops.olfaction_labels import classify_name
from fruitloops.paths import default_data_dir, default_duckdb_path, default_live_cache_dir
from fruitloops.plotting import PlotSpec


class CliTest(unittest.TestCase):
    def test_no_command_prints_help(self) -> None:
        output = run_cli()

        self.assertIn("usage: fruitloops", output)
        self.assertIn("status", output)
        self.assertIn("table", output)
        self.assertIn("find", output)
        self.assertNotIn("==SUPPRESS==", output)

    def test_locations_reports_absolute_paths(self) -> None:
        output = run_cli("locations", "--format", "csv")

        self.assertIn("name,path,exists,env", output)
        self.assertIn(str(DEFAULT_DUCKDB_PATH), output)
        self.assertTrue(DEFAULT_DUCKDB_PATH.is_absolute())
        self.assertTrue(DEFAULT_CACHE_DIR.is_absolute())

    def test_status_summarizes_locations_and_datasets(self) -> None:
        output = run_cli("status", "--csv")

        self.assertIn("section,name,value,path,exists", output)
        self.assertIn("location,data_dir,configured", output)
        self.assertIn("dataset,flywire,", output)

    def test_examples_and_admin_passthrough(self) -> None:
        examples = run_cli("examples")
        admin = run_cli("admin", "bulk", "sources", "--csv")

        self.assertIn("fruitloops status --csv", examples)
        self.assertIn("dataset,kind,format", admin)

    def test_setup_wraps_bulk_cache_and_olfaction_build(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            store = Path(tmp) / "fruitloops.duckdb"
            with patch(
                "fruitloops.cli_setup.setup_practical_bulk",
                return_value=[
                    {
                        "dataset": "flywire",
                        "action": "import",
                        "target": "flywire_proofread_connections",
                        "status": "7",
                        "path": "/tmp/proofread.feather",
                        "store": str(store),
                    }
                ],
            ) as bulk_setup:
                with patch(
                    "fruitloops.cli_setup.build_olfaction_cache",
                    return_value=[
                        {
                            "dataset": "flywire",
                            "table": "olf_neurons",
                            "rows": "5",
                            "status": "built",
                            "store": str(store),
                        }
                    ],
                ) as olfaction_build:
                    output, progress = run_cli_capture(
                        "setup",
                        "--flywire",
                        "--cache-dir",
                        str(cache_dir),
                        "--store",
                        str(store),
                        "--csv",
                    )
                    cache_exists = cache_dir.exists()

        self.assertTrue(cache_exists)
        bulk_setup.assert_called_once()
        olfaction_build.assert_called_once()
        self.assertTrue(bulk_setup.call_args.kwargs["skip_current"])
        self.assertTrue(olfaction_build.call_args.kwargs["skip_current"])
        self.assertIn("fruitloops setup [1/3] prepare live cache", progress)
        self.assertIn("fruitloops setup [2/3] flywire: download/import bulk connectivity", progress)
        self.assertIn("fruitloops setup [3/3] build derived olfaction tables", progress)
        self.assertIn("fruitloops setup done: write setup summary", progress)
        self.assertIn("all,cache,live_cache,ready", output)
        self.assertIn("flywire,import,flywire_proofread_connections,7", output)
        self.assertIn("flywire,olfaction-build,olf_neurons,built:5", output)

    def test_setup_table_output_uses_compact_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            store = Path(tmp) / "fruitloops.duckdb"
            with patch(
                "fruitloops.cli_setup.setup_practical_bulk",
                return_value=[
                    {
                        "dataset": "flywire",
                        "action": "download",
                        "target": "proofread-connections",
                        "status": "ok",
                        "path": "/very/long/path/proofread_connections_783.feather",
                        "store": str(store),
                    }
                ],
            ):
                with patch(
                    "fruitloops.cli_setup.build_olfaction_cache",
                    return_value=[
                        {
                            "dataset": "flywire",
                            "table": "olf_neurons",
                            "rows": "5",
                            "status": "built",
                            "store": str(store),
                        }
                    ],
                ):
                    output = run_cli(
                        "setup",
                        "--flywire",
                        "--cache-dir",
                        str(cache_dir),
                        "--store",
                        str(store),
                        "--no-progress",
                    )

        self.assertIn("all:\n  - cache: live_cache -> ready", output)
        self.assertIn("flywire:\n  - download: proofread-connections -> ok", output)
        self.assertIn("  - olfaction-build: olf_neurons -> built:5", output)
        self.assertNotIn("dataset", output)
        self.assertNotIn("target", output)
        self.assertNotIn("path", output)
        self.assertNotIn("store", output)
        self.assertNotIn("/very/long/path", output)
        self.assertNotIn(str(store), output)

    def test_setup_can_cache_annotations_before_rebuild(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            store = Path(tmp) / "fruitloops.duckdb"
            with patch("fruitloops.cli_setup.setup_practical_bulk", return_value=[]):
                with patch(
                    "fruitloops.cli_setup.build_olfaction_cache",
                    side_effect=[
                        [
                            {
                                "dataset": "flywire",
                                "table": "olf_neurons",
                                "rows": "5",
                                "status": "built",
                                "store": str(store),
                            }
                        ],
                        [
                            {
                                "dataset": "flywire",
                                "table": "olf_neurons",
                                "rows": "6",
                                "status": "built",
                                "store": str(store),
                            }
                        ],
                    ],
                ) as olfaction_build:
                    with patch(
                        "fruitloops.cli_setup.cache_olfaction_annotations",
                        return_value=[
                            {
                                "dataset": "flywire",
                                "table": "flywire_neurons",
                                "rows": "9",
                                "status": "cached",
                                "store": str(store),
                            }
                        ],
                    ) as annotation_cache:
                        output, progress = run_cli_capture(
                            "setup",
                            "--flywire",
                            "--no-progress",
                            "--cache-annotations",
                            "--cache-dir",
                            str(cache_dir),
                            "--store",
                            str(store),
                            "--csv",
                        )

        annotation_cache.assert_called_once()
        self.assertEqual(olfaction_build.call_count, 2)
        self.assertEqual(progress, "")
        self.assertIn("flywire,annotation-cache,flywire_neurons,cached:9", output)
        self.assertIn("flywire,olfaction-rebuild,olf_neurons,built:6", output)

    def test_admin_passthrough_preserves_global_data_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            write_csv(
                data / "manifest.csv",
                [
                    {
                        "dataset": "flywire",
                        "collection": "analysis_outputs",
                        "file_id": "summary",
                        "relative_path": "flywire/analysis_outputs/summary.csv",
                        "rows": "1",
                        "columns": "LN_type",
                        "size_bytes": "1",
                        "sha256": "x",
                    }
                ],
            )
            write_csv(data / "flywire/analysis_outputs/summary.csv", [{"LN_type": "il3LN6"}])

            output = run_cli("--data-dir", str(data), "admin", "status", "--csv")

        self.assertIn("dataset,flywire,1 tables", output)

    def test_path_defaults_do_not_depend_on_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            os.environ,
            {
                "FRUITLOOPS_BULK_DIR": str(Path(tmp) / "bulk"),
                "FRUITLOOPS_DUCKDB_PATH": str(Path(tmp) / "custom.duckdb"),
                "FRUITLOOPS_CACHE_DIR": str(Path(tmp) / "cache"),
            },
        ):
            tmp_path = Path(tmp).resolve()
            self.assertEqual(default_bulk_dir(), tmp_path / "bulk")
            self.assertEqual(default_duckdb_path(), tmp_path / "custom.duckdb")
            self.assertEqual(default_live_cache_dir(), tmp_path / "cache")

    def test_data_dir_falls_back_to_user_data_when_not_bundled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            os.environ,
            {"XDG_DATA_HOME": str(Path(tmp) / "xdg")},
            clear=False,
        ):
            with patch("fruitloops.paths.package_root", return_value=Path(tmp) / "missing-package"):
                with patch("fruitloops.paths.sys.prefix", str(Path(tmp) / "missing-prefix")):
                    self.assertEqual(default_data_dir(), Path(tmp).resolve() / "xdg" / "fruitloops" / "data")

    def test_olfaction_label_classifier_covers_olfactory_targets(self) -> None:
        self.assertEqual(classify_name("OSN_DM1_R"), "ORN")
        self.assertEqual(classify_name("lateral horn neuron LHN_R"), "LHN")
        self.assertEqual(classify_name("lateral horn target"), "")
        self.assertEqual(classify_name("MBON01"), "MBON")

    def test_missing_required_arguments_print_command_help(self) -> None:
        self.assertIn("usage: fruitloops schema", run_cli("schema"))
        self.assertIn("--table", run_cli("schema"))

        self.assertIn("usage: fruitloops ln", run_cli("ln"))
        self.assertIn("name", run_cli("ln"))

        self.assertIn("usage: fruitloops plot", run_cli("plot"))
        self.assertIn("--csv", run_cli("plot"))

    def test_missing_nested_commands_print_subcommand_help(self) -> None:
        self.assertIn("usage: fruitloops live", run_cli("live"))
        self.assertIn("hemibrain", run_cli("live"))

        self.assertIn("usage: fruitloops live hemibrain", run_cli("live", "hemibrain"))
        self.assertIn("connections", run_cli("live", "hemibrain"))

        self.assertIn("usage: fruitloops bulk download", run_cli("bulk", "download"))
        self.assertIn("--dataset", run_cli("bulk", "download"))

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

    def test_table_command_lists_queries_and_aggregates(self) -> None:
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

            listed = run_cli("--data-dir", str(data), "table", "--flywire", "--contains", "orn", "--csv")
            queried = run_cli(
                "--data-dir",
                str(data),
                "table",
                "audit",
                "--where",
                "LN_type=il3LN6",
                "--select",
                "LN_type,n_synapses",
                "--csv",
            )
            aggregated = run_cli(
                "--data-dir",
                str(data),
                "table",
                "audit",
                "--where",
                "LN_type=il3LN6",
                "--by",
                "analysis_hemisphere,input_relation",
                "--sum",
                "n_synapses",
                "--csv",
            )

        self.assertIn("flywire,source_audit,audit", listed)
        self.assertIn("LN_type,n_synapses", queried)
        self.assertIn("il3LN6,2", queried)
        self.assertIn("L,ipsi,2,5", aggregated)

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

    def test_bulk_sources_include_primary_offline_tables(self) -> None:
        rows = list_sources()
        keys = {(row["dataset"], row["kind"]) for row in rows}

        self.assertIn(("flywire", "proofread-connections"), keys)
        self.assertIn(("hemibrain", "compact-adjacencies"), keys)
        self.assertIn(("hemibrain", "neo4j-inputs"), keys)

    def test_bulk_setup_wraps_download_import_and_optimize(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch("fruitloops.bulk.download_source", return_value=Path("/tmp/proofread.feather")):
                with patch(
                    "fruitloops.bulk.import_to_duckdb",
                    return_value={"table": "flywire_proofread_connections", "rows": "7"},
                ):
                    with patch(
                        "fruitloops.bulk.optimize_connection_table",
                        return_value=[{"action": "analyze", "name": "flywire_proofread_connections", "column": ""}],
                    ):
                        output = run_cli(
                            "bulk",
                            "--bulk-dir",
                            tmp,
                            "setup",
                            "--dataset",
                            "flywire",
                            "--format",
                            "csv",
                        )

        self.assertIn("flywire,download,proofread-connections,ok,/tmp/proofread.feather", output)
        self.assertIn("flywire,import,flywire_proofread_connections,7,/tmp/proofread.feather", output)
        self.assertIn("flywire,optimize,flywire_proofread_connections,analyze,", output)

    def test_bulk_identifier_and_where_clause_are_sanitized(self) -> None:
        self.assertEqual(safe_identifier("pre.pt-root id"), "pre_pt_root_id")
        clause, params = where_clause([("pre.pt-root id", "123")])

        self.assertEqual(clause, " WHERE pre_pt_root_id = ?")
        self.assertEqual(params, ["123"])
        self.assertEqual(
            archive_stem(Path("exported-traced-adjacencies-v1.2.tar.gz")),
            "exported-traced-adjacencies-v1.2",
        )

    @unittest.skipIf(importlib.util.find_spec("duckdb") is None, "duckdb not installed")
    def test_bulk_setup_skips_current_import_and_optimize(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "fruitloops.duckdb"
            fixture = Path("tests/fixtures/bulk/flywire_olf_connections.csv").resolve()
            with patch("fruitloops.bulk.download_source", return_value=fixture):
                first = setup_flywire_bulk(
                    Path(tmp) / "bulk",
                    store,
                    replace=True,
                    skip_current=True,
                )
                second = setup_flywire_bulk(
                    Path(tmp) / "bulk",
                    store,
                    replace=True,
                    skip_current=True,
                )

        self.assertTrue(
            any(row["action"] == "import" and row["status"] == "4" for row in first)
        )
        self.assertTrue(
            any(row["action"] == "import" and row["status"] == "current:4" for row in second)
        )
        self.assertTrue(
            any(row["action"] == "optimize" and row["status"] == "current" for row in second)
        )
        self.assertNotIn("_fruitloops_setup_state", {row["table"] for row in table_summary(store)})

    @unittest.skipIf(importlib.util.find_spec("duckdb") is None, "duckdb not installed")
    def test_bulk_import_and_partner_commands_with_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "fixture.duckdb"
            fixture = Path("tests/fixtures/bulk/flywire_connections.csv")
            run_cli(
                "bulk",
                "--store",
                str(store),
                "import",
                "--path",
                str(fixture),
                "--table",
                "flywire_test_connections",
                "--replace",
            )

            output = run_cli(
                "bulk",
                "--store",
                str(store),
                "inputs",
                "--table",
                "flywire_test_connections",
                "--body-id",
                "201",
                "--format",
                "csv",
            )
            run_cli(
                "bulk",
                "--store",
                str(store),
                "views",
                "--table",
                "flywire_test_connections",
                "--prefix",
                "flywire_test",
            )
            optimize_output = run_cli(
                "bulk",
                "--store",
                str(store),
                "optimize",
                "--table",
                "flywire_test_connections",
                "--prefix",
                "flywire_test",
                "--format",
                "csv",
            )
            view_output = run_cli(
                "bulk",
                "--store",
                str(store),
                "query",
                "--table",
                "flywire_test_partners",
                "--where",
                "body_id=201",
                "--where",
                "direction=input",
                "--limit",
                "1",
                "--format",
                "csv",
            )

        self.assertIn("101,AL_R,7,1", output)
        self.assertIn("index,flywire_test_pre_idx,pre_pt_root_id", optimize_output)
        self.assertIn("analyze,flywire_test_connections,", optimize_output)
        self.assertIn("body_id,partner_id,direction,roi,total_weight,connection_rows", view_output)

    @unittest.skipIf(importlib.util.find_spec("duckdb") is None, "duckdb not installed")
    def test_olfaction_builds_offline_tables_and_orn_input_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "fixture.duckdb"
            imports = [
                ("tests/fixtures/bulk/flywire_olf_connections.csv", "flywire_proofread_connections"),
                ("tests/fixtures/bulk/hemibrain_neurons.csv", "hemibrain_traced_neurons"),
                ("tests/fixtures/bulk/flywire_hierarchical.csv", "flywire_hierarchical_neuron_annotations"),
                ("tests/fixtures/bulk/flywire_neuron_info.csv", "flywire_neuron_information_v2"),
            ]
            for path, table in imports:
                run_cli(
                    "bulk",
                    "--store",
                    str(store),
                    "import",
                    "--path",
                    path,
                    "--table",
                    table,
                    "--replace",
                )

            build_output = run_cli(
                "olfaction",
                "--store",
                str(store),
                "build",
                "--dataset",
                "flywire",
                "--format",
                "csv",
            )
            pn_output = run_cli(
                "olf",
                "--store",
                str(store),
                "pns",
                "--dataset",
                "flywire",
                "--glomerulus",
                "DM1",
                "--format",
                "csv",
            )
            orn_input_output = run_cli(
                "olf",
                "--store",
                str(store),
                "orn-inputs",
                "--dataset",
                "flywire",
                "--glomerulus",
                "DM1",
                "--by-side",
                "--format",
                "csv",
            )
            class_output = run_cli(
                "olf",
                "--store",
                str(store),
                "classes",
                "--dataset",
                "flywire",
                "--region",
                "AL",
                "--class",
                "ORN",
                "--format",
                "csv",
            )
            glomerulus_output = run_cli(
                "olf",
                "--store",
                str(store),
                "glomerulus",
                "DM1",
                "--dataset",
                "flywire",
                "--format",
                "csv",
            )
            pathway_output = run_cli(
                "olf",
                "--store",
                str(store),
                "pathway",
                "ORN",
                "PN",
                "--dataset",
                "flywire",
                "--glomerulus",
                "DM1",
                "--by-side",
                "--format",
                "csv",
            )
            inputs_output = run_cli(
                "olf",
                "--store",
                str(store),
                "inputs",
                "--dataset",
                "flywire",
                "--target-class",
                "PN",
                "--source-class",
                "ORN",
                "--glomerulus",
                "DM1",
                "--by-side",
                "--format",
                "csv",
            )
            outputs_output = run_cli(
                "olf",
                "--store",
                str(store),
                "outputs",
                "--dataset",
                "flywire",
                "--source-class",
                "PN",
                "--target-class",
                "KC",
                "--region",
                "MB",
                "--by-side",
                "--format",
                "csv",
            )

        self.assertIn("flywire,flywire_proofread_connections,4,imported", build_output)
        self.assertIn("all,olf_annotations,5,built", build_output)
        self.assertIn("all,olf_pathway_edges,4,built", build_output)
        self.assertIn("all,olf_cell_type_summary,8,built", build_output)
        self.assertIn("flywire,2001,DM1_lPN_R,,PN,DM1,R", pn_output)
        self.assertIn("flywire,2001,DM1_lPN_R,DM1,1,12,R,R,ipsi", orn_input_output)
        self.assertIn("flywire,2001,DM1_lPN_R,DM1,1,5,R,L,contra", orn_input_output)
        self.assertIn("flywire,AL,R,ORN,DM1,R,1,0,12,12", class_output)
        self.assertIn("flywire,DM1,2,1,0,3,2,1,17", glomerulus_output)
        self.assertIn("flywire,ORN,PN,DM1,DM1,AL,R,ipsi,ipsi,ipsi,1,1,12", pathway_output)
        self.assertIn("flywire,2001,DM1_lPN_R,PN,DM1,ORN,DM1,1,12,R,R,R,ipsi,ipsi,ipsi", inputs_output)
        self.assertIn("flywire,2001,DM1_lPN_R,PN,DM1,KC,,1,7,R,R,R,ipsi,ipsi,ipsi", outputs_output)

    @unittest.skipIf(importlib.util.find_spec("duckdb") is None, "duckdb not installed")
    def test_olfaction_build_skips_current_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "fixture.duckdb"
            for path, table in [
                ("tests/fixtures/bulk/flywire_olf_connections.csv", "flywire_proofread_connections"),
                ("tests/fixtures/bulk/flywire_hierarchical.csv", "flywire_hierarchical_neuron_annotations"),
                ("tests/fixtures/bulk/flywire_neuron_info.csv", "flywire_neuron_information_v2"),
            ]:
                import_to_duckdb(Path(path), table, store=store, replace=True)

            first = build_olfaction_cache(
                store=store,
                datasets=["flywire"],
                replace=True,
                skip_current=True,
            )
            second = build_olfaction_cache(
                store=store,
                datasets=["flywire"],
                replace=True,
                skip_current=True,
            )

        self.assertTrue(
            any(row["table"] == "olf_neurons" and row["status"] == "built" for row in first)
        )
        self.assertTrue(
            any(row["table"] == "olf_neurons" and row["status"] == "current" for row in second)
        )

    @unittest.skipIf(importlib.util.find_spec("duckdb") is None, "duckdb not installed")
    def test_olfaction_build_rebuilds_incomplete_current_cache(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "fixture.duckdb"
            for path, table in [
                ("tests/fixtures/bulk/flywire_olf_connections.csv", "flywire_proofread_connections"),
                ("tests/fixtures/bulk/flywire_hierarchical.csv", "flywire_hierarchical_neuron_annotations"),
                ("tests/fixtures/bulk/flywire_neuron_info.csv", "flywire_neuron_information_v2"),
            ]:
                import_to_duckdb(Path(path), table, store=store, replace=True)

            build_olfaction_cache(
                store=store,
                datasets=["flywire"],
                replace=True,
                skip_current=True,
            )
            with duckdb.connect(str(store)) as connection:
                connection.execute("DROP TABLE olf_pathway_edges")

            rebuilt = build_olfaction_cache(
                store=store,
                datasets=["flywire"],
                replace=True,
                skip_current=True,
            )

        self.assertTrue(
            any(row["table"] == "olf_pathway_edges" and row["status"] == "built" for row in rebuilt)
        )

    @unittest.skipIf(importlib.util.find_spec("duckdb") is None, "duckdb not installed")
    def test_olfaction_rebuilds_after_same_shape_source_reimport(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            store = tmp_path / "fixture.duckdb"
            first_source = tmp_path / "first.csv"
            second_source = tmp_path / "second.csv"
            rows = [
                {
                    "pre_pt_root_id": "1001",
                    "post_pt_root_id": "2001",
                    "neuropil": "AL_R",
                    "syn_count": "12",
                }
            ]
            write_csv(first_source, rows)
            rows[0]["syn_count"] = "13"
            write_csv(second_source, rows)

            import_to_duckdb(
                first_source,
                "flywire_proofread_connections",
                store=store,
                replace=True,
                skip_current=True,
            )
            build_olfaction_cache(
                store=store,
                datasets=["flywire"],
                replace=True,
                skip_current=True,
            )
            import_to_duckdb(
                second_source,
                "flywire_proofread_connections",
                store=store,
                replace=True,
                skip_current=True,
            )
            rebuilt = build_olfaction_cache(
                store=store,
                datasets=["flywire"],
                replace=True,
                skip_current=True,
            )

        self.assertTrue(
            any(row["table"] == "olf_neurons" and row["status"] == "built" for row in rebuilt)
        )


def run_cli(*args: str) -> str:
    return run_cli_capture(*args)[0]


def run_cli_capture(*args: str) -> tuple[str, str]:
    output = StringIO()
    progress = StringIO()
    with redirect_stdout(output), redirect_stderr(progress):
        try:
            result = main(list(args))
        except SystemExit as error:
            if error.code != 0:
                raise
            result = 0
    if result != 0:
        raise AssertionError(f"CLI exited with {result}")
    return output.getvalue(), progress.getvalue()


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
