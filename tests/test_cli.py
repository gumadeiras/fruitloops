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
from fruitloops.cli_olfaction import CELL_CLASS_CHOICES
from fruitloops.olfaction import (
    HEMIBRAIN_OLFACTION_ORN_PN_TABLE,
    OLFACTION_REGIONS,
    build_olfaction_cache,
    olfaction_class_summary,
    olfaction_glomerulus_summary,
    olfaction_input_summary,
    olfaction_orn_inputs,
    olfaction_pathway_summary,
    olfaction_pns,
)
from fruitloops.olfaction_labels import classify_name, infer_glomerulus
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
        cache_help = run_cli("olf", "cache-annotations", "--help")

        self.assertIn("fruitloops status --csv", examples)
        self.assertNotIn("ROOT", examples)
        self.assertNotIn("BODY", examples)
        self.assertNotIn("...", examples)
        self.assertIn("dataset,kind,format", admin)
        self.assertIn("--hemibrain", cache_help)
        self.assertIn("--flywire", cache_help)

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

    def test_setup_reports_annotation_errors_after_rebuild(self) -> None:
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
                ):
                    with patch(
                        "fruitloops.cli_setup.cache_olfaction_annotations",
                        return_value=[
                            {
                                "dataset": "flywire",
                                "table": "flywire_neurons",
                                "rows": "9",
                                "status": "cached",
                                "store": str(store),
                            },
                            {
                                "dataset": "hemibrain",
                                "table": "hemibrain_olfaction_neuron_annotations",
                                "rows": "503 Service Unavailable",
                                "status": "error",
                                "store": str(store),
                            },
                        ],
                    ):
                        output = run_cli(
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

        self.assertIn("flywire,annotation-cache,flywire_neurons,cached:9", output)
        self.assertIn("flywire,olfaction-rebuild,olf_neurons,built:6", output)
        self.assertIn(
            "hemibrain,annotation-cache,hemibrain_olfaction_neuron_annotations,error:503 Service Unavailable",
            output,
        )
        self.assertLess(
            output.index("flywire,olfaction-rebuild,olf_neurons,built:6"),
            output.index("hemibrain,annotation-cache,hemibrain_olfaction_neuron_annotations,error:503"),
        )

    def test_setup_continues_when_annotation_cache_raises(self) -> None:
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
                                "rows": "5",
                                "status": "current",
                                "store": str(store),
                            }
                        ],
                    ],
                ):
                    with patch(
                        "fruitloops.cli_setup.cache_olfaction_annotations",
                        side_effect=RuntimeError("503 Service Unavailable"),
                    ):
                        output = run_cli(
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

        self.assertIn("flywire,olfaction-rebuild,olf_neurons,current:5", output)
        self.assertIn("all,annotation-cache,live_annotations,error:503 Service Unavailable", output)

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
                with patch("fruitloops.paths.sys.platform", "linux"), patch(
                    "fruitloops.paths.sys.prefix", str(Path(tmp) / "missing-prefix")
                ):
                    self.assertEqual(default_data_dir(), Path(tmp) / "xdg" / "fruitloops" / "data")

    def test_olfaction_label_classifier_covers_olfactory_targets(self) -> None:
        self.assertEqual(classify_name("OSN_DM1_R"), "ORN")
        self.assertEqual(classify_name("lateral horn neuron LHN_R"), "LHN")
        self.assertEqual(classify_name("LHAV4a1_b"), "LHN")
        self.assertEqual(classify_name("LHCENT12"), "LHN")
        self.assertEqual(classify_name("LHp2_medial"), "LHN")
        self.assertEqual(classify_name("lateral horn target"), "")
        self.assertEqual(classify_name("LH_R"), "")
        self.assertEqual(classify_name("MBON01"), "MBON")
        self.assertEqual(infer_glomerulus("DM1 / Or42b ORN"), "DM1")
        self.assertEqual(infer_glomerulus("sensory,DA1,ORN"), "DA1")
        self.assertEqual(infer_glomerulus("sensory,ORN,VA1d"), "VA1d")
        self.assertEqual(infer_glomerulus("DM3_adPN"), "DM3")

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

    def test_env_file_routes_path_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            env_file = root / "paths.env"
            env_file.write_text(
                f"FRUITLOOPS_BULK_DIR={root / 'bulk'}\n"
                f"FRUITLOOPS_DUCKDB_PATH={root / 'custom.duckdb'}\n"
                f"FRUITLOOPS_CACHE_DIR={root / 'cache'}\n"
            )
            with patch.dict(os.environ, {"HOME": str(root)}, clear=True):
                output = run_cli("--env-file", str(env_file), "locations", "--format", "csv")

        self.assertIn(f"bulk_dir,{root / 'bulk'}", output)
        self.assertIn(f"duckdb,{root / 'custom.duckdb'}", output)
        self.assertIn(f"live_cache,{root / 'cache'}", output)

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

    def test_cache_metadata_uses_current_location_after_move(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            old_dir = Path(tmp) / "old"
            new_dir = Path(tmp) / "new"
            get_or_fetch(
                old_dir,
                "hemibrain",
                "neurons",
                {"body_id": [1]},
                lambda: [{"id": "1"}],
            )

            old_dir.rename(new_dir)
            entry = list_cache(new_dir)[0]

            self.assertTrue(entry.path.is_relative_to(new_dir))
            self.assertTrue(entry.path.exists())

    def test_offline_list_reports_cached_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            get_or_fetch(
                cache_dir,
                "hemibrain",
                "neurons",
                {"body_id": [1], "limit": 1},
                lambda: [{"id": "1", "weight": "2"}],
            )

            output = run_cli(
                "admin",
                "offline",
                "--cache-dir",
                str(cache_dir),
                "list",
                "--format",
                "csv",
            )

        self.assertIn("dataset,action,key,rows,created_at,path", output)
        self.assertIn("hemibrain,neurons,", output)

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

    @unittest.skipIf(importlib.util.find_spec("duckdb") is None, "duckdb not installed")
    def test_annotation_cache_continues_after_one_service_error(self) -> None:
        import duckdb

        from fruitloops.olfaction_live import cache_olfaction_annotations

        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "fixture.duckdb"
            with duckdb.connect(str(store)) as connection:
                connection.execute("CREATE TABLE olf_neurons(dataset VARCHAR, body_id VARCHAR)")

            with patch(
                "fruitloops.olfaction_live.cache_hemibrain_annotations",
                side_effect=RuntimeError("503 Service Unavailable"),
            ):
                with patch(
                    "fruitloops.olfaction_live.cache_flywire_annotations",
                    return_value=[
                        {
                            "dataset": "flywire",
                            "table": "flywire_neurons",
                            "rows": "9",
                            "status": "cached",
                            "store": str(store),
                        }
                    ],
                ):
                    rows = cache_olfaction_annotations(
                        store=store,
                        datasets=["hemibrain", "flywire"],
                        rebuild=False,
                    )

        self.assertEqual(rows[0]["dataset"], "hemibrain")
        self.assertEqual(rows[0]["status"], "error")
        self.assertEqual(rows[0]["rows"], "503 Service Unavailable")
        self.assertEqual(rows[1]["dataset"], "flywire")
        self.assertEqual(rows[1]["status"], "cached")

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
            any(row["action"] == "import" and row["status"] == "9" for row in first)
        )
        self.assertTrue(
            any(row["action"] == "import" and row["status"] == "current:9" for row in second)
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
            import duckdb

            with duckdb.connect(str(store), read_only=True) as connection:
                coverage = set(
                    connection.execute(
                        """
                        SELECT region, cell_class
                        FROM olf_neuron_regions
                        WHERE dataset = 'flywire' AND cell_class <> ''
                        """
                    ).fetchall()
                )
                can_rows = connection.execute(
                    "SELECT count(*) FROM olf_edges_by_neuropil WHERE neuropil = 'CAN_R'"
                ).fetchone()[0]
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
            mbon_output = run_cli(
                "olf",
                "--store",
                str(store),
                "outputs",
                "--dataset",
                "flywire",
                "--source-class",
                "PN",
                "--target-class",
                "MBON",
                "--region",
                "MB",
                "--format",
                "csv",
            )
            dan_output = run_cli(
                "olf",
                "--store",
                str(store),
                "outputs",
                "--dataset",
                "flywire",
                "--source-class",
                "PN",
                "--target-class",
                "DAN",
                "--region",
                "MB",
                "--format",
                "csv",
            )
            apl_output = run_cli(
                "olf",
                "--store",
                str(store),
                "outputs",
                "--dataset",
                "flywire",
                "--source-class",
                "PN",
                "--target-class",
                "APL",
                "--region",
                "MB",
                "--format",
                "csv",
            )
            lhn_output = run_cli(
                "olf",
                "--store",
                str(store),
                "outputs",
                "--dataset",
                "flywire",
                "--source-class",
                "PN",
                "--target-class",
                "LHN",
                "--region",
                "LH",
                "--format",
                "csv",
            )

        self.assertIn("flywire,flywire_proofread_connections,8,imported", build_output)
        self.assertIn("all,olf_annotations,9,built", build_output)
        self.assertIn("all,olf_pathway_edges,8,built", build_output)
        self.assertIn("all,olf_cell_type_summary,12,built", build_output)
        self.assertTrue(
            {
                ("AL", "ORN"),
                ("AL", "PN"),
                ("LH", "PN"),
                ("LH", "LN"),
                ("LH", "LHN"),
                ("MB", "PN"),
                ("MB", "KC"),
                ("MB", "MBON"),
                ("MB", "DAN"),
                ("MB", "APL"),
            }.issubset(coverage)
        )
        self.assertEqual(can_rows, 0)
        self.assertIn("flywire,2001,DM1_lPN_R,,PN,DM1,R", pn_output)
        self.assertIn("flywire,2001,DM1_lPN_R,DM1,1,12,R,R,ipsi", orn_input_output)
        self.assertIn("flywire,2001,DM1_lPN_R,DM1,1,5,R,L,contra", orn_input_output)
        self.assertIn("flywire,AL,R,ORN,DM1,R,1,0,12,12", class_output)
        self.assertIn("flywire,DM1,2,1,0,3,2,1,17", glomerulus_output)
        self.assertIn("flywire,ORN,PN,DM1,DM1,AL,R,ipsi,ipsi,ipsi,1,1,12", pathway_output)
        self.assertIn("flywire,2001,DM1_lPN_R,PN,DM1,ORN,DM1,1,12,R,R,R,ipsi,ipsi,ipsi", inputs_output)
        self.assertIn("flywire,2001,DM1_lPN_R,PN,DM1,KC,,1,7,R,R,R,ipsi,ipsi,ipsi", outputs_output)
        self.assertIn("flywire,2001,DM1_lPN_R,PN,DM1,MBON,,1,6", mbon_output)
        self.assertIn("flywire,2001,DM1_lPN_R,PN,DM1,DAN,,1,5", dan_output)
        self.assertIn("flywire,2001,DM1_lPN_R,PN,DM1,APL,,1,4", apl_output)
        self.assertIn("flywire,2001,DM1_lPN_R,PN,DM1,LHN,,1,11", lhn_output)

    @unittest.skipIf(importlib.util.find_spec("duckdb") is None, "duckdb not installed")
    def test_olfaction_build_maps_hemibrain_mushroom_body_rois(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "fixture.duckdb"
            with duckdb.connect(str(store)) as connection:
                connection.execute(
                    """
                    CREATE TABLE hemibrain_traced_roi_connections(
                        bodyId_pre BIGINT,
                        bodyId_post BIGINT,
                        roi VARCHAR,
                        weight BIGINT
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO hemibrain_traced_roi_connections VALUES
                        (1001, 2001, 'AL(R)', 11),
                        (1001, 6001, 'AL(R)', 4),
                        (2001, 3001, 'LH(R)', 13),
                        (2001, 4001, 'CA(R)', 7),
                        (2001, 4001, 'PED(R)', 5),
                        (2001, 4001, 'aL(R)', 3),
                        (2001, 4001, 'a''L(R)', 2),
                        (2001, 4001, 'bL(R)', 2),
                        (2001, 4001, 'b''L(R)', 2),
                        (2001, 4001, 'gL(R)', 2),
                        (2001, 7001, 'CA(R)', 6),
                        (2001, 8001, 'PED(R)', 5),
                        (2001, 9001, 'gL(R)', 4),
                        (2001, 5001, 'CAN(R)', 17)
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE hemibrain_traced_neurons(
                        bodyId BIGINT,
                        type VARCHAR,
                        instance VARCHAR
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO hemibrain_traced_neurons VALUES
                        (1001, 'ORN_DM1', 'ORN_DM1_R'),
                        (2001, 'DM1_lPN', 'DM1_lPN_R'),
                        (3001, 'LHCENT12', 'LHCENT12_R'),
                        (4001, 'KCg', 'KCg_R'),
                        (5001, 'KCg', 'KCg_R'),
                        (6001, 'lLN1', 'lLN1_R'),
                        (7001, 'MBON01', 'MBON01_R'),
                        (8001, 'PAM01', 'PAM01_R'),
                        (9001, 'APL', 'APL_R')
                    """
                )

            build_olfaction_cache(store=store, datasets=["hemibrain"], replace=True)
            with duckdb.connect(str(store), read_only=True) as connection:
                coverage = set(
                    connection.execute(
                        """
                        SELECT region, cell_class
                        FROM olf_neuron_regions
                        WHERE dataset = 'hemibrain' AND cell_class <> ''
                        """
                    ).fetchall()
                )
                can_rows = connection.execute(
                    "SELECT count(*) FROM olf_edges_by_neuropil WHERE neuropil = 'CAN(R)'"
                ).fetchone()[0]
            output = run_cli(
                "olf",
                "--store",
                str(store),
                "outputs",
                "--dataset",
                "hemibrain",
                "--source-class",
                "PN",
                "--target-class",
                "KC",
                "--region",
                "MB",
                "--format",
                "csv",
            )
            mbon_output = run_cli(
                "olf",
                "--store",
                str(store),
                "outputs",
                "--dataset",
                "hemibrain",
                "--source-class",
                "PN",
                "--target-class",
                "MBON",
                "--region",
                "MB",
                "--format",
                "csv",
            )
            dan_output = run_cli(
                "olf",
                "--store",
                str(store),
                "outputs",
                "--dataset",
                "hemibrain",
                "--source-class",
                "PN",
                "--target-class",
                "DAN",
                "--region",
                "MB",
                "--format",
                "csv",
            )
            apl_output = run_cli(
                "olf",
                "--store",
                str(store),
                "outputs",
                "--dataset",
                "hemibrain",
                "--source-class",
                "PN",
                "--target-class",
                "APL",
                "--region",
                "MB",
                "--format",
                "csv",
            )
            lhn_output = run_cli(
                "olf",
                "--store",
                str(store),
                "outputs",
                "--dataset",
                "hemibrain",
                "--source-class",
                "PN",
                "--target-class",
                "LHN",
                "--region",
                "LH",
                "--format",
                "csv",
            )

        self.assertTrue(
            {
                ("AL", "ORN"),
                ("AL", "PN"),
                ("AL", "LN"),
                ("LH", "PN"),
                ("LH", "LHN"),
                ("MB", "PN"),
                ("MB", "KC"),
                ("MB", "MBON"),
                ("MB", "DAN"),
                ("MB", "APL"),
            }.issubset(coverage)
        )
        self.assertEqual(can_rows, 0)
        self.assertIn("hemibrain,2001,DM1_lPN,PN,DM1,KC,,1,23", output)
        self.assertIn("hemibrain,2001,DM1_lPN,PN,DM1,MBON,,1,6", mbon_output)
        self.assertIn("hemibrain,2001,DM1_lPN,PN,DM1,DAN,,1,5", dan_output)
        self.assertIn("hemibrain,2001,DM1_lPN,PN,DM1,APL,,1,4", apl_output)
        self.assertIn("hemibrain,2001,DM1_lPN,PN,DM1,LHN,,1,13", lhn_output)

    @unittest.skipIf(importlib.util.find_spec("duckdb") is None, "duckdb not installed")
    def test_olfaction_build_makes_every_hemibrain_orn_pn_glomerulus_queryable(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "fixture.duckdb"
            with duckdb.connect(str(store)) as connection:
                connection.execute(
                    """
                    CREATE TABLE hemibrain_traced_roi_connections(
                        bodyId_pre BIGINT,
                        bodyId_post BIGINT,
                        roi VARCHAR,
                        weight BIGINT
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE hemibrain_traced_neurons(
                        bodyId BIGINT,
                        type VARCHAR,
                        instance VARCHAR
                    )
                    """
                )
                connection.execute(
                    f"""
                    CREATE TABLE {HEMIBRAIN_OLFACTION_ORN_PN_TABLE}(
                        bodyId_pre BIGINT,
                        bodyId_post BIGINT,
                        roi VARCHAR,
                        weight BIGINT,
                        pre_type VARCHAR,
                        pre_instance VARCHAR,
                        post_type VARCHAR,
                        post_instance VARCHAR
                    )
                    """
                )
                connection.execute(
                    f"""
                    INSERT INTO {HEMIBRAIN_OLFACTION_ORN_PN_TABLE} VALUES
                        (1001, 2001, 'AL(R)', 12, 'ORN_DM1', 'ORN_DM1_R', 'DM1_lPN', 'DM1_lPN_R'),
                        (1002, 2001, 'AL(L)', 5, 'ORN_DM1', 'ORN_DM1_L', 'DM1_lPN', 'DM1_lPN_R'),
                        (1101, 2101, 'AL(R)', 9, 'ORN_DA2', 'ORN_DA2_R', 'DA2_lPN', 'DA2_lPN_R'),
                        (1201, 2201, 'AL(L)', 7, 'ORN_DM3', 'ORN_DM3_L', 'DM3_adPN', 'DM3_adPN_L')
                    """
                )

            build_olfaction_cache(store=store, datasets=["hemibrain"], replace=True)
            glomeruli = {
                row["glomerulus"]
                for row in olfaction_glomerulus_summary(store=store, dataset="hemibrain", limit=100)
            }
            rows_by_glomerulus = {
                glomerulus: olfaction_input_summary(
                    store=store,
                    dataset="hemibrain",
                    target_class="PN",
                    source_class="ORN",
                    glomerulus=glomerulus,
                    by_side=True,
                )
                for glomerulus in glomeruli
            }
            output, progress = run_cli_capture(
                "olf",
                "--store",
                str(store),
                "inputs",
                "--dataset",
                "hemibrain",
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

        self.assertEqual(glomeruli, {"DA2", "DM1", "DM3"})
        for glomerulus, rows in rows_by_glomerulus.items():
            self.assertTrue(rows, f"{glomerulus} should have ORN->PN input rows")
            self.assertTrue(
                any(
                    row["target_glomerulus"] == glomerulus
                    and row["source_glomerulus"] == glomerulus
                    and row["source_class"] == "ORN"
                    and row["target_class"] == "PN"
                    for row in rows
                ),
                f"{glomerulus} should preserve source and target glomerulus labels",
            )
        self.assertIn("hemibrain,2001,DM1_lPN,PN,DM1,ORN,DM1,1,12", output)
        self.assertNotIn("compact traced-adjacency cache", progress)

    @unittest.skipIf(importlib.util.find_spec("duckdb") is None, "duckdb not installed")
    def test_olfaction_cli_filter_matrix_covers_regions_classes_and_glomeruli(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "fixture.duckdb"
            with duckdb.connect(str(store)) as connection:
                create_olfaction_matrix_fixture(connection)

            build_olfaction_cache(store=store, datasets=None, replace=True)
            class_rows = olfaction_class_summary(store=store, limit=1000)
            glomerulus_rows = olfaction_glomerulus_summary(store=store, limit=1000)
            datasets = ("flywire", "hemibrain")

            self.assertGreaterEqual(len(BROAD_GLOMERULI), 60)
            for dataset in datasets:
                regions = {
                    row["region"]
                    for row in class_rows
                    if row["dataset"] == dataset and row["region"]
                }
                classes = {
                    row["cell_class"]
                    for row in class_rows
                    if row["dataset"] == dataset and row["cell_class"]
                }
                region_class_pairs = {
                    (row["region"], row["cell_class"])
                    for row in class_rows
                    if row["dataset"] == dataset and row["region"] and row["cell_class"]
                }
                glomeruli = {
                    row["glomerulus"]
                    for row in glomerulus_rows
                    if row["dataset"] == dataset and row["glomerulus"]
                }

                self.assertEqual(regions, set(OLFACTION_REGIONS))
                self.assertEqual(classes, set(CELL_CLASS_CHOICES))
                self.assertEqual(glomeruli, set(BROAD_GLOMERULI))

                for region in OLFACTION_REGIONS:
                    assert_nonempty_cli(
                        self,
                        run_cli("olf", "--store", str(store), "classes", "--dataset", dataset, "--region", region, "--csv"),
                    )
                    assert_nonempty_cli(
                        self,
                        run_cli("olf", "--store", str(store), "edges", "--dataset", dataset, "--region", region, "--csv"),
                    )

                for cell_class in CELL_CLASS_CHOICES:
                    assert_nonempty_cli(
                        self,
                        run_cli("olf", "--store", str(store), "classes", "--dataset", dataset, "--class", cell_class, "--csv"),
                    )

                for region, cell_class in sorted(region_class_pairs):
                    assert_nonempty_cli(
                        self,
                        run_cli(
                            "olf",
                            "--store",
                            str(store),
                            "neurons",
                            "--dataset",
                            dataset,
                            "--region",
                            region,
                            "--class",
                            cell_class,
                            "--csv",
                        ),
                    )

                for glomerulus in sorted(glomeruli):
                    self.assertTrue(
                        olfaction_pns(store=store, dataset=dataset, glomerulus=glomerulus),
                        f"{dataset} {glomerulus} pns should not be empty",
                    )
                    self.assertTrue(
                        olfaction_input_summary(
                            store=store,
                            dataset=dataset,
                            target_class="PN",
                            source_class="ORN",
                            glomerulus=glomerulus,
                            by_side=True,
                        ),
                        f"{dataset} {glomerulus} ORN->PN inputs should not be empty",
                    )
                    self.assertTrue(
                        olfaction_pathway_summary(
                            store=store,
                            dataset=dataset,
                            source_class="ORN",
                            target_class="PN",
                            source_glomerulus=glomerulus,
                            target_glomerulus=glomerulus,
                            by_side=True,
                        ),
                        f"{dataset} {glomerulus} ORN->PN pathway should not be empty",
                    )
                    self.assertTrue(
                        olfaction_orn_inputs(
                            store=store,
                            dataset=dataset,
                            glomerulus=glomerulus,
                            by_side=True,
                        ),
                        f"{dataset} {glomerulus} orn-inputs should not be empty",
                    )

                for glomerulus in ("DM1", "VA1d", "VP3+"):
                    for command in (
                        ("glomerulus", glomerulus, "--dataset", dataset, "--csv"),
                        ("pns", "--dataset", dataset, "--glomerulus", glomerulus, "--csv"),
                        (
                            "inputs",
                            "--dataset",
                            dataset,
                            "--target-class",
                            "PN",
                            "--source-class",
                            "ORN",
                            "--glomerulus",
                            glomerulus,
                            "--by-side",
                            "--csv",
                        ),
                        (
                            "pathway",
                            "ORN",
                            "PN",
                            "--dataset",
                            dataset,
                            "--source-glomerulus",
                            glomerulus,
                            "--target-glomerulus",
                            glomerulus,
                            "--by-side",
                            "--csv",
                        ),
                        ("orn-inputs", "--dataset", dataset, "--glomerulus", glomerulus, "--by-side", "--csv"),
                    ):
                        assert_nonempty_cli(
                            self,
                            run_cli("olf", "--store", str(store), *command),
                        )

                for target_class, region in (
                    ("LHN", "LH"),
                    ("KC", "MB"),
                    ("MBON", "MB"),
                    ("DAN", "MB"),
                    ("APL", "MB"),
                ):
                    assert_nonempty_cli(
                        self,
                        run_cli(
                            "olf",
                            "--store",
                            str(store),
                            "outputs",
                            "--dataset",
                            dataset,
                            "--source-class",
                            "PN",
                            "--target-class",
                            target_class,
                            "--region",
                            region,
                            "--by-side",
                            "--csv",
                        ),
                    )

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
    def test_olfaction_full_build_rebuilds_after_subset_cache(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "fixture.duckdb"
            for path, table in [
                ("tests/fixtures/bulk/flywire_olf_connections.csv", "flywire_proofread_connections"),
                ("tests/fixtures/bulk/hemibrain_neurons.csv", "hemibrain_traced_neurons"),
                ("tests/fixtures/bulk/flywire_hierarchical.csv", "flywire_hierarchical_neuron_annotations"),
                ("tests/fixtures/bulk/flywire_neuron_info.csv", "flywire_neuron_information_v2"),
            ]:
                import_to_duckdb(Path(path), table, store=store, replace=True)
            with duckdb.connect(str(store)) as connection:
                connection.execute(
                    """
                    CREATE TABLE hemibrain_traced_roi_connections(
                        bodyId_pre BIGINT,
                        bodyId_post BIGINT,
                        roi VARCHAR,
                        weight BIGINT
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO hemibrain_traced_roi_connections VALUES
                        (1001, 2001, 'AL(R)', 11)
                    """
                )

            build_olfaction_cache(store=store, datasets=None, replace=True, skip_current=True)
            build_olfaction_cache(store=store, datasets=["flywire"], replace=True, skip_current=True)
            rebuilt = build_olfaction_cache(store=store, datasets=None, replace=True, skip_current=True)

            with duckdb.connect(str(store), read_only=True) as connection:
                datasets = {
                    row[0]
                    for row in connection.execute(
                        "SELECT DISTINCT dataset FROM olf_provenance"
                    ).fetchall()
                }

        self.assertEqual(datasets, {"flywire", "hemibrain"})
        self.assertTrue(
            any(row["table"] == "olf_neurons" and row["status"] == "built" for row in rebuilt)
        )

    @unittest.skipIf(importlib.util.find_spec("duckdb") is None, "duckdb not installed")
    def test_olfaction_query_rebuilds_stale_flywire_annotations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "fixture.duckdb"
            import_to_duckdb(
                Path("tests/fixtures/bulk/flywire_olf_connections.csv"),
                "flywire_proofread_connections",
                store=store,
                replace=True,
            )
            build_olfaction_cache(store=store, datasets=["flywire"], replace=True)
            for path, table in [
                ("tests/fixtures/bulk/flywire_hierarchical.csv", "flywire_hierarchical_neuron_annotations"),
                ("tests/fixtures/bulk/flywire_neuron_info.csv", "flywire_neuron_information_v2"),
            ]:
                import_to_duckdb(Path(path), table, store=store, replace=True)

            output, progress = run_cli_capture(
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

        self.assertIn("rebuilt stale derived annotation tables", progress)
        self.assertIn("flywire,2001,DM1_lPN_R,PN,DM1,ORN,DM1,1,12", output)

    @unittest.skipIf(importlib.util.find_spec("duckdb") is None, "duckdb not installed")
    def test_olfaction_warns_when_hemibrain_compact_cache_lacks_orn_glomerulus(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "fixture.duckdb"
            with duckdb.connect(str(store)) as connection:
                connection.execute(
                    """
                    CREATE TABLE hemibrain_traced_roi_connections(
                        bodyId_pre BIGINT,
                        bodyId_post BIGINT,
                        roi VARCHAR,
                        weight BIGINT
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO hemibrain_traced_roi_connections VALUES
                        (3001, 2001, 'AL(R)', 8)
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE hemibrain_traced_neurons(
                        bodyId BIGINT,
                        type VARCHAR,
                        instance VARCHAR
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO hemibrain_traced_neurons VALUES
                        (2001, 'DM1_lPN', 'DM1_lPN_R'),
                        (3001, 'lLN1', 'lLN1_R')
                    """
                )
            build_olfaction_cache(store=store, datasets=["hemibrain"], replace=True)

            output, progress = run_cli_capture(
                "olf",
                "--store",
                str(store),
                "inputs",
                "--dataset",
                "hemibrain",
                "--target-class",
                "PN",
                "--source-class",
                "ORN",
                "--glomerulus",
                "DM1",
                "--format",
                "json",
            )

        self.assertEqual(output.strip(), "[]")
        self.assertIn("no hemibrain ORN rows in compact traced-adjacency cache", progress)

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


BROAD_GLOMERULI = (
    "D",
    "DA1",
    "DA2",
    "DA3",
    "DA4l",
    "DA4m",
    "DC1",
    "DC2",
    "DC3",
    "DC4",
    "DL1",
    "DL2d",
    "DL2v",
    "DL3",
    "DL4",
    "DL5",
    "DM1",
    "DM2",
    "DM3",
    "DM4",
    "DM5",
    "DM6",
    "DP1l",
    "DP1m",
    "M",
    "V",
    "VA1d",
    "VA1v",
    "VA2",
    "VA3",
    "VA4",
    "VA5",
    "VA6",
    "VA7l",
    "VA7m",
    "VC1",
    "VC2",
    "VC3",
    "VC4",
    "VC5",
    "VL1",
    "VL2a",
    "VL2p",
    "VM1",
    "VM2",
    "VM3",
    "VM4",
    "VM5d",
    "VM5v",
    "VM6",
    "VM7d",
    "VM7v",
    "VP1d",
    "VP1l",
    "VP1m",
    "VP2",
    "VP2+",
    "VP3+",
    "VP4",
    "VP4+",
    "VP5+",
)


def create_olfaction_matrix_fixture(connection) -> None:
    flywire_edges = []
    flywire_annotations = []
    hemibrain_edges = []
    hemibrain_neurons = []
    hemibrain_orn_pn_edges = []
    for index, glomerulus in enumerate(BROAD_GLOMERULI):
        flywire_orn = 100000 + index
        flywire_pn = 200000 + index
        hemibrain_orn = 300000 + index
        hemibrain_pn = 400000 + index
        side = "R" if index % 2 == 0 else "L"
        pn_suffix = ("lPN", "adPN", "vPN")[index % 3]
        flywire_edges.append((flywire_orn, flywire_pn, f"AL_{side}", 10 + index % 7))
        flywire_annotations.extend(
            [
                (flywire_orn, "cell_type", f"ORN_{glomerulus}_{side}"),
                (flywire_pn, "cell_type", f"{glomerulus}_{pn_suffix}_{side}"),
            ]
        )
        hemibrain_orn_pn_edges.append(
            (
                hemibrain_orn,
                hemibrain_pn,
                f"AL({side})",
                10 + index % 7,
                f"ORN_{glomerulus}",
                f"ORN_{glomerulus}_{side}",
                f"{glomerulus}_{pn_suffix}",
                f"{glomerulus}_{pn_suffix}_{side}",
            )
        )

    first_flywire_pn = 200000
    first_hemibrain_pn = 400000
    flywire_edges.extend(
        [
            (900001, first_flywire_pn, "AL_R", 3),
            (first_flywire_pn, 900002, "LH_R", 11),
            (first_flywire_pn, 900003, "MB_CA_R", 13),
            (first_flywire_pn, 900004, "MB_CA_R", 6),
            (first_flywire_pn, 900005, "MB_PED_R", 5),
            (first_flywire_pn, 900006, "MB_CA_R", 4),
        ]
    )
    flywire_annotations.extend(
        [
            (900001, "cell_type", "lLN1_R"),
            (900002, "cell_type", "LHCENT12_R"),
            (900003, "cell_type", "KCg_R"),
            (900004, "cell_type", "MBON01_R"),
            (900005, "cell_type", "PAM01_R"),
            (900006, "cell_type", "APL_R"),
        ]
    )
    hemibrain_edges.extend(
        [
            (910001, first_hemibrain_pn, "AL(R)", 3),
            (first_hemibrain_pn, 910002, "LH(R)", 11),
            (first_hemibrain_pn, 910003, "CA(R)", 13),
            (first_hemibrain_pn, 910004, "CA(R)", 6),
            (first_hemibrain_pn, 910005, "PED(R)", 5),
            (first_hemibrain_pn, 910006, "CA(R)", 4),
        ]
    )
    hemibrain_neurons.extend(
        [
            (910001, "lLN1", "lLN1_R"),
            (910002, "LHCENT12", "LHCENT12_R"),
            (910003, "KCg", "KCg_R"),
            (910004, "MBON01", "MBON01_R"),
            (910005, "PAM01", "PAM01_R"),
            (910006, "APL", "APL_R"),
        ]
    )

    connection.execute(
        """
        CREATE TABLE flywire_proofread_connections(
            pre_pt_root_id BIGINT,
            post_pt_root_id BIGINT,
            neuropil VARCHAR,
            syn_count BIGINT
        )
        """
    )
    connection.executemany("INSERT INTO flywire_proofread_connections VALUES (?, ?, ?, ?)", flywire_edges)
    connection.execute(
        """
        CREATE TABLE flywire_hierarchical_neuron_annotations(
            pt_root_id BIGINT,
            classification_system VARCHAR,
            cell_type VARCHAR
        )
        """
    )
    connection.executemany("INSERT INTO flywire_hierarchical_neuron_annotations VALUES (?, ?, ?)", flywire_annotations)
    connection.execute(
        """
        CREATE TABLE hemibrain_traced_roi_connections(
            bodyId_pre BIGINT,
            bodyId_post BIGINT,
            roi VARCHAR,
            weight BIGINT
        )
        """
    )
    connection.executemany("INSERT INTO hemibrain_traced_roi_connections VALUES (?, ?, ?, ?)", hemibrain_edges)
    connection.execute(
        """
        CREATE TABLE hemibrain_traced_neurons(
            bodyId BIGINT,
            type VARCHAR,
            instance VARCHAR
        )
        """
    )
    connection.executemany("INSERT INTO hemibrain_traced_neurons VALUES (?, ?, ?)", hemibrain_neurons)
    connection.execute(
        f"""
        CREATE TABLE {HEMIBRAIN_OLFACTION_ORN_PN_TABLE}(
            bodyId_pre BIGINT,
            bodyId_post BIGINT,
            roi VARCHAR,
            weight BIGINT,
            pre_type VARCHAR,
            pre_instance VARCHAR,
            post_type VARCHAR,
            post_instance VARCHAR
        )
        """
    )
    connection.executemany(
        f"INSERT INTO {HEMIBRAIN_OLFACTION_ORN_PN_TABLE} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        hemibrain_orn_pn_edges,
    )


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


def assert_nonempty_cli(test: unittest.TestCase, output: str) -> None:
    rows = [line for line in output.splitlines() if line.strip()]
    test.assertGreater(len(rows), 1, output)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
