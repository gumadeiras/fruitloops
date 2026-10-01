from __future__ import annotations

import csv
import hashlib
import importlib.util
import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from importlib import resources
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from fruitloops.bulk import (
    BULK_SOURCES,
    FLYWIRE_ANNOTATIONS_COMMIT,
    BulkSource,
    download_source,
    list_sources,
    setup_flywire_bulk,
)
from fruitloops.cli import main
from fruitloops.cli_wholebrain import PATH_COLUMNS, REACH_NEURON_COLUMNS, REACH_TYPE_COLUMNS, SIDE_COLUMNS
from fruitloops.curated import family_glomeruli, glomerulus_families, transmitter_overrides
from fruitloops.graph_cache import build_graph_cache, graph_cache_path
from fruitloops.table_import import import_to_duckdb

HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None

# Fixture graph. Ids: PNs 1-2, ORNs 11-13, upstream input 14, relays 21-26,
# descending targets 31-34, sub-threshold noise neurons 41-44.
NEURONS = [
    (1, "DA1_lPN", "left", "central", "ALPN", "uniglomerular", "DA1_lPN", "acetylcholine"),
    (2, "DA1_lPN", "right", "central", "ALPN", "uniglomerular", "DA1_lPN", "acetylcholine"),
    (11, "ORN_DA1", "left", "sensory", "olfactory", "", "", "acetylcholine"),
    (12, "ORN_DA1", "right", "sensory", "olfactory", "", "", "acetylcholine"),
    (13, "ORN_VL1", "left", "sensory", "olfactory", "", "", "acetylcholine"),
    (14, "XIN", "left", "central", "", "", "", "acetylcholine"),
    (21, "LHA", "left", "central", "", "", "", "acetylcholine"),
    (22, "LHB", "left", "central", "", "", "", "gaba"),
    (23, "KCg", "left", "central", "Kenyon_Cell", "", "", "dopamine"),
    (24, "MBON01", "left", "central", "MBON", "", "", "glutamate"),
    (25, "DAx", "left", "central", "DAN", "", "", "dopamine"),
    (26, "LHB", "right", "central", "", "", "", "acetylcholine"),
    (31, "DNx01", "left", "descending", "", "", "", "acetylcholine"),
    (32, "DNx01", "right", "descending", "", "", "", "acetylcholine"),
    (33, "DNx02", "left", "descending", "", "", "", "acetylcholine"),
    (34, "DNx01", "left", "descending", "", "", "", "acetylcholine"),
] + [(noise, "NOISE", "left", "central", "", "", "", "acetylcholine") for noise in (41, 42, 43, 44)]

EDGES = [
    # Inputs to the PNs (seed denominators): in(P1) = 100, in(P2) = 200.
    (11, 1, "AL_L", 30),
    (13, 1, "AL_L", 10),
    (14, 1, "AL_L", 60),
    (12, 2, "AL_R", 2),
    (11, 2, "AL_R", 2),
    (21, 2, "SLP_R", 100),
    (14, 2, "AL_R", 96),
    # First hops. P1 -> B has 6 LH + 2 SLP synapses (pair total 8); P1 -> K has 12 MB + 3 SLP.
    (1, 21, "LH_L", 10),
    (2, 21, "LH_R", 4),
    (1, 22, "LH_L", 6),
    (1, 22, "SLP_L", 2),
    (2, 22, "SMP_R", 20),
    (1, 23, "MB_CA_L", 12),
    (1, 23, "SLP_L", 3),
    (1, 25, "LH_L", 10),
    # Relays.
    (21, 22, "SLP_L", 5),
    (23, 24, "MB_ML_L", 20),
    (21, 31, "LAL_L", 10),
    (22, 31, "LAL_L", 20),
    (24, 31, "LAL_L", 15),
    (22, 32, "LAL_R", 10),
    (21, 33, "LAL_L", 5),
    (25, 33, "LAL_L", 15),
] + [(noise, 31, "LAL_L", 3) for noise in (41, 42, 43, 44)] + [(noise, 32, "LAL_R", 3) for noise in (41, 42)]

# Input totals over all partners (no threshold).
IN_A, IN_B, IN_K, IN_M, IN_D = 14, 33, 15, 20, 10
IN_T1, IN_T2, IN_T3, IN_P2 = 57, 16, 20, 200
W_P1A, W_P1B, W_P1B_LH, W_P2B = 10 / IN_A, 8 / IN_B, 6 / IN_B, 20 / IN_B
W_AB, W_AP2, W_KM, W_P1D = 5 / IN_B, 100 / IN_P2, 20 / IN_M, 10 / IN_D
W_AT1, W_BT1, W_MT1, W_BT2 = 10 / IN_T1, 20 / IN_T1, 15 / IN_T1, 10 / IN_T2
W_AT3, W_DT3 = 5 / IN_T3, 15 / IN_T3


def write_flywire_fixture(store: Path) -> None:
    import duckdb

    with duckdb.connect(str(store)) as connection:
        connection.execute(
            """
            CREATE TABLE flywire_proofread_connections(
                pre_pt_root_id BIGINT, post_pt_root_id BIGINT, neuropil VARCHAR, syn_count BIGINT
            )
            """
        )
        connection.executemany("INSERT INTO flywire_proofread_connections VALUES (?, ?, ?, ?)", EDGES)
        connection.execute(
            """
            CREATE TABLE flywire_neuron_annotations(
                root_id BIGINT, cell_type VARCHAR, side VARCHAR, super_class VARCHAR, cell_class VARCHAR,
                cell_sub_class VARCHAR, hemibrain_type VARCHAR, top_nt VARCHAR
            )
            """
        )
        connection.executemany(
            "INSERT INTO flywire_neuron_annotations VALUES (?, ?, ?, ?, nullif(?, ''), nullif(?, ''), nullif(?, ''), ?)",
            NEURONS,
        )


def write_hemibrain_fixture(store: Path) -> None:
    import duckdb

    with duckdb.connect(str(store)) as connection:
        connection.execute(
            "CREATE TABLE hemibrain_traced_roi_connections(bodyId_pre BIGINT, bodyId_post BIGINT, roi VARCHAR, weight BIGINT)"
        )
        connection.executemany(
            "INSERT INTO hemibrain_traced_roi_connections VALUES (?, ?, ?, ?)",
            [
                (101, 201, "LH(R)", 8),
                (101, 201, "SLP(R)", 2),
                (101, 202, "SMP(R)", 9),
                (201, 301, "LAL(R)", 12),
                (202, 301, "LAL(R)", 4),
                (202, 301, "CRE(R)", 4),
            ],
        )
        connection.execute("CREATE TABLE hemibrain_traced_neurons(bodyId BIGINT, type VARCHAR, instance VARCHAR)")
        connection.executemany(
            "INSERT INTO hemibrain_traced_neurons VALUES (?, ?, ?)",
            [
                (101, "DA1_lPN", "DA1_lPN_R"),
                (201, "LHX", "LHX_R"),
                (202, "SMPX", "SMPX_R"),
                (301, "DNa02", "DNa02_R"),
            ],
        )


def run_cli_rows(*args: str) -> tuple[list[dict[str, str]], str, str]:
    output, errors = StringIO(), StringIO()
    with redirect_stdout(output), redirect_stderr(errors):
        result = main(list(args))
    if result != 0:
        raise AssertionError(f"CLI exited with {result}")
    text = output.getvalue()
    return list(csv.DictReader(StringIO(text))), text, errors.getvalue()


def cli_error(*args: str) -> str:
    try:
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            main(list(args))
    except SystemExit as error:
        return str(error.code)
    raise AssertionError(f"expected an error from: {' '.join(args)}")


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
class WholeBrainFixtureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Path(self.tmp.name) / "fixture.duckdb"
        write_flywire_fixture(self.store)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def paths(self, *extra: str) -> list[dict[str, str]]:
        rows, _, _ = run_cli_rows(
            "paths", "--flywire", "--store", str(self.store), "--source-class", "ALPN", *extra, "--csv"
        )
        return rows

    def reach(self, *extra: str) -> list[dict[str, str]]:
        rows, _, _ = run_cli_rows(
            "reach", "--flywire", "--store", str(self.store), "--source-class", "ALPN",
            "--target-super-class", "descending", *extra, "--csv",
        )
        return rows

    def assertValue(self, text: str, expected: float) -> None:
        self.assertAlmostEqual(float(text), expected, delta=abs(expected) * 1e-5 + 1e-12)

    def test_strongest_path_differs_from_fewest_hops_and_ranks_last_relays(self) -> None:
        rows = self.paths("--target-id", "31")

        self.assertEqual([row["path_types"] for row in rows], [
            "DA1_lPN > KCg > MBON01 > DNx01",
            "DA1_lPN > LHB > DNx01",
            "DA1_lPN > LHA > DNx01",
        ])
        self.assertEqual([row["path_ids"] for row in rows], ["1 > 23 > 24 > 31", "2 > 22 > 31", "1 > 21 > 31"])
        self.assertValue(rows[0]["strength"], 1 * W_KM * W_MT1)
        self.assertValue(rows[1]["strength"], W_P2B * W_BT1)
        self.assertValue(rows[2]["strength"], W_P1A * W_AT1)
        self.assertEqual([row["hops"] for row in rows], ["3", "2", "2"])
        self.assertEqual({row["shortest_hops"] for row in rows}, {"2"})
        self.assertEqual(rows[1]["relation"], "contra")

    def test_weights_use_all_input_synapses_as_denominator(self) -> None:
        row = self.paths("--target-id", "31", "--top", "3")[2]

        self.assertEqual(row["step_synapses"], "10 > 10")
        self.assertValue(row["step_weights"].split(" > ")[1], 10 / 57)
        self.assertNotAlmostEqual(float(row["step_weights"].split(" > ")[1]), 10 / 45, places=3)

    def test_routes_restrict_first_hop_synapses(self) -> None:
        lh = self.paths("--target-id", "32", "--via", "LH", "--top", "1")[0]
        other = self.paths("--target-id", "31", "--via", "other")
        mb = self.paths("--target-id", "31", "--via", "MB")[0]
        kc = self.paths("--target-id", "31", "--via", "kc")[0]

        self.assertEqual(lh["path_types"], "DA1_lPN > LHB > DNx01")
        self.assertEqual(lh["step_synapses"], "6 > 10")
        self.assertValue(lh["step_weights"].split(" > ")[0], W_P1B_LH)
        self.assertValue(lh["strength"], W_P1B_LH * W_BT2)
        self.assertEqual([row["path_ids"] for row in other], ["2 > 22 > 31", "1 > 23 > 24 > 31"])
        self.assertValue(other[1]["strength"], 3 / IN_K * W_KM * W_MT1)
        self.assertEqual(mb["step_synapses"], "12 > 20 > 15")
        self.assertValue(mb["strength"], 12 / IN_K * W_KM * W_MT1)
        self.assertValue(kc["strength"], 15 / IN_K * W_KM * W_MT1)
        self.assertEqual(kc["route"], "kc")

    def test_paths_never_reenter_through_sources(self) -> None:
        rows = self.paths("--target-id", "31,32", "--via", "LH")
        by_target = {}
        for row in rows:
            by_target.setdefault(row["target_id"], []).append(row)

        for row in rows:
            self.assertNotIn(" 2 >", f" {row['path_ids']}")
        self.assertEqual(by_target["32"][0]["path_ids"], "1 > 22 > 32")
        self.assertValue(by_target["32"][0]["strength"], W_P1B_LH * W_BT2)
        self.assertEqual([row["path_ids"] for row in by_target["31"]], ["1 > 21 > 31", "1 > 22 > 31"])
        self.assertValue(by_target["31"][1]["strength"], W_P1B_LH * W_BT1)

    def test_signs_use_kenyon_cell_override_and_signed_search(self) -> None:
        unsigned = self.paths("--target-id", "31,33", "--top", "1")
        signed = self.paths("--target-id", "31,33", "--top", "1", "--signed")
        neurons, _, _ = run_cli_rows("neurons", "--flywire", "--store", str(self.store), "--type", "KCg,LHB", "--csv")

        kc_path, dan_path = unsigned
        self.assertEqual(kc_path["transmitters"], "acetylcholine > acetylcholine > glutamate > acetylcholine")
        self.assertEqual(kc_path["sign"], "-1")
        self.assertValue(kc_path["signed_strength"], -W_KM * W_MT1)
        self.assertEqual(dan_path["path_types"], "DA1_lPN > DAx > DNx02")
        self.assertEqual(dan_path["sign"], "0")
        self.assertEqual(signed[0]["path_types"], "DA1_lPN > KCg > MBON01 > DNx01")
        self.assertEqual(signed[1]["path_types"], "DA1_lPN > LHA > DNx02")
        self.assertEqual(signed[1]["sign"], "1")
        self.assertValue(signed[1]["strength"], W_P1A * W_AT3)
        self.assertEqual({row["type"]: row["top_nt"] for row in neurons if row["type"] == "KCg"}, {"KCg": "dopamine"})
        self.assertEqual([row["transmitter"] for row in neurons if row["type"] == "KCg"], ["acetylcholine"])
        self.assertEqual({row["type_sign_conflict"] for row in neurons if row["type"] == "LHB"}, {"true"})
        lhb = self.paths("--target-id", "32", "--top", "1")[0]
        self.assertEqual(lhb["sign_conflict_types"], "LHB")

    def test_orn_seed_weights_use_all_orn_synapses(self) -> None:
        orco = self.paths("--target-id", "31", "--orn-family", "orco")
        ir = self.paths("--target-id", "31", "--orn-family", "ir", "--top", "1")[0]
        right = self.paths("--target-id", "32", "--orn-glomerulus", "DA1", "--orn-side", "right", "--top", "1")[0]
        left, _, _ = run_cli_rows(
            "reach", "--flywire", "--store", str(self.store), "--source-class", "ALPN", "--target-id", "22",
            "--orn-glomerulus", "DA1", "--orn-side", "left", "--hops", "1", "--per-neuron", "--csv",
        )
        reach = self.reach("--orn-family", "orco", "--hops", "2", "--per-neuron")

        self.assertEqual([row["path_ids"] for row in orco], ["1 > 23 > 24 > 31", "1 > 21 > 31", "1 > 22 > 31"])
        self.assertEqual(orco[0]["seed_weight"], "0.3")
        self.assertValue(orco[0]["strength"], 0.3 * W_KM * W_MT1)
        self.assertValue(orco[2]["strength"], 0.3 * W_P1B * W_BT1)
        self.assertValue(ir["seed_weight"], 10 / 100)
        self.assertEqual((right["source_id"], right["seed_weight"]), ("2", "0.01"))
        self.assertValue(left[0]["reach"], 0.3 * W_P1B + (2 / IN_P2) * W_P2B)
        t2 = next(row for row in reach if row["target_id"] == "32")
        self.assertValue(t2["reach"], 0.3 * W_P1B * W_BT2 + (4 / IN_P2) * W_P2B * W_BT2)

    def test_reach_sums_paths_and_ranks_type_means(self) -> None:
        rows = self.reach("--hops", "1,2,3,4")
        by_key = {(row["hop"], row["target_type"]): row for row in rows}
        t1_2 = W_P1A * W_AT1 + (W_P1B + W_P2B) * W_BT1
        t2_2 = (W_P1B + W_P2B) * W_BT2
        t1_3 = W_KM * W_MT1 + W_P1A * W_AB * W_BT1
        t2_3 = W_P1A * W_AB * W_BT2

        self.assertEqual(list(rows[0].keys()), REACH_TYPE_COLUMNS)
        self.assertValue(by_key[("1", "DNx01")]["reach"], 0.0)
        self.assertValue(by_key[("2", "DNx01")]["reach"], (t1_2 + t2_2 + 0.0) / 3)
        self.assertValue(by_key[("2", "DNx02")]["reach"], W_P1A * W_AT3 + W_P1D * W_DT3)
        self.assertEqual(by_key[("2", "DNx02")]["rank"], "1")
        self.assertEqual(by_key[("2", "DNx01")]["rank"], "2")
        self.assertEqual(by_key[("2", "DNx01")]["neurons"], "3")
        self.assertValue(by_key[("3", "DNx01")]["reach"], (t1_3 + t2_3) / 3)
        self.assertEqual(by_key[("3", "DNx01")]["rank"], "1")
        self.assertValue(by_key[("4", "DNx01")]["reach"], 0.0)
        self.assertEqual(by_key[("4", "DNx01")]["rank_of"], "2")

    def test_reach_routes_add_up_to_all_route(self) -> None:
        rows = self.reach("--hops", "2", "--per-neuron", "--by-route")
        values = {(row["route"], row["target_id"]): float(row["reach"]) for row in rows}

        self.assertValue(str(values[("LH", "31")]), W_P1A * W_AT1 + W_P1B_LH * W_BT1)
        self.assertValue(str(values[("MB", "31")]), 0.0)
        self.assertValue(str(values[("kc", "31")]), 0.0)
        for target in ("31", "32", "33"):
            parts = sum(values[(route, target)] for route in ("AL", "LH", "MB", "other"))
            self.assertAlmostEqual(parts, values[("all", target)], places=12)

    def test_laterality_uses_per_side_means(self) -> None:
        row = next(
            row for row in self.reach("--hops", "2", "--by-side") if row["target_type"] == "DNx01"
        )
        left_t1, left_t2 = W_P1A * W_AT1 + W_P1B * W_BT1, W_P1B * W_BT2
        right_t1, right_t2 = W_P2B * W_BT1, W_P2B * W_BT2
        ipsi = (left_t1 + 0.0) / 2 + right_t2
        contra = left_t2 + (right_t1 + 0.0) / 2
        signed_ipsi = (W_P1A * W_AT1 - W_P1B * W_BT1) / 2 - W_P2B * W_BT2
        signed_contra = -W_P1B * W_BT2 - W_P2B * W_BT1 / 2

        self.assertEqual(list(row.keys()), REACH_TYPE_COLUMNS + ["left_neurons", "right_neurons"] + SIDE_COLUMNS)
        self.assertEqual((row["left_neurons"], row["right_neurons"]), ("2", "1"))
        self.assertValue(row["ipsi"], ipsi)
        self.assertValue(row["contra"], contra)
        self.assertValue(row["ai"], (ipsi - contra) / (ipsi + contra))
        self.assertValue(row["signed_net"], signed_ipsi - signed_contra)

    def test_paths_output_columns_and_unreachable_targets(self) -> None:
        rows, text, errors = run_cli_rows(
            "paths", "--flywire", "--store", str(self.store), "--source-class", "ALPN",
            "--target-type", "DNx01", "--max-hops", "1", "--csv",
        )
        reach_rows, _, _ = run_cli_rows(
            "reach", "--flywire", "--store", str(self.store), "--source-class", "ALPN",
            "--target-id", "31", "--per-neuron", "--hops", "2", "--csv",
        )

        self.assertEqual(rows, [])
        self.assertEqual(text.splitlines()[0], ",".join(PATH_COLUMNS))
        self.assertIn("no path within 1 hops for 2 target/route combinations", errors)
        self.assertIn("1 target neurons have no connections", errors)
        self.assertEqual(list(reach_rows[0].keys()), REACH_NEURON_COLUMNS)

    def test_selector_errors_name_vocabulary_and_close_matches(self) -> None:
        base = ["paths", "--flywire", "--store", str(self.store)]

        self.assertIn("--source-class ALPN", cli_error(*base, "--source-class", "PN", "--target-type", "DNx01"))
        self.assertIn("close matches: DNx01", cli_error(*base, "--source-class", "ALPN", "--target-type", "DNx1"))
        self.assertIn("matches no", cli_error(*base, "--source-class", "ALPN", "--target-type", "ZZ*"))
        self.assertIn(
            "selects no flywire neurons",
            cli_error(*base, "--source-class", "ALPN", "--source-type", "LHA", "--target-type", "DNx01"),
        )
        self.assertIn("--target-type", cli_error(*base, "--source-class", "ALPN"))
        self.assertIn("unknown flywire ids: 999", cli_error(*base, "--source-id", "999", "--target-id", "31"))
        self.assertIn(
            "close matches: DA1",
            cli_error(*base, "--source-class", "ALPN", "--target-id", "31", "--orn-glomerulus", "DA11"),
        )
        self.assertIn(
            "--orn-side needs",
            cli_error(*base, "--source-class", "ALPN", "--target-id", "31", "--orn-side", "left"),
        )
        self.assertIn("between 1 and", cli_error(*base, "--source-class", "ALPN", "--target-id", "31", "--max-hops", "0"))
        self.assertIn(
            "drop --orn-side",
            cli_error(
                "reach", "--flywire", "--store", str(self.store), "--source-class", "ALPN", "--target-id", "31",
                "--by-side", "--orn-family", "orco", "--orn-side", "left",
            ),
        )

    def test_neurons_lookup_and_graph_stage_status(self) -> None:
        rows, _, _ = run_cli_rows("neurons", "--flywire", "--store", str(self.store), "--type", "DNx*", "--csv")
        missing, _, _ = run_cli_rows("status", "--store", str(self.store), "--csv")
        first = build_graph_cache(self.store, "flywire", skip_current=True)
        second = build_graph_cache(self.store, "flywire", skip_current=True)
        current, _, _ = run_cli_rows("status", "--store", str(self.store), "--csv")

        self.assertEqual([row["id"] for row in rows], ["31", "32", "33", "34"])
        self.assertIn(("graph", "flywire", "missing"), {(r["section"], r["name"], r["value"]) for r in missing})
        self.assertEqual(first["status"], str(len({(pre, post) for pre, post, _, _ in EDGES})))
        self.assertEqual(second["status"], f"current:{first['status']}")
        self.assertTrue(graph_cache_path(self.store, "flywire").exists())
        flywire = next(row for row in current if row["section"] == "graph" and row["name"] == "flywire")
        self.assertTrue(flywire["value"].startswith("current: 18 neurons"))

        import duckdb

        with duckdb.connect(str(self.store)) as connection:
            connection.execute("INSERT INTO flywire_proofread_connections VALUES (21, 33, 'LAL_L', 5)")
        stale, _, _ = run_cli_rows("status", "--store", str(self.store), "--csv")
        rebuilt = build_graph_cache(self.store, "flywire", skip_current=True)

        self.assertTrue(next(r for r in stale if r["name"] == "flywire" and r["section"] == "graph")["value"].startswith("stale"))
        self.assertEqual(rebuilt["status"], first["status"])
        _, _, errors = run_cli_rows(
            "paths", "--flywire", "--store", str(self.store), "--source-class", "ALPN", "--target-id", "33", "--csv"
        )
        self.assertNotIn("graph cache", errors)


    def test_orn_weighted_laterality_uses_antenna_side_seeds(self) -> None:
        row = next(
            row
            for row in self.reach("--hops", "2", "--by-side", "--orn-family", "orco")
            if row["target_type"] == "DNx01"
        )
        # Orco seeds: P1 gets 30 DA1 synapses from the left antenna; P2 gets 2 from each side.
        p1_left, p2_side = 30 / 100, 2 / IN_P2
        left_t1 = p1_left * (W_P1A * W_AT1 + W_P1B * W_BT1) + p2_side * W_P2B * W_BT1
        left_t2 = (p1_left * W_P1B + p2_side * W_P2B) * W_BT2
        right_t1, right_t2 = p2_side * W_P2B * W_BT1, p2_side * W_P2B * W_BT2
        ipsi = left_t1 / 2 + right_t2
        contra = left_t2 + right_t1 / 2

        self.assertValue(row["ipsi"], ipsi)
        self.assertValue(row["contra"], contra)
        self.assertValue(row["ai"], (ipsi - contra) / (ipsi + contra))

    def test_per_neuron_laterality_uses_soma_side_seeds(self) -> None:
        row = next(row for row in self.reach("--hops", "2", "--by-side", "--per-neuron") if row["target_id"] == "31")

        self.assertEqual(list(row.keys()), REACH_NEURON_COLUMNS + SIDE_COLUMNS)
        self.assertValue(row["ipsi"], W_P1A * W_AT1 + W_P1B * W_BT1)
        self.assertValue(row["contra"], W_P2B * W_BT1)

    def test_ranked_paths_never_pass_through_the_target(self) -> None:
        import duckdb

        # The best path to U runs through T, so U's rank uses its best path that avoids T.
        with duckdb.connect(str(self.store)) as connection:
            connection.execute("DELETE FROM flywire_proofread_connections")
            connection.executemany(
                "INSERT INTO flywire_proofread_connections VALUES (?, ?, ?, ?)",
                [(1, 31, "LH_L", 50), (31, 22, "LAL_L", 50), (22, 31, "LAL_L", 10), (21, 22, "LAL_L", 5), (1, 21, "LH_L", 10)],
            )
        rows = self.paths("--target-id", "31")

        self.assertEqual([row["path_ids"] for row in rows], ["1 > 31", "1 > 21 > 22 > 31"])
        self.assertValue(rows[0]["strength"], 50 / 60)
        self.assertValue(rows[1]["strength"], 10 / 10 * 5 / 55 * 10 / 60)

    def test_hop_limit_and_synapse_threshold_change_paths_not_denominators(self) -> None:
        two_hops = self.paths("--target-id", "31", "--max-hops", "2", "--top", "1")
        strict = self.paths("--target-id", "31", "--min-synapses", "16")

        self.assertEqual(two_hops[0]["path_types"], "DA1_lPN > LHB > DNx01")
        self.assertValue(two_hops[0]["strength"], W_P2B * W_BT1)
        # MBON01 -> DNx01 (15 synapses) is dropped; the kept edges keep their weights.
        self.assertEqual([row["path_types"] for row in strict], ["DA1_lPN > LHB > DNx01"])
        self.assertValue(strict[0]["strength"], W_P2B * W_BT1)

    def test_notes_for_excluded_sources_and_unconnected_targets(self) -> None:
        base = ["--flywire", "--store", str(self.store)]
        signed, _, signed_errors = run_cli_rows(
            "paths", *base, "--source-type", "DAx", "--target-id", "33", "--signed", "--csv"
        )
        _, _, reach_errors = run_cli_rows(
            "reach", *base, "--source-class", "ALPN", "--target-id", "34", "--hops", "2", "--csv"
        )

        self.assertEqual(signed, [])
        self.assertIn("--signed excludes 1 source neurons without a fast-transmitter sign", signed_errors)
        self.assertIn("1 target neurons have no connections in the flywire graph; their reach is 0", reach_errors)

    def test_rejects_conflicting_datasets_and_invalid_limits(self) -> None:
        conflict = cli_error("neurons", "--hemibrain", "--flywire", "--store", str(self.store), "--type", "DNx01")
        limit = cli_error("neurons", "--flywire", "--store", str(self.store), "--type", "DNx01", "--limit", "0")

        self.assertEqual(conflict, "2")
        self.assertEqual(limit, "--limit must be at least 1")


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
class HemibrainPathsTest(unittest.TestCase):
    def test_hemibrain_paths_use_type_selectors_and_hemibrain_rois(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "hemibrain.duckdb"
            write_hemibrain_fixture(store)
            rows, _, errors = run_cli_rows(
                "paths", "--hemibrain", "--store", str(store), "--source-type", "*_*PN*",
                "--target-type", "DNa02", "--via", "LH", "--via", "all", "--csv",
            )
            by_id, _, _ = run_cli_rows(
                "paths", "--hemibrain", "--store", str(store), "--source-id", "101", "--target-id", "301",
                "--top", "1", "--csv",
            )
            base = ["--hemibrain", "--store", str(store), "--target-type", "DNa02"]
            class_error = cli_error("paths", *base, "--source-class", "ALPN")
            orn_error = cli_error("paths", *base, "--source-type", "DA1_lPN", "--orn-family", "orco")
            signed_error = cli_error("paths", *base, "--source-type", "DA1_lPN", "--signed")
            side_error = cli_error("reach", *base, "--source-type", "DA1_lPN", "--by-side")
            olf_name_error = cli_error("paths", *base, "--source-type", "PN")

        self.assertIn("graph cache is missing", errors)
        self.assertEqual([(row["route"], row["path_types"]) for row in rows], [
            ("LH", "DA1_lPN > LHX > DNa02"),
            ("all", "DA1_lPN > LHX > DNa02"),
            ("all", "DA1_lPN > SMPX > DNa02"),
        ])
        self.assertEqual(rows[0]["step_synapses"], "8 > 12")
        self.assertEqual([(row["path_ids"], row["strength"]) for row in by_id], [(rows[1]["path_ids"], rows[1]["strength"])])
        self.assertAlmostEqual(float(rows[0]["strength"]), 8 / 10 * 12 / 20, places=6)
        self.assertAlmostEqual(float(rows[2]["strength"]), 9 / 9 * 8 / 20, places=6)
        self.assertEqual(rows[0]["target_side"], "right")
        self.assertEqual(rows[0]["sign"], "")
        self.assertIn("--source-type '*_*PN*'", class_error)
        self.assertIn("not supported for hemibrain", orn_error)
        self.assertIn("--signed is not supported for hemibrain", signed_error)
        self.assertIn("--by-side is not supported for hemibrain", side_error)
        self.assertIn("no class annotations", olf_name_error)
        self.assertIn("--source-type '*_*PN*'", olf_name_error)


class AnnotationSourceTest(unittest.TestCase):
    def test_flywire_annotations_are_pinned_to_a_commit_and_hash(self) -> None:
        source = next(row for row in list_sources() if row["kind"] == "neuron-annotations")

        self.assertIn(f"/flywire_annotations/{FLYWIRE_ANNOTATIONS_COMMIT}/", source["url"])
        self.assertNotIn("/main/", source["url"])
        self.assertEqual(len(source["sha256"]), 64)
        self.assertEqual(source["table_name"], "flywire_neuron_annotations")

    def test_download_verifies_sha256(self) -> None:
        content = b"root_id\tcell_type\n1\tDNa02\n"
        good = BulkSource("test", "tsv", "https://example.invalid/a.tsv", "a.tsv", "tsv", "t", "test",
                          hashlib.sha256(content).hexdigest())
        bad = BulkSource("test", "bad", "https://example.invalid/b.tsv", "b.tsv", "tsv", "t", "test", "0" * 64)
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(BULK_SOURCES, {("test", "tsv"): good, ("test", "bad"): bad}):
                with patch("fruitloops.bulk.urllib.request.urlopen", side_effect=lambda url: io.BytesIO(content)):
                    path = download_source("test", "tsv", output_dir=Path(tmp))
                    with self.assertRaisesRegex(ValueError, "sha256 mismatch"):
                        download_source("test", "bad", output_dir=Path(tmp))
                path.write_bytes(b"changed")
                with self.assertRaisesRegex(ValueError, "sha256 mismatch"):
                    download_source("test", "tsv", output_dir=Path(tmp))
            leftovers = sorted(item.name for item in (Path(tmp) / "test").iterdir())

        self.assertEqual(leftovers, ["a.tsv", "a.tsv.json"])

    @unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
    def test_setup_imports_annotations_as_own_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "store.duckdb"
            annotations = Path(tmp) / "annotations.tsv"
            annotations.write_text("root_id\tcell_type\tside\n720575940604737708\tDNa02\tright\n")
            connections = Path("tests/fixtures/bulk/flywire_olf_connections.csv").resolve()

            def fake_download(dataset, kind, output_dir):
                return annotations if kind == "neuron-annotations" else connections

            with patch("fruitloops.bulk.download_source", side_effect=fake_download):
                first = setup_flywire_bulk(Path(tmp) / "bulk", store, replace=True, skip_current=True)
                second = setup_flywire_bulk(Path(tmp) / "bulk", store, replace=True, skip_current=True)
            imported = import_to_duckdb(annotations, "check_types", store)
            import duckdb

            with duckdb.connect(str(store), read_only=True) as connection:
                kind = connection.execute("SELECT typeof(root_id) FROM check_types").fetchone()[0]

        self.assertIn(("import", "flywire_neuron_annotations", "1"), {(r["action"], r["target"], r["status"]) for r in first})
        self.assertIn(("import", "flywire_neuron_annotations", "current:1"), {(r["action"], r["target"], r["status"]) for r in second})
        self.assertEqual(imported["rows"], "1")
        self.assertEqual(kind, "BIGINT")


class CuratedTablesTest(unittest.TestCase):
    def test_glomerulus_families_cite_dois_and_exclude_unverified_rows(self) -> None:
        table = glomerulus_families()

        self.assertTrue(resources.files("fruitloops").joinpath("curated", "glomerulus_receptor_families.csv").is_file())
        for entry in table.values():
            dois = entry.doi.split("; ")
            self.assertEqual(len(dois), len(entry.source.split("; ")), entry.glomerulus)
            self.assertTrue(all(doi.startswith("10.") for doi in dois), entry.glomerulus)
        self.assertEqual(table["VL1"].family, "ir")
        self.assertEqual({table[name].family for name in ("VM6l", "VM6m", "VM6v")}, {"amt"})
        self.assertIn("DA1", family_glomeruli("orco"))
        self.assertTrue({"VL1", "VM6v", "DC4", "V"}.isdisjoint(family_glomeruli("orco")))
        self.assertFalse(table["VP1l"].verified)
        self.assertNotIn("VP1l", family_glomeruli("hygro"))
        self.assertNotIn("VP3b", family_glomeruli("thermo"))
        self.assertEqual(family_glomeruli("gr"), {"V"})

    def test_kenyon_cell_transmitter_override_cites_barnstedt(self) -> None:
        overrides = {(item.dataset, item.field, item.value): item for item in transmitter_overrides()}
        kc = overrides[("flywire", "cell_class", "Kenyon_Cell")]

        self.assertEqual(kc.transmitter, "acetylcholine")
        self.assertEqual(kc.doi, "10.1016/j.neuron.2016.02.015")


if __name__ == "__main__":
    unittest.main()
