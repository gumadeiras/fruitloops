from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fruitloops.curated import family_glomeruli, glomerulus_families, hemibrain_glomerulus_names
from test_wholebrain import HAS_DUCKDB, HEMIBRAIN_CLASS_SOURCE, cli_error, run_cli_rows, write_flywire_annotations

# Traced bodies: PNs 101 and 102, another input 103, DNa02 targets 301 (right) and 302 (left),
# and the traced ORN 501, which is in both the graph and the ORN table.
TRACED = [
    (101, "DA1_lPN", "DA1_lPN_R"),
    (102, "DM1_lPN", "DM1_lPN_R"),
    (103, "LNX", "LNX_R"),
    (301, "DNa02", "DNa02_R"),
    (302, "DNa02", "DNa02_L"),
    (501, "ORN_DA1", "ORN_DA1_L"),
]
# Graph input: 101 gets 60 + 10 = 70, 102 gets 40, 301 gets 20, and 302 gets 10.
EDGES = [
    (103, 101, "AL(R)", 60),
    (501, 101, "AL(R)", 10),
    (103, 102, "AL(R)", 40),
    (101, 301, "LAL(R)", 10),
    (102, 301, "LAL(R)", 10),
    (101, 302, "LAL(L)", 10),
]
# ORN table rows. v1.2 ORN_VC5 is VM6 (amt) and ORN_VC3l is VC3 (orco); body 999 is an untraced PN.
ORN_PN = [
    (601, 101, "AL(R)", 30, "ORN_DA1", "ORN_DA1_R", "DA1_lPN", "DA1_lPN_R"),
    (501, 101, "AL(R)", 10, "ORN_DA1", "ORN_DA1_L", "DA1_lPN", "DA1_lPN_R"),
    (602, 101, "AL(R)", 20, "ORN_VC5", "ORN_VC5", "DA1_lPN", "DA1_lPN_R"),
    (603, 102, "AL(R)", 20, "ORN_DM1", "ORN_DM1_R", "DM1_lPN", "DM1_lPN_R"),
    (604, 102, "AL(R)", 10, "ORN_VC3l", "ORN_VC3l_L", "DM1_lPN", "DM1_lPN_R"),
    (605, 999, "AL(R)", 7, "ORN_DM1", "ORN_DM1_R", "DM1_lPN", None),
]
# Seed denominators: graph input without ORN partners plus all table ORN synapses.
# 101: 70 - 10 (traced ORN 501, counted once) + 60 = 120. 102: 40 - 0 + 30 = 70.
TOTAL_101, TOTAL_102 = 120, 70
W_101_301, W_102_301, W_101_302 = 10 / 20, 10 / 20, 10 / 10


def write_store(store: Path, orn_table: bool = True) -> None:
    import duckdb

    with duckdb.connect(str(store)) as connection:
        connection.execute("CREATE TABLE hemibrain_traced_neurons(bodyId BIGINT, type VARCHAR, instance VARCHAR)")
        connection.executemany("INSERT INTO hemibrain_traced_neurons VALUES (?, ?, ?)", TRACED)
        connection.execute(
            "CREATE TABLE hemibrain_traced_roi_connections(bodyId_pre BIGINT, bodyId_post BIGINT, roi VARCHAR, weight BIGINT)"
        )
        connection.executemany("INSERT INTO hemibrain_traced_roi_connections VALUES (?, ?, ?, ?)", EDGES)
        connection.execute(
            "CREATE TABLE hemibrain_body_neurotransmitters(body UBIGINT, gaba DOUBLE, acetylcholine DOUBLE, "
            "glutamate DOUBLE, serotonin DOUBLE, octopamine DOUBLE, dopamine DOUBLE, neither DOUBLE)"
        )
        write_flywire_annotations(connection, HEMIBRAIN_CLASS_SOURCE)
        if orn_table:
            connection.execute(
                "CREATE TABLE hemibrain_olfaction_orn_pn_connections(bodyId_pre BIGINT, bodyId_post BIGINT, roi VARCHAR, "
                "weight BIGINT, pre_type VARCHAR, pre_instance VARCHAR, post_type VARCHAR, post_instance VARCHAR)"
            )
            connection.executemany(
                "INSERT INTO hemibrain_olfaction_orn_pn_connections VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ORN_PN
            )


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
class HemibrainOrnSeedTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Path(self.tmp.name) / "hemibrain.duckdb"
        write_store(self.store)
        self.base = ["--hemibrain", "--store", str(self.store), "--source-type", "*_*PN*"]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def assertValue(self, text: str, expected: float) -> None:
        self.assertAlmostEqual(float(text), expected, delta=abs(expected) * 1e-5 + 1e-12)

    def seeds(self, *flags: str) -> tuple[dict[str, str], str]:
        rows, _, errors = run_cli_rows("paths", *self.base, "--target-id", "301", "--top", "2", *flags, "--csv")
        return {row["source_id"]: row["seed_weight"] for row in rows}, errors

    def test_seed_is_the_input_fraction_from_the_chosen_orns(self) -> None:
        orco, _ = self.seeds("--orn-family", "orco")
        amt, _ = self.seeds("--orn-family", "amt")
        da1, _ = self.seeds("--orn-glomerulus", "DA1")
        right, _ = self.seeds("--orn-family", "orco", "--orn-side", "right")
        left, _ = self.seeds("--orn-family", "orco", "--orn-side", "left")

        self.assertValue(orco["101"], (30 + 10) / TOTAL_101)
        self.assertValue(orco["102"], (20 + 10) / TOTAL_102)
        self.assertEqual(set(amt), {"101"})
        self.assertValue(amt["101"], 20 / TOTAL_101)
        self.assertEqual(set(da1), {"101"})
        self.assertValue(da1["101"], 40 / TOTAL_101)
        self.assertValue(right["101"], 30 / TOTAL_101)
        self.assertValue(right["102"], 20 / TOTAL_102)
        self.assertValue(left["101"], 10 / TOTAL_101)
        self.assertValue(left["102"], 10 / TOTAL_102)

    def test_seeds_leave_graph_weights_unchanged(self) -> None:
        weighted, _, _ = run_cli_rows(
            "paths", *self.base, "--target-id", "301", "--orn-family", "orco", "--top", "2", "--csv"
        )
        plain, _, _ = run_cli_rows("paths", *self.base, "--target-id", "301", "--top", "2", "--csv")

        self.assertEqual([row["path_ids"] for row in weighted], ["102 > 301", "101 > 301"])
        self.assertEqual([row["step_weights"] for row in weighted], [row["step_weights"] for row in plain])
        self.assertEqual([row["step_weights"] for row in weighted], ["0.5", "0.5"])
        self.assertValue(weighted[0]["strength"], 30 / TOTAL_102 * W_102_301)
        self.assertValue(weighted[1]["strength"], 40 / TOTAL_101 * W_101_301)

    def test_reach_by_side_uses_the_orn_antenna_side(self) -> None:
        rows, _, _ = run_cli_rows(
            "reach", *self.base, "--target-type", "DNa02", "--orn-family", "orco", "--hops", "1", "--by-side", "--csv"
        )
        seed = {101: 40 / TOTAL_101, 102: 30 / TOTAL_102}
        left = {101: 10 / TOTAL_101, 102: 10 / TOTAL_102}
        right = {101: 30 / TOTAL_101, 102: 20 / TOTAL_102}

        def reach(seeds: dict[int, float]) -> tuple[float, float]:
            return seeds[101] * W_101_301 + seeds[102] * W_102_301, seeds[101] * W_101_302

        right_301, right_302 = reach(right)
        left_301, left_302 = reach(left)
        all_301, all_302 = reach(seed)
        ipsi, contra = left_302 + right_301, right_302 + left_301

        self.assertEqual(len(rows), 1)
        self.assertValue(rows[0]["reach"], (all_301 + all_302) / 2)
        self.assertValue(rows[0]["ipsi"], ipsi)
        self.assertValue(rows[0]["contra"], contra)
        self.assertValue(rows[0]["ai"], (ipsi - contra) / (ipsi + contra))

    def test_glomeruli_use_the_renamed_hemibrain_names(self) -> None:
        vc3, errors = self.seeds("--orn-glomerulus", "VC3")
        old_name = cli_error("paths", *self.base, "--target-id", "301", "--orn-glomerulus", "VC3l")
        ir = cli_error("paths", *self.base, "--target-id", "301", "--orn-family", "ir")

        self.assertEqual(set(vc3), {"102"})
        self.assertValue(vc3["102"], 10 / TOTAL_102)
        self.assertIn("hemibrain glomerulus VC3 is v1.2 type ORN_VC3l", errors)
        self.assertIn("use --orn-glomerulus VC3", old_name)
        # v1.2 ORN_VC5 is VM6 (amt), so no fixture ORN is in the ir family.
        self.assertEqual(ir, "no hemibrain ORNs match --orn-family ir")

    def test_unmatched_options_and_missing_table_are_clear_errors(self) -> None:
        target = ["--target-id", "301"]
        side = cli_error("paths", *self.base, *target, "--orn-glomerulus", "DM1", "--orn-side", "left")
        thermo = cli_error("paths", *self.base, *target, "--orn-family", "thermo")
        unknown = cli_error("paths", *self.base, *target, "--orn-glomerulus", "DA2")
        by_side = cli_error("reach", *self.base, *target, "--orn-family", "orco", "--orn-side", "left", "--by-side")
        write_store(Path(self.tmp.name) / "no-orns.duckdb", orn_table=False)
        no_table = ["--hemibrain", "--store", str(Path(self.tmp.name) / "no-orns.duckdb"), "--source-type", "*_*PN*"]
        missing = [
            cli_error("paths", *no_table, *target, "--orn-family", "orco"),
            cli_error("reach", *no_table, *target, "--orn-glomerulus", "DA1", "--by-side"),
        ]

        self.assertEqual(side, "no hemibrain ORNs match --orn-glomerulus DM1 --orn-side left")
        self.assertEqual(thermo, "no hemibrain ORNs match --orn-family thermo")
        self.assertIn("--orn-glomerulus 'DA2' has no hemibrain ORNs; close matches: DA1", unknown)
        self.assertIn("drop --orn-side", by_side)
        for error in missing:
            self.assertIn("missing hemibrain_olfaction_orn_pn_connections; run `fruitloops setup --hemibrain`", error)


class HemibrainGlomerulusNameTest(unittest.TestCase):
    def test_renames_cite_schlegel_2021_and_map_into_the_family_table(self) -> None:
        names = hemibrain_glomerulus_names()
        families = glomerulus_families()

        self.assertEqual(names, {"VC3l": "VC3", "VC3m": "VC5", "VC5": "VM6"})
        self.assertEqual({name: families[name].family for name in names.values()}, {"VC3": "orco", "VC5": "ir", "VM6": "amt"})
        self.assertEqual(families["VM6"].doi, "10.7554/eLife.66018")
        self.assertIn("VM6", family_glomeruli("amt"))


if __name__ == "__main__":
    unittest.main()
