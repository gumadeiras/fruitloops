from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fruitloops.neuron_labels import load_neuron_labels
from test_wholebrain import HAS_DUCKDB, cli_error, run_cli_rows, write_flywire_annotations

# Hemibrain bodies: (body, type, instance).
TRACED = [
    (101, "DA1_lPN", "DA1_lPN_R"),
    (102, "MAJ", "MAJ_R"),
    (103, "TIE", "TIE_R"),
    (104, "MOSTLYNONE", "MOSTLYNONE_R"),
    (105, "SPLIT3", "SPLIT3_R"),
    (106, "COMPA", "COMPA_R"),
    (107, "COMPB", "COMPB_L"),
    (108, "UNMATCHED", "UNMATCHED_R"),
    (109, "", ""),
    (110, "KCg-m", "KCg-m_R"),
    (111, "DNa02", "DNa02_R"),
    (112, "DNa02", "DNa02_L"),
]
EDGES = [(101, 102, "LH(R)", 10), (102, 111, "LAL(R)", 8), (102, 112, "LAL(L)", 6), (110, 103, "CA(R)", 5)]


def flywire(root: int, super_class: str, cell_class: str, hemibrain_type: str | None) -> tuple:
    return (root, f"fw{root}", "right", super_class, cell_class, "", hemibrain_type or "", "acetylcholine")


# FlyWire neurons and the hemibrain types they match. Expected classes, by the majority rule:
FLYWIRE = [
    # DA1_lPN: 2 of 2 ALPN -> ALPN, central.
    flywire(1, "central", "ALPN", "DA1_lPN"),
    flywire(2, "central", "ALPN", "DA1_lPN"),
    # MAJ: LHLN 2 of 3 -> LHLN; central 3 of 3.
    flywire(3, "central", "LHLN", "MAJ"),
    flywire(4, "central", "LHLN", "MAJ"),
    flywire(5, "central", "", "MAJ"),
    # TIE: LHLN 1 of 2 and LHCENT 1 of 2 -> no majority, cell class empty; central 2 of 2.
    flywire(6, "central", "LHLN", "TIE"),
    flywire(7, "central", "LHCENT", "TIE"),
    # MOSTLYNONE: unclassified 2 of 3 -> cell class empty; central 3 of 3.
    flywire(8, "central", "", "MOSTLYNONE"),
    flywire(9, "central", "", "MOSTLYNONE"),
    flywire(10, "central", "LHLN", "MOSTLYNONE"),
    # SPLIT3: three super classes of 1 each -> no majority, super class empty.
    flywire(11, "central", "", "SPLIT3"),
    flywire(12, "descending", "", "SPLIT3"),
    flywire(13, "ascending", "AN", "SPLIT3"),
    # One neuron matched to two types, with a space after the comma: it counts for both.
    flywire(14, "descending", "", "COMPA, COMPB"),
    # KCg-m: 3 of 3 Kenyon_Cell.
    flywire(15, "central", "Kenyon_Cell", "KCg-m"),
    flywire(16, "central", "Kenyon_Cell", "KCg-m"),
    flywire(17, "central", "Kenyon_Cell", "KCg-m"),
    # DNa02: 2 of 2 descending; unmatched FlyWire neurons and empty list items do not vote.
    flywire(18, "descending", "", "DNa02,"),
    flywire(19, "descending", "", "DNa02"),
    flywire(20, "central", "LHLN", None),
]
EXPECTED = {
    101: ("central", "ALPN"),
    102: ("central", "LHLN"),
    103: ("central", ""),
    104: ("central", ""),
    105: ("", ""),
    106: ("descending", ""),
    107: ("descending", ""),
    108: ("", ""),
    109: ("", ""),
    110: ("central", "Kenyon_Cell"),
    111: ("descending", ""),
    112: ("descending", ""),
}


def write_store(store: Path) -> None:
    import duckdb

    with duckdb.connect(str(store)) as connection:
        connection.execute("CREATE TABLE hemibrain_traced_neurons(bodyId BIGINT, type VARCHAR, instance VARCHAR)")
        connection.executemany("INSERT INTO hemibrain_traced_neurons VALUES (?, nullif(?, ''), nullif(?, ''))", TRACED)
        connection.execute(
            "CREATE TABLE hemibrain_traced_roi_connections(bodyId_pre BIGINT, bodyId_post BIGINT, roi VARCHAR, weight BIGINT)"
        )
        connection.executemany("INSERT INTO hemibrain_traced_roi_connections VALUES (?, ?, ?, ?)", EDGES)
        connection.execute(
            "CREATE TABLE hemibrain_body_neurotransmitters(body UBIGINT, gaba DOUBLE, acetylcholine DOUBLE, "
            "glutamate DOUBLE, serotonin DOUBLE, octopamine DOUBLE, dopamine DOUBLE, neither DOUBLE)"
        )
        write_flywire_annotations(connection, FLYWIRE)


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
class HemibrainClassTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Path(self.tmp.name) / "hemibrain.duckdb"
        write_store(self.store)
        self.base = ["--hemibrain", "--store", str(self.store)]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def neuron_ids(self, *flags: str) -> list[str]:
        rows, _, _ = run_cli_rows("neurons", *self.base, *flags, "--csv")
        return [row["id"] for row in rows]

    def test_each_type_gets_the_class_of_more_than_half_of_its_flywire_matches(self) -> None:
        labels = load_neuron_labels(self.store, "hemibrain")
        classes = {
            int(body): (super_class, cell_class)
            for body, super_class, cell_class in zip(
                labels.ids.tolist(), labels.field("super_class").tolist(), labels.field("cell_class").tolist()
            )
        }

        self.assertEqual(classes, EXPECTED)

    def test_class_selectors_use_the_flywire_vocabulary(self) -> None:
        self.assertEqual(self.neuron_ids("--class", "ALPN"), ["101"])
        self.assertEqual(self.neuron_ids("--class", "LHLN"), ["102"])
        self.assertEqual(self.neuron_ids("--class", "Kenyon_Cell"), ["110"])
        self.assertEqual(self.neuron_ids("--super-class", "descending"), ["106", "107", "111", "112"])
        self.assertEqual(self.neuron_ids("--super-class", "descending", "--type", "DN*"), ["111", "112"])

        rows, _, _ = run_cli_rows("neurons", *self.base, "--id", "101", "--csv")
        self.assertEqual((rows[0]["super_class"], rows[0]["cell_class"]), ("central", "ALPN"))

    def test_unknown_class_names_get_close_matches_and_olf_hints(self) -> None:
        folded = cli_error("neurons", *self.base, "--class", "kenyon_cell")
        close = cli_error("neurons", *self.base, "--super-class", "decending")
        olf = cli_error("paths", *self.base, "--source-class", "PN", "--target-type", "DNa02")

        self.assertEqual(folded, "--class 'kenyon_cell' is not a hemibrain cell_class; close matches: Kenyon_Cell")
        self.assertIn("close matches: descending", close)
        self.assertIn("The whole-brain equivalent is --source-class ALPN", olf)
        self.assertIn("--source-type '*_*PN*' also selects the hemibrain PN types without a FlyWire match", olf)

    def test_paths_and_reach_accept_class_selectors(self) -> None:
        paths, _, _ = run_cli_rows(
            "paths", *self.base, "--source-class", "ALPN", "--target-super-class", "descending",
            "--target-type", "DN*", "--top", "1", "--csv",
        )
        reach, _, _ = run_cli_rows(
            "reach", *self.base, "--source-class", "ALPN", "--target-super-class", "descending",
            "--hops", "2", "--csv",
        )

        self.assertEqual([row["path_ids"] for row in paths], ["101 > 102 > 112", "101 > 102 > 111"])
        self.assertAlmostEqual(float(paths[1]["strength"]), 1.0, places=6)
        self.assertEqual([(row["target_type"], row["reach"]) for row in reach], [("DNa02", "1"), ("COMPA", "0"), ("COMPB", "0")])

    def test_missing_class_source_stops_with_the_setup_command(self) -> None:
        import duckdb

        with duckdb.connect(str(self.store)) as connection:
            connection.execute("DROP TABLE flywire_neuron_annotations")
        errors = [
            cli_error("neurons", *self.base, "--class", "ALPN"),
            cli_error("neurons", *self.base, "--type", "DNa02"),
            cli_error("paths", *self.base, "--source-class", "ALPN", "--target-type", "DNa02"),
            cli_error("reach", *self.base, "--source-type", "DA1_lPN", "--target-super-class", "descending"),
        ]

        for error in errors:
            self.assertIn("missing flywire_neuron_annotations", error)
            self.assertIn("run `fruitloops setup --hemibrain`", error)


if __name__ == "__main__":
    unittest.main()
