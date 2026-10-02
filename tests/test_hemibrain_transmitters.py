from __future__ import annotations

import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fruitloops.bulk import list_sources, setup_hemibrain_bulk
from fruitloops.curated import transmitter_overrides
from fruitloops.neuron_labels import HEMIBRAIN_TRANSMITTER_CLASSES, load_neuron_labels
from fruitloops.table_import import import_to_duckdb
from test_wholebrain import HAS_DUCKDB, cli_error, run_cli_rows

# Hemibrain fixture. DA1_lPN 101 (right) and 102 (left) are the sources; DNa02 301 (right) and
# 302 (left) are the targets. SMPX has an ACh neuron (202) and a glutamate neuron (205), so it is a
# sign-conflict type. KCg-m 203 is predicted dopamine and set to acetylcholine by the override.
NEURONS = [
    (101, "DA1_lPN", "DA1_lPN_R"),
    (102, "DA1_lPN", "DA1_lPN_L"),
    (201, "LHX", "LHX_R"),
    (202, "SMPX", "SMPX_R"),
    (203, "KCg-m", "KCg-m_R"),
    (204, "MBON01", "MBON01_R"),
    (205, "SMPX", "SMPX_L"),
    (206, "NEI", "NEI_R"),
    (207, "NOPRED", "NOPRED_R"),
    (208, "TIE", "TIE_R"),
    (301, "DNa02", "DNa02_R"),
    (302, "DNa02", "DNa02_L"),
]
EDGES = [
    (101, 201, "LH(R)", 8),
    (101, 201, "SLP(R)", 2),
    (102, 201, "LH(R)", 5),
    (101, 202, "SMP(R)", 9),
    (101, 203, "CA(R)", 10),
    (203, 204, "gL(R)", 20),
    (102, 205, "SMP(L)", 10),
    (101, 206, "SMP(R)", 7),
    (201, 301, "LAL(R)", 12),
    (202, 301, "LAL(R)", 7),
    (204, 301, "LAL(R)", 6),
    (205, 302, "LAL(L)", 10),
    (206, 302, "LAL(L)", 5),
]
# Mean class probabilities per body, in HEMIBRAIN_TRANSMITTER_CLASSES order:
# gaba, acetylcholine, glutamate, serotonin, octopamine, dopamine, neither. Body 207 has no row.
PREDICTIONS = {
    101: (0.05, 0.80, 0.05, 0.02, 0.02, 0.03, 0.03),
    102: (0.10, 0.60, 0.10, 0.05, 0.05, 0.05, 0.05),
    201: (0.40, 0.39, 0.10, 0.03, 0.03, 0.03, 0.02),
    202: (0.10, 0.70, 0.10, 0.02, 0.02, 0.03, 0.03),
    203: (0.05, 0.20, 0.05, 0.05, 0.05, 0.55, 0.05),
    204: (0.10, 0.10, 0.70, 0.02, 0.02, 0.03, 0.03),
    205: (0.10, 0.20, 0.60, 0.02, 0.02, 0.03, 0.03),
    206: (0.10, 0.20, 0.10, 0.05, 0.05, 0.05, 0.45),
    208: (0.30, 0.30, 0.10, 0.10, 0.10, 0.05, 0.05),
    301: (0.05, 0.80, 0.05, 0.02, 0.02, 0.03, 0.03),
    302: (0.10, 0.70, 0.10, 0.02, 0.02, 0.03, 0.03),
    # Untraced body: imported, but not a label row.
    999: (0.90, 0.02, 0.02, 0.02, 0.02, 0.01, 0.01),
}

# Input fractions over all traced partners.
W_101_201, W_102_201, W_101_201_LH = 10 / 15, 5 / 15, 8 / 15
W_201_301, W_202_301, W_204_301 = 12 / 25, 7 / 25, 6 / 25
W_205_302, W_206_302 = 10 / 15, 5 / 15


def write_predictions(path: Path, predictions: dict[int, tuple[float, ...]]) -> None:
    """Feather file with the columns of the pinned hemibrain body-mean transmitter file."""
    import pyarrow as pa
    import pyarrow.feather as feather

    bodies = sorted(predictions)
    columns = {"type": pa.array([None] * len(bodies), pa.string())}
    for index, name in enumerate(HEMIBRAIN_TRANSMITTER_CLASSES):
        columns[name] = pa.array([predictions[body][index] for body in bodies], pa.float64())
    columns["predicted_nt"] = pa.array(
        [HEMIBRAIN_TRANSMITTER_CLASSES[max(range(7), key=predictions[body].__getitem__)] for body in bodies]
    )
    columns["body"] = pa.array(bodies, pa.uint64())
    feather.write_feather(pa.table(columns), str(path))


def write_compact_tables(store: Path) -> None:
    import duckdb

    with duckdb.connect(str(store)) as connection:
        connection.execute(
            "CREATE TABLE hemibrain_traced_roi_connections(bodyId_pre BIGINT, bodyId_post BIGINT, roi VARCHAR, weight BIGINT)"
        )
        connection.executemany("INSERT INTO hemibrain_traced_roi_connections VALUES (?, ?, ?, ?)", EDGES)
        connection.execute("CREATE TABLE hemibrain_traced_neurons(bodyId BIGINT, type VARCHAR, instance VARCHAR)")
        connection.executemany("INSERT INTO hemibrain_traced_neurons VALUES (?, ?, ?)", NEURONS)


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
class HemibrainTransmitterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Path(self.tmp.name) / "hemibrain.duckdb"
        write_compact_tables(self.store)
        feather_path = Path(self.tmp.name) / "predictions.feather"
        write_predictions(feather_path, PREDICTIONS)
        import_to_duckdb(feather_path, "hemibrain_body_neurotransmitters", self.store, replace=True)
        self.base = ["--hemibrain", "--store", str(self.store)]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def assertValue(self, text: str, expected: float) -> None:
        self.assertAlmostEqual(float(text), expected, delta=abs(expected) * 1e-5 + 1e-12)

    def test_top_transmitter_is_the_largest_mean_probability(self) -> None:
        labels = load_neuron_labels(self.store, "hemibrain")
        top = dict(zip(labels.ids.tolist(), labels.field("top_nt").tolist()))
        signs = dict(zip(labels.ids.tolist(), labels.sign.tolist()))

        self.assertEqual(labels.ids.tolist(), [body for body, _, _ in NEURONS])
        self.assertEqual(top[201], "gaba")
        self.assertEqual(top[205], "glutamate")
        self.assertEqual(top[206], "neither")
        self.assertEqual(top[208], "gaba")
        self.assertEqual(top[207], "")
        self.assertEqual((signs[101], signs[201], signs[205], signs[206], signs[207]), (1, -1, -1, 0, 0))

    def test_neurons_report_kenyon_cell_override_and_sign_conflicts(self) -> None:
        rows, _, _ = run_cli_rows("neurons", *self.base, "--type", "KCg-m,SMPX,NEI,NOPRED", "--csv")
        conflicts, _, _ = run_cli_rows("neurons", *self.base, "--sign-conflicts", "--csv")
        by_id = {row["id"]: row for row in rows}

        self.assertEqual(
            (by_id["203"]["top_nt"], by_id["203"]["transmitter"], by_id["203"]["sign"]),
            ("dopamine", "acetylcholine", "1"),
        )
        self.assertEqual((by_id["202"]["transmitter"], by_id["202"]["sign"]), ("acetylcholine", "1"))
        self.assertEqual((by_id["205"]["transmitter"], by_id["205"]["sign"]), ("glutamate", "-1"))
        self.assertEqual({by_id[key]["type_sign_conflict"] for key in ("202", "205")}, {"true"})
        self.assertEqual(by_id["203"]["type_sign_conflict"], "false")
        self.assertEqual((by_id["206"]["transmitter"], by_id["206"]["sign"]), ("neither", "0"))
        self.assertEqual((by_id["207"]["top_nt"], by_id["207"]["transmitter"], by_id["207"]["sign"]), ("", "", "0"))
        self.assertEqual([row["id"] for row in conflicts], ["202", "205"])

    def test_paths_report_signs_and_signed_search_drops_unsigned_relays(self) -> None:
        paths, _, _ = run_cli_rows("paths", *self.base, "--source-type", "DA1_lPN", "--target-id", "301", "--csv")
        unsigned, _, _ = run_cli_rows(
            "paths", *self.base, "--source-type", "DA1_lPN", "--target-id", "302", "--csv"
        )
        signed, _, _ = run_cli_rows(
            "paths", *self.base, "--source-type", "DA1_lPN", "--target-id", "302", "--signed", "--csv"
        )
        kc, _, _ = run_cli_rows(
            "paths", *self.base, "--source-type", "DA1_lPN", "--target-id", "301", "--via", "kc", "--csv"
        )

        self.assertEqual([row["path_ids"] for row in paths], ["101 > 201 > 301", "101 > 202 > 301", "101 > 203 > 204 > 301"])
        self.assertEqual([row["sign"] for row in paths], ["-1", "1", "-1"])
        self.assertValue(paths[0]["signed_strength"], -W_101_201 * W_201_301)
        self.assertEqual(paths[0]["transmitters"], "acetylcholine > gaba > acetylcholine")
        self.assertEqual(paths[1]["sign_conflict_types"], "SMPX")
        self.assertEqual(kc[0]["transmitters"], "acetylcholine > acetylcholine > glutamate > acetylcholine")
        self.assertValue(kc[0]["signed_strength"], -W_204_301)
        self.assertEqual([row["path_ids"] for row in unsigned], ["102 > 205 > 302", "101 > 206 > 302"])
        self.assertEqual((unsigned[1]["sign"], unsigned[1]["signed_strength"]), ("0", "0"))
        self.assertEqual(unsigned[1]["transmitters"], "acetylcholine > neither > acetylcholine")
        self.assertEqual([row["path_ids"] for row in signed], ["102 > 205 > 302"])
        self.assertValue(signed[0]["signed_strength"], -W_205_302)

    def test_type_routes_sum_signed_paths(self) -> None:
        rows, _, _ = run_cli_rows(
            "paths", *self.base, "--source-type", "DA1_lPN", "--target-type", "DNa02", "--by-type", "--top", "5", "--csv"
        )
        by_route = {row["path_types"]: row for row in rows}
        smpx = by_route["DA1_lPN > SMPX > DNa02"]
        lhx = by_route["DA1_lPN > LHX > DNa02"]

        self.assertEqual(rows[0]["path_types"], "DA1_lPN > SMPX > DNa02")
        self.assertValue(smpx["strength"], W_202_301 + W_205_302)
        self.assertValue(smpx["signed_strength"], W_202_301 - W_205_302)
        self.assertValue(lhx["signed_strength"], -(W_101_201 + W_102_201) * W_201_301)
        self.assertEqual(by_route["DA1_lPN > NEI > DNa02"]["signed_strength"], "0")
        self.assertValue(by_route["DA1_lPN > KCg-m > MBON01 > DNa02"]["signed_strength"], -W_204_301)

    def test_reach_by_side_uses_soma_sides_and_leaves_one_sided_types_without_ai(self) -> None:
        rows, _, errors = run_cli_rows(
            "reach", *self.base, "--source-type", "DA1_lPN", "--target-type", "DNa02,MBON01",
            "--hops", "2", "--by-side", "--csv",
        )
        by_type = {row["target_type"]: row for row in rows}
        dna02, mbon = by_type["DNa02"], by_type["MBON01"]
        ipsi = W_205_302 + (W_101_201 * W_201_301 + W_202_301)
        contra = W_206_302 + W_102_201 * W_201_301
        signed_ipsi = -W_205_302 + (-W_101_201 * W_201_301 + W_202_301)
        signed_contra = 0.0 - W_102_201 * W_201_301

        self.assertEqual((dna02["left_neurons"], dna02["right_neurons"]), ("1", "1"))
        self.assertValue(dna02["ipsi"], ipsi)
        self.assertValue(dna02["contra"], contra)
        self.assertValue(dna02["ai"], (ipsi - contra) / (ipsi + contra))
        self.assertValue(dna02["signed_net"], signed_ipsi - signed_contra)
        self.assertEqual((mbon["left_neurons"], mbon["right_neurons"]), ("0", "1"))
        self.assertValue(mbon["reach"], 1.0)
        self.assertEqual((mbon["ipsi"], mbon["ai"]), ("", ""))
        self.assertIn("1 hemibrain types have neurons with conflicting signs", errors)

    def test_commands_without_transmitter_table_name_setup(self) -> None:
        import duckdb

        with duckdb.connect(str(self.store)) as connection:
            connection.execute("DROP TABLE hemibrain_body_neurotransmitters")
        errors = [
            cli_error("neurons", *self.base, "--type", "DNa02"),
            cli_error("neurons", *self.base, "--sign-conflicts"),
            cli_error("paths", *self.base, "--source-type", "DA1_lPN", "--target-type", "DNa02"),
            cli_error("paths", *self.base, "--source-type", "DA1_lPN", "--target-type", "DNa02", "--signed"),
            cli_error("reach", *self.base, "--source-type", "DA1_lPN", "--target-type", "DNa02", "--by-side"),
        ]

        for error in errors:
            self.assertIn("missing hemibrain_body_neurotransmitters", error)
            self.assertIn("fruitloops setup --hemibrain", error)

    def test_orn_weighting_stays_flywire_only(self) -> None:
        error = cli_error(
            "paths", *self.base, "--source-type", "DA1_lPN", "--target-type", "DNa02", "--orn-family", "orco"
        )

        self.assertIn("ORN weighting is not supported for hemibrain", error)


class HemibrainTransmitterSourceTest(unittest.TestCase):
    def test_source_is_pinned_by_sha256(self) -> None:
        source = next(row for row in list_sources() if row["kind"] == "body-neurotransmitters")

        self.assertEqual(source["dataset"], "hemibrain")
        self.assertEqual(
            source["url"],
            "https://storage.googleapis.com/hemibrain/v1.2/hemibrain-v1.2-body-mean-neurotransmitters.feather",
        )
        self.assertEqual(source["sha256"], "aab49d858415f559f469a9293adfb4d58e423db83a5debb24272ee4d66e059ad")
        self.assertEqual(source["table_name"], "hemibrain_body_neurotransmitters")

    def test_kenyon_cell_override_covers_every_hemibrain_kc_type(self) -> None:
        hemibrain = {item.value: item for item in transmitter_overrides() if item.dataset == "hemibrain"}

        self.assertEqual(
            set(hemibrain),
            {"KCa'b'-ap1", "KCa'b'-ap2", "KCa'b'-m", "KCab-c", "KCab-m", "KCab-p", "KCab-s",
             "KCg-d", "KCg-m", "KCg-s1", "KCg-s2", "KCg-s3", "KCg-s4", "KCg-t"},
        )
        self.assertEqual({(item.field, item.transmitter, item.doi) for item in hemibrain.values()},
                         {("type", "acetylcholine", "10.1016/j.neuron.2016.02.015")})

    @unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
    def test_setup_imports_transmitters_as_own_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / "exported-traced-adjacencies-v1.2.tar.gz"
            csvs = {
                "traced-roi-connections.csv": "bodyId_pre,bodyId_post,roi,weight\n101,201,LH(R),8\n",
                "traced-total-connections.csv": "bodyId_pre,bodyId_post,weight\n101,201,8\n",
                "traced-neurons.csv": "bodyId,type,instance\n101,DA1_lPN,DA1_lPN_R\n201,LHX,LHX_R\n",
            }
            with tarfile.open(archive, "w:gz") as handle:
                for name, text in csvs.items():
                    member = root / name
                    member.write_text(text)
                    handle.add(member, arcname=f"exported-traced-adjacencies-v1.2/{name}")
            predictions = root / "hemibrain-v1.2-body-mean-neurotransmitters.feather"
            write_predictions(predictions, {101: PREDICTIONS[101], 201: PREDICTIONS[201]})
            store = root / "store.duckdb"

            def fake_download(dataset, kind, output_dir):
                return predictions if kind == "body-neurotransmitters" else archive

            def failing_download(dataset, kind, output_dir):
                if kind == "body-neurotransmitters":
                    raise ValueError("sha256 mismatch for hemibrain:body-neurotransmitters")
                return archive

            with patch("fruitloops.bulk.download_source", side_effect=fake_download):
                first = setup_hemibrain_bulk(root / "bulk", store, replace=True, skip_current=True)
                second = setup_hemibrain_bulk(root / "bulk", store, replace=True, skip_current=True)
            with patch("fruitloops.bulk.download_source", side_effect=failing_download):
                failed = setup_hemibrain_bulk(root / "bulk", root / "other.duckdb", replace=True, skip_current=True)

        def statuses(rows):
            return {(row["action"], row["target"]): row["status"] for row in rows}

        self.assertEqual(statuses(first)[("import", "hemibrain_body_neurotransmitters")], "2")
        self.assertEqual(statuses(second)[("import", "hemibrain_body_neurotransmitters")], "current:2")
        self.assertEqual(statuses(second)[("import", "hemibrain_traced_neurons")], "current:2")
        self.assertTrue(statuses(failed)[("download", "body-neurotransmitters")].startswith("error: sha256 mismatch"))
        self.assertEqual(statuses(failed)[("import", "hemibrain_traced_neurons")], "2")


if __name__ == "__main__":
    unittest.main()
