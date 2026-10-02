from __future__ import annotations

import csv
import dataclasses
import hashlib
import importlib.util
import tempfile
import threading
import unittest
import zipfile
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO, StringIO
from pathlib import Path
from unittest.mock import patch

from fruitloops import hemibrain_neo4j
from fruitloops.bulk import BULK_SOURCES, resolve_source, source_path
from fruitloops.cli import main
from fruitloops.hemibrain_neo4j import CONNECTIONS_MEMBER, NEURONS_MEMBER, BundleError, setup_hemibrain_bundle
from fruitloops.olfaction import (
    HEMIBRAIN_OLFACTION_ANNOTATION_TABLE,
    HEMIBRAIN_OLFACTION_ORN_PN_TABLE,
    build_olfaction_cache,
)
from fruitloops.olfaction_live import replace_table_from_frames

HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None
BUNDLE_KEY = ("hemibrain", "neo4j-inputs")

# Neurons member with the real neo4j header syntax, including columns the reader skips.
NEURON_HEADER = (
    '":ID(Body-ID)","bodyId:long","pre:int","post:int","status:string","cropped:boolean",'
    '"instance:string","type:string","somaLocation:point{srid:9157}","size:long","roiInfo:string",'
    '":LABEL","AL(R):boolean","a\'L(R):boolean"'
)
NEURON_LABELS = "Segment;hemibrain_Segment;Neuron;hemibrain_Neuron;Cell;hemibrain_Cell"
SEGMENT_LABELS = "Segment;hemibrain_Segment"
# id, status, cropped, instance, type, size, labels. Empty fields set no property.
NEURONS = [
    (11, "Assign", "true", "ORN_DM1_R", "ORN_DM1", "110", NEURON_LABELS),
    (12, "Assign", "true", "ORN_DM1_L", "ORN_DM1", "120", NEURON_LABELS),
    (13, "Orphan", "", "ORN_DA1_R", "ORN_DA1", "130", NEURON_LABELS),
    (14, "", "", "ORN_DM1_R", "ORN_DM1", "140", SEGMENT_LABELS),
    (15, "Assign", "true", "xORN_DM1_R", "xORN_DM1", "150", NEURON_LABELS),
    (16, "Assign", "true", "ORN_DM1_R", "ORN_DM1", "160", "Segment;hemibrain_Neuron"),
    (21, "Traced", "false", "DM1_lPN_R", "DM1_lPN", "2100", NEURON_LABELS),
    (22, "Traced", "false", "DA1_lPN_L", "DA1_lPN", "2200", NEURON_LABELS),
    (23, "Traced", "false", "lLN1_R", "lLN1", "2300", NEURON_LABELS),
    (24, "", "", "DM1_lPN_R", "DM1_lPN", "2400", SEGMENT_LABELS),
    (25, "Traced", "true", "", "M_vPNml50", "", NEURON_LABELS),
    (31, "Traced", "false", "lLN2F_R", "lLN2F", "3100", NEURON_LABELS),
    (41, "Traced", "false", "FB1_R", "FB1", "4100", NEURON_LABELS),
    (51, "", "", "", "", "5100", SEGMENT_LABELS),
]
# start, weight, weightHP, end
CONNECTIONS = [
    (11, 7, 6, 21),
    (12, 3, 3, 21),
    (13, 5, 5, 22),
    (13, 1, 1, 25),
    (11, 0, 0, 22),  # weight > 0
    (14, 9, 9, 21),  # source is not a Neuron
    (15, 4, 4, 21),  # source type does not start with ORN_
    (16, 4, 4, 21),  # hemibrain_Neuron is not the Neuron label
    (11, 6, 6, 23),  # target type has no PN
    (11, 2, 2, 24),  # target is not a Neuron
    (21, 2, 2, 11),  # PN to ORN
]
EXPECTED_ORN_PN = {
    (11, 21, "AL(R)", 7, "ORN_DM1", "ORN_DM1_R", "DM1_lPN", "DM1_lPN_R"),
    (12, 21, "AL(R)", 3, "ORN_DM1", "ORN_DM1_L", "DM1_lPN", "DM1_lPN_R"),
    (13, 22, "AL(L)", 5, "ORN_DA1", "ORN_DA1_R", "DA1_lPN", "DA1_lPN_L"),
    (13, 25, "AL", 1, "ORN_DA1", "ORN_DA1_R", "M_vPNml50", None),
}
# Compact traced connections that define the hemibrain olf bodies: 21, 31, 51.
COMPACT_CONNECTIONS = [
    (21, 31, "AL(R)", 4),
    (31, 21, "LH(R)", 2),
    (21, 51, "AL(R)", 3),
    (41, 31, "FB", 9),
]


def write_bundle(path: Path, neuron_header: str = NEURON_HEADER, connections=CONNECTIONS) -> Path:
    neuron_lines = [neuron_header]
    for body, status, cropped, instance, cell_type, size, labels in NEURONS:
        fields = [body, body, 3, 10, status, cropped, instance, cell_type, "", size, '"{}"', labels, "", ""]
        neuron_lines.append(",".join(str(field) for field in fields))
    connection_lines = [":START_ID(Body-ID),weight:int,weightHP:int,:END_ID(Body-ID),roiInfo:string"]
    for start, weight, weight_hp, end in connections:
        connection_lines.append(f'{start},{weight},{weight_hp},{end},"{{""AL(R)"": {{""pre"": 1}}}}"')
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("hemibrain_v1.2_neo4j_inputs/", "")
        archive.writestr(NEURONS_MEMBER, "\n".join(neuron_lines) + "\n")
        archive.writestr(CONNECTIONS_MEMBER, "\n".join(connection_lines) + "\n")
        archive.writestr("hemibrain_v1.2_neo4j_inputs/Neuprint_Meta_31597.csv", "tag:string,:LABEL\nv1.2,Meta\n")
    return path


@contextmanager
def pinned_to(path: Path, *, member_pins: bool = True, sha256: bool = True, url: str | None = None):
    """Pin the bundle source to a synthetic zip, as the real pins describe the released bundle."""
    with zipfile.ZipFile(path) as archive:
        infos = [archive.getinfo(member) for member in hemibrain_neo4j.MEMBER_PINS]
    pins = {info.filename: (info.file_size, info.CRC) for info in infos}
    source = resolve_source(*BUNDLE_KEY)
    changes = {}
    if sha256:
        changes["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    if url:
        changes["url"] = url
    with patch.dict(BULK_SOURCES, {BUNDLE_KEY: dataclasses.replace(source, **changes)}):
        with patch.dict(hemibrain_neo4j.MEMBER_PINS, pins if member_pins else {}):
            yield


def write_compact_store(store: Path) -> None:
    """Write the compact hemibrain tables and build the olf tables, as setup does first."""
    import duckdb

    with duckdb.connect(str(store)) as connection:
        connection.execute(
            "CREATE TABLE hemibrain_traced_roi_connections(bodyId_pre BIGINT, bodyId_post BIGINT, roi VARCHAR, weight BIGINT)"
        )
        connection.executemany("INSERT INTO hemibrain_traced_roi_connections VALUES (?, ?, ?, ?)", COMPACT_CONNECTIONS)
        connection.execute("CREATE TABLE hemibrain_traced_neurons(bodyId BIGINT, type VARCHAR, instance VARCHAR)")
        connection.executemany(
            "INSERT INTO hemibrain_traced_neurons VALUES (?, ?, ?)",
            [(21, "DM1_lPN", "DM1_lPN_R"), (31, "lLN2F", "lLN2F_R"), (41, "FB1", "FB1_R")],
        )
    build_olfaction_cache(store=store, datasets=["hemibrain"], replace=True, skip_current=True)


def run_cli(*args: str) -> tuple[list[dict[str, str]], str]:
    output, errors = StringIO(), StringIO()
    with redirect_stdout(output), redirect_stderr(errors):
        result = main(list(args))
    if result != 0:
        raise AssertionError(f"CLI exited with {result}")
    return list(csv.DictReader(StringIO(output.getvalue()))), errors.getvalue()


def query(store: Path, sql: str) -> list[tuple]:
    import duckdb

    with duckdb.connect(str(store), read_only=True) as connection:
        return connection.execute(sql).fetchall()


def column_types(store: Path, table: str) -> list[tuple]:
    return query(
        store,
        "SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_name = '{table}' ORDER BY ordinal_position",
    )


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
class BundleCacheTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.store = root / "store.duckdb"
        self.bulk = root / "bulk"
        self.bundle = write_bundle(source_path(resolve_source(*BUNDLE_KEY), self.bulk / "raw"))
        write_compact_store(self.store)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def cache(self, *flags: str) -> tuple[list[dict[str, str]], str]:
        return run_cli(
            "olf", "--store", str(self.store), "cache-annotations", "--hemibrain",
            "--source", "neo4j-inputs", "--bulk-dir", str(self.bulk), *flags, "--format", "csv",
        )

    def test_reader_keeps_neuron_label_nodes_with_neo4j_empty_field_rules(self) -> None:
        with pinned_to(self.bundle), hemibrain_neo4j.open_bundle(self.bulk) as (handle, label):
            neurons = hemibrain_neo4j.read_olfaction_bundle(handle, label).neurons.to_pylist()
        by_id = {row["bodyId"]: row for row in neurons}

        # 14, 24 and 51 carry only Segment labels; 16 has hemibrain_Neuron without Neuron.
        self.assertEqual(sorted(by_id), [11, 12, 13, 15, 21, 22, 23, 25, 31, 41])
        self.assertEqual([by_id[body]["cropped"] for body in (11, 21, 13)], [True, False, None])
        self.assertEqual((by_id[25]["instance"], by_id[25]["size"]), (None, None))

    def test_orn_pn_table_applies_label_type_and_weight_rules(self) -> None:
        with pinned_to(self.bundle):
            rows, _ = self.cache("--no-rebuild")

        orn_pn = set(query(self.store, f"SELECT * FROM {HEMIBRAIN_OLFACTION_ORN_PN_TABLE}"))
        self.assertEqual(orn_pn, EXPECTED_ORN_PN)
        self.assertEqual(
            column_types(self.store, HEMIBRAIN_OLFACTION_ORN_PN_TABLE),
            [("bodyId_pre", "BIGINT"), ("bodyId_post", "BIGINT"), ("roi", "VARCHAR"), ("weight", "BIGINT"),
             ("pre_type", "VARCHAR"), ("pre_instance", "VARCHAR"), ("post_type", "VARCHAR"), ("post_instance", "VARCHAR")],
        )
        self.assertEqual(
            [(row["table"], row["rows"], row["status"]) for row in rows],
            [(HEMIBRAIN_OLFACTION_ORN_PN_TABLE, "4", "cached"), (HEMIBRAIN_OLFACTION_ANNOTATION_TABLE, "2", "cached")],
        )

    def test_annotation_table_covers_olf_bodies_that_are_neurons(self) -> None:
        with pinned_to(self.bundle):
            self.cache("--no-rebuild")

        annotations = query(self.store, f"SELECT * FROM {HEMIBRAIN_OLFACTION_ANNOTATION_TABLE} ORDER BY bodyId")
        schema = column_types(self.store, HEMIBRAIN_OLFACTION_ANNOTATION_TABLE)
        # Body 51 is an olf body without the Neuron label; body 41 is a Neuron outside AL/LH/MB.
        self.assertEqual(
            annotations,
            [(21, "DM1_lPN", "DM1_lPN_R", "Traced", False, 2100), (31, "lLN2F", "lLN2F_R", "Traced", False, 3100)],
        )
        self.assertEqual([row[1] for row in schema], ["BIGINT", "VARCHAR", "VARCHAR", "VARCHAR", "BOOLEAN", "BIGINT"])

    def test_cache_records_freshness_and_no_rebuild_defers_the_olf_rebuild(self) -> None:
        with pinned_to(self.bundle):
            self.cache("--no-rebuild")
            state = dict(query(self.store, "SELECT stage_key, rows FROM _fruitloops_setup_state"))
            queries = [self.glomerulus("DM1")[1], self.glomerulus("DM1")[1]]

        self.assertEqual(state[f"import:{HEMIBRAIN_OLFACTION_ORN_PN_TABLE}"], "4")
        self.assertEqual(state[f"import:{HEMIBRAIN_OLFACTION_ANNOTATION_TABLE}"], "2")
        self.assertEqual(state["neo4j-inputs:olf"], "4")
        self.assertEqual(["rebuilt stale" in progress for progress in queries], [True, False])

    def test_glomerulus_and_inputs_count_hemibrain_orns_end_to_end(self) -> None:
        with pinned_to(self.bundle):
            self.cache()
            glomerulus, progress = self.glomerulus("DM1")
            inputs, _ = run_cli(
                "olf", "--store", str(self.store), "inputs", "--hemibrain", "--target-class", "PN",
                "--source-class", "ORN", "--glomerulus", "DM1", "--format", "csv",
            )

        self.assertEqual(len(glomerulus), 1)
        self.assertEqual(glomerulus[0]["orn_count"], "2")
        self.assertEqual(glomerulus[0]["orn_input_pre_neurons"], "2")
        self.assertEqual(glomerulus[0]["orn_to_pn_synapses"], "10")
        self.assertNotIn("rebuilt stale", progress)
        self.assertEqual([(row["target_id"], row["synapses"]) for row in inputs], [("21", "10")])

    def glomerulus(self, name: str) -> tuple[list[dict[str, str]], str]:
        return run_cli("olf", "--store", str(self.store), "glomerulus", name, "--hemibrain", "--format", "csv")

    def test_source_requires_hemibrain_only(self) -> None:
        for flags in ((), ("--flywire",), ("--hemibrain", "--flywire")):
            with self.subTest(flags=flags), redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    main(["olf", "--store", str(self.store), "cache-annotations", *flags, "--source", "neo4j-inputs"])
                self.assertIn("requires --hemibrain", str(raised.exception.code))

    def test_mismatched_or_corrupt_bundle_is_a_clear_error_and_keeps_tables(self) -> None:
        with pinned_to(self.bundle):
            self.cache("--no-rebuild")
        good = self.bundle.read_bytes()
        corrupt = bytearray(good)
        with zipfile.ZipFile(self.bundle) as archive:
            info = archive.getinfo(NEURONS_MEMBER)
        corrupt[info.header_offset + 30 + len(info.filename) + info.compress_size // 2] ^= 0xFF
        bad_header = write_bundle(Path(self.tmp.name) / "header.zip", NEURON_HEADER.replace('"bodyId:long"', '"bodyId:string"'))
        cases = {
            "sha256": (good, dict(sha256=False), "sha256 mismatch"),
            "member pin": (good, dict(member_pins=False), r"CRC-32 [0-9a-f]{8}; the pinned bundle has"),
            "corrupt member": (bytes(corrupt), {}, "corrupt hemibrain neo4j bundle"),
            "header": (bad_header.read_bytes(), {}, r"lacks neo4j header field\(s\) bodyId:long"),
        }
        for name, (content, pins, message) in cases.items():
            with self.subTest(name):
                self.bundle.write_bytes(content)
                with pinned_to(self.bundle, **pins):
                    rows, _ = self.cache("--no-rebuild")
                self.assertEqual([row["status"] for row in rows], ["error"])
                self.assertRegex(rows[0]["rows"], message)
                self.assertEqual(len(query(self.store, f"SELECT * FROM {HEMIBRAIN_OLFACTION_ORN_PN_TABLE}")), 4)

        with self.assertRaisesRegex(BundleError, "cannot open hemibrain neo4j bundle"):
            hemibrain_neo4j.read_olfaction_bundle(BytesIO(b"not a zip"), "bytes")


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
class BundleSetupTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.store = root / "store.duckdb"
        self.bulk = root / "bulk"
        self.bundle = write_bundle(source_path(resolve_source(*BUNDLE_KEY), self.bulk / "raw"))
        write_compact_store(self.store)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def setup(self) -> list[tuple[str, str, str]]:
        with (
            patch("fruitloops.cli_setup.setup_practical_bulk", return_value=[]),
            patch("fruitloops.cli_setup.build_graph_cache", return_value=graph_row(self.store)),
            pinned_to(self.bundle),
        ):
            rows, _ = run_cli(
                "setup", "--hemibrain", "--no-progress", "--csv", "--store", str(self.store),
                "--bulk-dir", str(self.bulk), "--cache-dir", str(Path(self.tmp.name) / "cache"),
            )
        return [(row["action"], row["target"], row["status"]) for row in rows]

    def test_setup_builds_bundle_tables_as_a_fingerprinted_stage(self) -> None:
        first = self.setup()
        second = self.setup()
        glomerulus, _ = run_cli("olf", "--store", str(self.store), "glomerulus", "DM1", "--hemibrain", "--format", "csv")

        self.assertIn(("import", HEMIBRAIN_OLFACTION_ORN_PN_TABLE, "4"), first)
        self.assertIn(("import", HEMIBRAIN_OLFACTION_ANNOTATION_TABLE, "2"), first)
        self.assertIn(("olfaction-rebuild", "olf_neurons", "built:8"), first)
        self.assertIn(("import", HEMIBRAIN_OLFACTION_ORN_PN_TABLE, "current:4"), second)
        self.assertFalse(any(action == "olfaction-rebuild" for action, _, _ in second))
        self.assertEqual((glomerulus[0]["orn_count"], glomerulus[0]["orn_to_pn_synapses"]), ("2", "10"))

    def test_setup_rewrites_stale_tables_but_keeps_tables_from_another_writer(self) -> None:
        import duckdb

        self.setup()
        with duckdb.connect(str(self.store)) as connection:
            # A changed compact connection table changes the olf bodies, so the stage is stale.
            connection.execute("INSERT INTO hemibrain_traced_roi_connections VALUES (21, 41, 'LH(R)', 5)")
            connection.execute("DELETE FROM _fruitloops_setup_state WHERE stage_key = 'import:hemibrain_traced_roi_connections'")
        stale = self.setup()
        with duckdb.connect(str(self.store)) as connection:
            replace_table_from_frames(connection, HEMIBRAIN_OLFACTION_ANNOTATION_TABLE, [live_annotation_frame()])
            connection.execute("INSERT INTO hemibrain_traced_roi_connections VALUES (21, 23, 'LH(R)', 5)")
            connection.execute("DELETE FROM _fruitloops_setup_state WHERE stage_key = 'import:hemibrain_traced_roi_connections'")
        kept = self.setup()

        # The rewrite labels the current olf bodies, which now include the ORN->PN bodies.
        self.assertIn(("import", HEMIBRAIN_OLFACTION_ANNOTATION_TABLE, "8"), stale)
        self.assertIn(("import", HEMIBRAIN_OLFACTION_ANNOTATION_TABLE, "existing:1"), kept)

    def test_setup_keeps_tables_that_a_live_fetch_wrote_first(self) -> None:
        import duckdb
        import pandas as pd

        with duckdb.connect(str(self.store)) as connection:
            live_orn_pn = pd.DataFrame(
                [(11, 21, "AL(R)", 7, "ORN_DM1", "ORN_DM1_R", "DM1_lPN", "DM1_lPN_R")],
                columns=["bodyId_pre", "bodyId_post", "roi", "weight", "pre_type", "pre_instance", "post_type", "post_instance"],
            )
            replace_table_from_frames(connection, HEMIBRAIN_OLFACTION_ORN_PN_TABLE, [live_orn_pn])
            replace_table_from_frames(connection, HEMIBRAIN_OLFACTION_ANNOTATION_TABLE, [live_annotation_frame()])
        rows = self.setup()

        self.assertIn(("import", HEMIBRAIN_OLFACTION_ORN_PN_TABLE, "existing:1"), rows)
        self.assertIn(("import", HEMIBRAIN_OLFACTION_ANNOTATION_TABLE, "existing:1"), rows)

    def test_setup_reports_bundle_errors_and_continues(self) -> None:
        self.bundle.write_bytes(b"not a zip")
        with (
            patch("fruitloops.cli_setup.setup_practical_bulk", return_value=[]),
            patch("fruitloops.cli_setup.build_graph_cache", return_value=graph_row(self.store)),
        ):
            rows, _ = run_cli(
                "setup", "--hemibrain", "--no-progress", "--csv", "--store", str(self.store),
                "--bulk-dir", str(self.bulk), "--cache-dir", str(Path(self.tmp.name) / "cache"),
            )

        errors = [row for row in rows if row["target"] == "neo4j-inputs"]
        self.assertEqual(len(errors), 1)
        self.assertIn("error: sha256 mismatch", errors[0]["status"])
        self.assertIn(("olfaction-build", "olf_neurons"), {(row["action"], row["target"]) for row in rows})


def graph_row(store: Path) -> dict[str, str]:
    return {
        "dataset": "hemibrain", "action": "graph", "target": "hemibrain", "status": "current", "path": "", "store": str(store),
    }


def live_annotation_frame():
    """One annotation row as the live writer stores it."""
    import pandas as pd

    return pd.DataFrame([{"bodyId": 21, "type": "DM1_lPN", "instance": "DM1_lPN_R", "status": "Traced", "cropped": False, "size": 1}])


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
class RangeReadTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.store = root / "store.duckdb"
        self.bulk = root / "empty-bulk"
        self.bundle = write_bundle(root / "served" / "bundle.zip")
        write_compact_store(self.store)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), range_handler(self.bundle.read_bytes()))
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/bundle.zip"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def read(self, etag: str = '"fixture"') -> hemibrain_neo4j.OlfactionBundle:
        with (
            pinned_to(self.bundle, url=self.url),
            patch.object(hemibrain_neo4j, "BUNDLE_SIZE", self.bundle.stat().st_size),
            patch.object(hemibrain_neo4j, "BUNDLE_ETAG", etag),
            patch.object(hemibrain_neo4j, "RANGE_CHUNK", 1024),
            patch("fruitloops.hemibrain_neo4j.time.sleep"),
        ):
            with hemibrain_neo4j.open_bundle(self.bulk) as (handle, label):
                return hemibrain_neo4j.read_olfaction_bundle(handle, label)

    def test_bundle_without_local_file_is_read_by_http_ranges(self) -> None:
        bundle = self.read()

        self.assertEqual(set(bundle.orn_pn.itertuples(index=False, name=None)), EXPECTED_ORN_PN)
        self.assertFalse((self.bulk / "raw").exists())
        self.assertGreater(self.server.RequestHandlerClass.failures, 0)

    def test_changed_remote_object_is_rejected(self) -> None:
        with self.assertRaisesRegex(BundleError, "no longer matches the pinned bundle"):
            self.read(etag='"other"')


def range_handler(content: bytes):
    class RangeHandler(BaseHTTPRequestHandler):
        requests = 0
        failures = 0

        def do_GET(self) -> None:
            # Fail the first request and then every fifth one, so the reader must retry.
            RangeHandler.requests += 1
            if RangeHandler.failures * 5 < RangeHandler.requests:
                RangeHandler.failures += 1
                self.send_error(503)
                return
            first, last = (int(value) for value in self.headers["Range"].removeprefix("bytes=").split("-"))
            body = content[first : last + 1]
            self.send_response(206)
            self.send_header("ETag", '"fixture"')
            self.send_header("Content-Range", f"bytes {first}-{last}/{len(content)}")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args) -> None:
            pass

    return RangeHandler


if __name__ == "__main__":
    unittest.main()
