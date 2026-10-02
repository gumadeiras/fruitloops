"""Hemibrain olfaction tables from the pinned neuPrint neo4j import bundle.

The bulk source ``hemibrain:neo4j-inputs`` is the CSV bundle that neuPrint loads
with ``neo4j-admin import`` to build the hemibrain v1.2 database. This module
streams two of its members from the zip and writes the same tables, with the
same schema and freshness record, as ``olf cache-annotations --hemibrain``
writes from the live neuPrint query.

The neo4j import header defines how the CSV columns map to the graph:

- ``Neuprint_Neurons_31597.csv`` holds one node per segment. ``:ID(Body-ID)``
  is the node id, ``name:type`` columns are node properties, and ``:LABEL``
  lists the node labels, separated by ``;``. A node is ``:Neuron`` when
  ``Neuron`` is one of its labels.
- ``Neuprint_Neuron_Connections_31597.csv`` is loaded with
  ``--relationships=ConnectsTo=...``, so each row is one ``ConnectsTo``
  relationship from ``:START_ID(Body-ID)`` to ``:END_ID(Body-ID)``, and
  ``weight:int`` is ``ConnectsTo.weight``.
- An empty field sets no property, which Cypher reads as null; a quoted ``""``
  is an empty string. A boolean is true only for the text ``true``.

A downloaded bundle in the bulk raw directory is read locally after its sha256
check. Otherwise the two members are read from the pinned URL by HTTP range
requests, so the 6.2 GB zip is never stored.
"""

from __future__ import annotations

import csv
import http.client
import io
import time
import urllib.error
import urllib.request
import zipfile
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .bulk import resolve_source, source_path, verify_source_sha256
from .duckdb_store import require_duckdb, safe_identifier, table_exists, table_row_count
from .olfaction import (
    HEMIBRAIN_CONNECTION_TABLE,
    HEMIBRAIN_OLFACTION_ANNOTATION_TABLE,
    HEMIBRAIN_OLFACTION_ORN_PN_TABLE,
    OLFACTION_PREFIX,
    OLFACTION_SCHEMA_VERSION,
)
from .olfaction_live import (
    add_hemibrain_orn_pn_roi,
    annotation_row,
    create_empty_annotation_table,
    olfaction_body_ids,
    record_cached_table,
)
from .setup_state import (
    setup_row,
    setup_state_fingerprint,
    setup_state_matches,
    sha256_json,
    table_fingerprint,
    write_setup_state,
)

BUNDLE_DIR = "hemibrain_v1.2_neo4j_inputs/"
NEURONS_MEMBER = f"{BUNDLE_DIR}Neuprint_Neurons_31597.csv"
CONNECTIONS_MEMBER = f"{BUNDLE_DIR}Neuprint_Neuron_Connections_31597.csv"
# Pins checked against the server and the zip central directory: the object
# size and ETag (its MD5), and each member's uncompressed size and CRC-32.
# The local file is pinned by the sha256 of the bulk source.
BUNDLE_SIZE = 6175266195
BUNDLE_ETAG = '"e8443e69d1e238f8cb32168b3f6f6bab"'
MEMBER_PINS = {
    NEURONS_MEMBER: (10625728664, 0x3CA765B5),
    CONNECTIONS_MEMBER: (4876659508, 0x70137EE6),
}
BUNDLE_TABLES = (HEMIBRAIN_OLFACTION_ORN_PN_TABLE, HEMIBRAIN_OLFACTION_ANNOTATION_TABLE)
NEURON_LABEL = "Neuron"
# Output name -> neo4j import header field. Each property field must declare the
# type that the live query reads.
NEURON_FIELDS = {
    "id": ":ID(Body-ID)",
    "labels": ":LABEL",
    "bodyId": "bodyId:long",
    "type": "type:string",
    "instance": "instance:string",
    "status": "status:string",
    "cropped": "cropped:boolean",
    "size": "size:long",
}
CONNECTION_FIELDS = {
    "start": ":START_ID(Body-ID)",
    "end": ":END_ID(Body-ID)",
    "weight": "weight:int",
}
# Node ids stay strings, as neo4j matches them; typed properties parse as numbers.
INTEGER_FIELDS = ("bodyId", "size", "weight")
CSV_BLOCK_SIZE = 8 << 20
RANGE_CHUNK = 16 << 20
RANGE_RETRIES = 4
RANGE_TIMEOUT = 60


class BundleError(ValueError):
    """The bundle cannot be read, or it is not the pinned hemibrain neo4j bundle."""


@dataclass(frozen=True)
class OlfactionBundle:
    neurons: object
    """Arrow table of ``:Neuron`` nodes: id, bodyId, type, instance, status, cropped, size."""
    orn_pn: object
    """Pandas frame with the columns of ``hemibrain_olfaction_orn_pn_connections``."""


def setup_hemibrain_bundle(
    store: Path,
    bulk_dir: Path,
    replace: bool = True,
    prefix: str = OLFACTION_PREFIX,
) -> list[dict[str, str]]:
    """Setup stage: write both hemibrain olfaction tables from the bundle when they are missing or stale."""
    duckdb = require_duckdb("hemibrain neo4j bundle setup")
    prefix = safe_identifier(prefix)
    location = bundle_location(bulk_dir)
    with duckdb.connect(str(store)) as connection:
        if all(table_exists(connection, table) for table in BUNDLE_TABLES):
            status = kept_bundle_status(connection, prefix, replace)
            if status:
                return [
                    setup_row("hemibrain", "import", table, f"{status}:{table_row_count(connection, table)}", location, store)
                    for table in BUNDLE_TABLES
                ]
        rows = cache_hemibrain_bundle_annotations(connection, store, prefix, bulk_dir)
    return [setup_row("hemibrain", "import", row["table"], row["rows"], location, store) for row in rows]


def kept_bundle_status(connection, prefix: str, replace: bool) -> str:
    """Return ``current`` or ``existing`` when setup keeps the existing tables, or ``""`` to rewrite them."""
    if not setup_state_matches(connection, bundle_tables_key(prefix), bundle_tables_fingerprint(connection)):
        # Another writer, such as a live fetch, replaced the tables after this stage wrote them.
        return "existing"
    if setup_state_matches(connection, bundle_stage_key(prefix), bundle_stage_fingerprint(connection)):
        return "current"
    return "" if replace else "existing"


def cache_hemibrain_bundle_annotations(connection, store: Path, prefix: str, bulk_dir: Path) -> list[dict[str, str]]:
    """Write both hemibrain olfaction tables from the bundle, as the live fetch does."""
    with open_bundle(bulk_dir) as (handle, label):
        bundle = read_olfaction_bundle(handle, label)
    # The live fetch writes the ORN->PN table first and then labels the bodies of
    # the current olf tables; one transaction keeps a failed write from leaving a
    # half-replaced pair.
    connection.begin()
    try:
        orn_pn_rows = replace_cached_table(connection, HEMIBRAIN_OLFACTION_ORN_PN_TABLE, bundle.orn_pn)
        body_ids = olfaction_body_ids(connection, prefix, "hemibrain")
        annotation_rows = replace_cached_table(
            connection,
            HEMIBRAIN_OLFACTION_ANNOTATION_TABLE,
            neuron_annotations(bundle.neurons, body_ids),
        )
        write_setup_state(connection, bundle_stage_key(prefix), bundle_stage_fingerprint(connection), str(orn_pn_rows))
        write_setup_state(connection, bundle_tables_key(prefix), bundle_tables_fingerprint(connection), str(annotation_rows))
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    return [
        annotation_row("hemibrain", HEMIBRAIN_OLFACTION_ORN_PN_TABLE, orn_pn_rows, store),
        annotation_row("hemibrain", HEMIBRAIN_OLFACTION_ANNOTATION_TABLE, annotation_rows, store),
    ]


def bundle_stage_key(prefix: str) -> str:
    return f"neo4j-inputs:{prefix}"


def bundle_tables_key(prefix: str) -> str:
    return f"neo4j-inputs:{prefix}:tables"


def bundle_tables_fingerprint(connection) -> str:
    # Each cached table records its content hash on write, so this changes when
    # any writer replaces either table.
    return sha256_json({table: setup_state_fingerprint(connection, f"import:{table}") for table in BUNDLE_TABLES})


def bundle_stage_fingerprint(connection) -> str:
    # The annotation rows cover the hemibrain bodies of the olf tables, which the
    # compact connection table defines. The ORN->PN rows that this stage adds to
    # those tables are left out, so the stage does not invalidate itself.
    source = resolve_source("hemibrain", "neo4j-inputs")
    return sha256_json(
        {
            "url": source.url,
            "sha256": source.sha256,
            "size": BUNDLE_SIZE,
            "etag": BUNDLE_ETAG,
            "members": {member: list(pin) for member, pin in MEMBER_PINS.items()},
            "olfaction_schema": OLFACTION_SCHEMA_VERSION,
            "hemibrain_connections": table_fingerprint(connection, HEMIBRAIN_CONNECTION_TABLE),
        }
    )


def bundle_location(bulk_dir: Path) -> str:
    source = resolve_source("hemibrain", "neo4j-inputs")
    local = source_path(source, bulk_dir / "raw")
    return str(local) if local.exists() else source.url


@contextmanager
def open_bundle(bulk_dir: Path):
    """Yield ``(binary file, label)``: the local bundle after its sha256 check, or ranged reads of the pinned URL."""
    source = resolve_source("hemibrain", "neo4j-inputs")
    local = source_path(source, bulk_dir / "raw")
    if local.exists():
        verify_source_sha256(local, source)
        with local.open("rb") as handle:
            yield handle, str(local)
        return
    with io.BufferedReader(RangeFile(source.url, BUNDLE_SIZE, BUNDLE_ETAG), buffer_size=RANGE_CHUNK) as handle:
        yield handle, source.url


class RangeFile(io.RawIOBase):
    """Read-only, seekable view of a remote file through HTTP range requests.

    Every response must carry the pinned ETag and total size, so all bytes come
    from one version of the object. Failed requests are retried with backoff.
    """

    def __init__(self, url: str, size: int, etag: str) -> None:
        self.url = url
        self.size = size
        self.etag = etag
        self.position = 0
        self.bytes_read = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self.position, io.SEEK_END: self.size}[whence]
        if base + offset < 0:
            raise ValueError(f"negative seek position {base + offset}")
        self.position = base + offset
        return self.position

    def readinto(self, buffer) -> int:
        count = min(len(buffer), self.size - self.position, RANGE_CHUNK)
        if count <= 0:
            return 0
        data = self.fetch(self.position, self.position + count - 1)
        buffer[:count] = data
        self.position += count
        self.bytes_read += count
        return count

    def fetch(self, first: int, last: int) -> bytes:
        expected_range = f"bytes {first}-{last}/{self.size}"
        error: Exception | None = None
        for attempt in range(RANGE_RETRIES + 1):
            if attempt:
                time.sleep(2 ** (attempt - 1))
            request = urllib.request.Request(self.url, headers={"Range": f"bytes={first}-{last}"})
            try:
                with urllib.request.urlopen(request, timeout=RANGE_TIMEOUT) as response:
                    if response.headers.get("ETag") != self.etag or response.headers.get("Content-Range") != expected_range:
                        raise BundleError(
                            f"{self.url} no longer matches the pinned bundle: expected ETag {self.etag} and "
                            f"{expected_range}, got {response.headers.get('ETag')} and {response.headers.get('Content-Range')}"
                        )
                    data = response.read()
            except urllib.error.HTTPError as caught:
                caught.close()
                error = caught
                continue
            except (OSError, http.client.HTTPException) as caught:
                error = caught
                continue
            if len(data) == last - first + 1:
                return data
            error = OSError(f"short read of {len(data)} bytes")
        raise BundleError(f"cannot read {self.url} bytes {first}-{last} after {RANGE_RETRIES + 1} attempts: {error}")


def read_olfaction_bundle(handle, label: str) -> OlfactionBundle:
    """Stream the Neuron nodes and the ORN->PN ConnectsTo rows from a bundle zip."""
    try:
        archive = zipfile.ZipFile(handle)
    except (OSError, EOFError, zipfile.BadZipFile) as error:
        raise BundleError(f"cannot open hemibrain neo4j bundle {label}: {error}") from error
    with archive:
        for member in MEMBER_PINS:
            require_pinned_member(archive, label, member)
        neurons = read_neurons(archive, label)
        pairs = read_orn_pn_pairs(archive, label, neurons)
    return OlfactionBundle(neurons=neurons, orn_pn=orn_pn_frame(neurons, pairs))


def require_pinned_member(archive: zipfile.ZipFile, label: str, member: str) -> None:
    try:
        info = archive.getinfo(member)
    except KeyError as error:
        raise BundleError(f"{label} has no member {member}; expected the pinned hemibrain neo4j bundle") from error
    size, crc = MEMBER_PINS[member]
    if (info.file_size, info.CRC) != (size, crc):
        raise BundleError(
            f"{label}: {member} has {info.file_size} bytes and CRC-32 {info.CRC:08x}; "
            f"the pinned bundle has {size} bytes and CRC-32 {crc:08x}"
        )


def read_neurons(archive: zipfile.ZipFile, label: str):
    import pyarrow.compute as pc

    def neurons_only(table):
        # MATCH (n:Neuron): "Neuron" must be one of the ;-separated labels.
        table = table.filter(pc.match_substring_regex(table["labels"], f"(^|;){NEURON_LABEL}(;|$)"))
        cropped = table.schema.get_field_index("cropped")
        table = table.set_column(cropped, "cropped", pc.equal(table["cropped"], "true"))
        return table.select([name for name in NEURON_FIELDS if name != "labels"])

    return scan_member(archive, label, NEURONS_MEMBER, NEURON_FIELDS, neurons_only)


def read_orn_pn_pairs(archive: zipfile.ZipFile, label: str, neurons):
    import pyarrow.compute as pc

    # orn.type STARTS WITH 'ORN_' AND pn.type CONTAINS 'PN'; a null type matches neither.
    orn_ids = neurons["id"].filter(pc.starts_with(neurons["type"], "ORN_"))
    pn_ids = neurons["id"].filter(pc.match_substring(neurons["type"], "PN"))

    def orn_to_pn(table):
        mask = pc.and_(
            pc.and_(
                pc.is_in(table["start"], value_set=orn_ids),
                pc.is_in(table["end"], value_set=pn_ids),
            ),
            pc.greater(table["weight"], 0),
        )
        return table.filter(mask)

    return scan_member(archive, label, CONNECTIONS_MEMBER, CONNECTION_FIELDS, orn_to_pn)


def scan_member(archive: zipfile.ZipFile, label: str, member: str, fields: dict[str, str], select):
    """Stream ``fields`` of one CSV member and keep the rows that ``select`` returns.

    zipfile checks the member CRC-32 when the stream reaches its end.
    """
    import pyarrow as pa
    import pyarrow.csv as pacsv

    require_header_fields(archive, label, member, fields)
    names = list(fields)
    convert = pacsv.ConvertOptions(
        include_columns=list(fields.values()),
        column_types={field: pa.int64() if name in INTEGER_FIELDS else pa.string() for name, field in fields.items()},
        strings_can_be_null=True,
        quoted_strings_can_be_null=False,
    )
    kept = []
    try:
        with archive.open(member) as handle:
            reader = pacsv.open_csv(
                handle,
                read_options=pacsv.ReadOptions(block_size=CSV_BLOCK_SIZE),
                convert_options=convert,
            )
            schema = reader.schema
            for batch in reader:
                kept.append(select(pa.Table.from_batches([batch]).rename_columns(names)))
            if handle.read(1):
                raise BundleError(f"{label}: {member} has data after the CSV parser stopped")
    except (OSError, EOFError, zlib.error, zipfile.BadZipFile, pa.ArrowException) as error:
        raise BundleError(f"corrupt hemibrain neo4j bundle {label}: {member}: {error}") from error
    if not kept:
        return select(schema.empty_table().rename_columns(names))
    return pa.concat_tables(kept)


def require_header_fields(archive: zipfile.ZipFile, label: str, member: str, fields: dict[str, str]) -> None:
    try:
        with archive.open(member) as handle:
            header = next(csv.reader(io.TextIOWrapper(handle, encoding="utf-8", newline="")), [])
    except (OSError, EOFError, zlib.error, zipfile.BadZipFile, UnicodeDecodeError, csv.Error) as error:
        raise BundleError(f"corrupt hemibrain neo4j bundle {label}: {member}: {error}") from error
    missing = [field for field in fields.values() if field not in header]
    if missing:
        raise BundleError(
            f"{label}: {member} lacks neo4j header field(s) {', '.join(missing)}; "
            "expected the pinned hemibrain neo4j bundle"
        )


def orn_pn_frame(neurons, pairs):
    """Join ConnectsTo rows to their Neuron nodes and add ``roi`` like the live fetch."""
    nodes = neurons.select(["id", "bodyId", "type", "instance"]).to_pandas()
    pre = nodes.rename(columns={"id": "start", "bodyId": "bodyId_pre", "type": "pre_type", "instance": "pre_instance"})
    post = nodes.rename(columns={"id": "end", "bodyId": "bodyId_post", "type": "post_type", "instance": "post_instance"})
    frame = pairs.to_pandas().merge(pre, on="start").merge(post, on="end")
    labels = ["pre_type", "pre_instance", "post_type", "post_instance"]
    frame[labels] = frame[labels].astype(object).where(frame[labels].notna(), None)
    # The live query orders by pn.type, orn.type, w.weight DESC.
    frame = frame.sort_values(
        ["post_type", "pre_type", "weight", "bodyId_post", "bodyId_pre"],
        ascending=[True, True, False, True, True],
        kind="stable",
    ).reset_index(drop=True)
    return add_hemibrain_orn_pn_roi(frame)


def neuron_annotations(neurons, body_ids: list[str]):
    """``MATCH (n:Neuron) WHERE n.bodyId IN [...]`` over the olf bodies, ordered by bodyId."""
    import pyarrow as pa
    import pyarrow.compute as pc

    wanted = pa.array([int(value) for value in body_ids], type=pa.int64())
    rows = neurons.filter(pc.is_in(neurons["bodyId"], value_set=wanted))
    return rows.select(["bodyId", "type", "instance", "status", "cropped", "size"]).sort_by("bodyId")


def replace_cached_table(connection, table: str, rows) -> int:
    """Replace ``table`` with the live schema, then record it as the live fetch does."""
    table = safe_identifier(table)
    connection.execute(f"DROP TABLE IF EXISTS {table}")
    create_empty_annotation_table(connection, table)
    connection.register("_fruitloops_bundle_rows", rows)
    try:
        connection.execute(f"INSERT INTO {table} BY NAME SELECT * FROM _fruitloops_bundle_rows")
    finally:
        connection.unregister("_fruitloops_bundle_rows")
    return record_cached_table(connection, table)
