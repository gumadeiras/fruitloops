from __future__ import annotations

import hashlib
import json
import shutil
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path

from .archives import archive_stem, extract_archive_csvs
from .connection_tables import optimize_connection_table
from .duckdb_store import DEFAULT_DUCKDB_PATH, require_duckdb, safe_identifier
from .paths import default_bulk_dir
from .setup_state import (
    SETUP_STATE_TABLE,
    file_fingerprint,
    setup_row,
    setup_state_is_current,
    write_setup_state_for_store,
)
from .table_import import import_to_duckdb

DEFAULT_BULK_DIR = default_bulk_dir()
FLYWIRE_ANNOTATIONS_COMMIT = "a83b2776d60d5764cef36b927f5f9679c16c47a2"


@dataclass(frozen=True)
class BulkSource:
    dataset: str
    kind: str
    url: str
    filename: str
    format: str
    table_name: str
    description: str
    sha256: str = ""


BULK_SOURCES = {
    (source.dataset, source.kind): source
    for source in [
        BulkSource(
            dataset="flywire",
            kind="proofread-connections",
            url="https://zenodo.org/records/10676866/files/proofread_connections_783.feather?download=1",
            filename="proofread_connections_783.feather",
            format="feather",
            table_name="flywire_proofread_connections",
            description="FlyWire proofread neuron-neuron connections by neuropil.",
        ),
        BulkSource(
            dataset="flywire",
            kind="synapses",
            url="https://zenodo.org/records/10676866/files/flywire_synapses_783.feather?download=1",
            filename="flywire_synapses_783.feather",
            format="feather",
            table_name="flywire_synapses",
            description="FlyWire all released synapses with NT probabilities.",
        ),
        BulkSource(
            dataset="flywire",
            kind="pre-neuropil-counts",
            url="https://zenodo.org/records/10676866/files/per_neuron_neuropil_count_pre_783.feather?download=1",
            filename="per_neuron_neuropil_count_pre_783.feather",
            format="feather",
            table_name="flywire_pre_neuropil_counts",
            description="FlyWire presynapse counts per neuron and neuropil.",
        ),
        BulkSource(
            dataset="flywire",
            kind="post-neuropil-counts",
            url="https://zenodo.org/records/10676866/files/per_neuron_neuropil_count_post_783.feather?download=1",
            filename="per_neuron_neuropil_count_post_783.feather",
            format="feather",
            table_name="flywire_post_neuropil_counts",
            description="FlyWire postsynapse counts per neuron and neuropil.",
        ),
        BulkSource(
            dataset="flywire",
            kind="neuron-annotations",
            url=(
                "https://raw.githubusercontent.com/flyconnectome/flywire_annotations/"
                f"{FLYWIRE_ANNOTATIONS_COMMIT}/supplemental_files/Supplemental_file1_neuron_annotations.tsv"
            ),
            filename="Supplemental_file1_neuron_annotations.tsv",
            format="tsv",
            table_name="flywire_neuron_annotations",
            description="FlyWire v783 whole-brain neuron annotations (Schlegel et al. 2024), pinned commit.",
            sha256="b214970b55d2fbe0853bba536fdcb9e28730f4eb7ab06f600491df795da683cd",
        ),
        BulkSource(
            dataset="hemibrain",
            kind="compact-adjacencies",
            url="https://storage.googleapis.com/hemibrain/v1.2/exported-traced-adjacencies-v1.2.tar.gz",
            filename="exported-traced-adjacencies-v1.2.tar.gz",
            format="tar.gz",
            table_name="hemibrain_traced_roi_connections",
            description="Hemibrain v1.2 compact traced neuron adjacency CSV bundle.",
        ),
        BulkSource(
            dataset="hemibrain",
            kind="neo4j-inputs",
            url="https://storage.googleapis.com/hemibrain-release/neuprint/hemibrain_v1.2_neo4j_inputs.zip",
            filename="hemibrain_v1.2_neo4j_inputs.zip",
            format="zip",
            table_name="hemibrain_neo4j_inputs",
            description=(
                "Hemibrain v1.2 neuPrint Neo4j import CSV bundle; its connectivity matches neuPrint hemibrain:v1.2.1."
            ),
            sha256="d6bcdba98d7fd1a41be08aff79e5725c22cc24cec4a830e6d422ddfecbb98b6e",
        ),
        BulkSource(
            dataset="hemibrain",
            kind="body-neurotransmitters",
            url="https://storage.googleapis.com/hemibrain/v1.2/hemibrain-v1.2-body-mean-neurotransmitters.feather",
            filename="hemibrain-v1.2-body-mean-neurotransmitters.feather",
            format="feather",
            table_name="hemibrain_body_neurotransmitters",
            description=(
                "Hemibrain v1.2 transmitter predictions per body: mean T-bar class probabilities "
                "from the Eckstein et al. (2024) classifier."
            ),
            sha256="aab49d858415f559f469a9293adfb4d58e423db83a5debb24272ee4d66e059ad",
        ),
    ]
}
HEMIBRAIN_COMPACT_IMPORTS = {
    "traced-roi-connections.csv": "hemibrain_traced_roi_connections",
    "traced-total-connections.csv": "hemibrain_traced_total_connections",
    "traced-neurons.csv": "hemibrain_traced_neurons",
}


def list_sources() -> list[dict[str, str]]:
    return [
        {
            "dataset": source.dataset,
            "kind": source.kind,
            "format": source.format,
            "filename": source.filename,
            "table_name": source.table_name,
            "description": source.description,
            "url": source.url,
            "sha256": source.sha256,
        }
        for source in sorted(BULK_SOURCES.values(), key=lambda item: (item.dataset, item.kind))
    ]


def setup_practical_bulk(
    bulk_dir: Path = DEFAULT_BULK_DIR,
    store: Path = DEFAULT_DUCKDB_PATH,
    datasets: list[str] | None = None,
    replace: bool = True,
    skip_current: bool = False,
) -> list[dict[str, str]]:
    selected = datasets or ["flywire", "hemibrain"]
    rows = []
    for dataset in selected:
        if dataset == "flywire":
            rows.extend(setup_flywire_bulk(bulk_dir, store, replace, skip_current=skip_current))
        elif dataset == "hemibrain":
            rows.extend(setup_hemibrain_bulk(bulk_dir, store, replace, skip_current=skip_current))
    return rows


def setup_flywire_bulk(
    bulk_dir: Path,
    store: Path,
    replace: bool,
    *,
    skip_current: bool = False,
) -> list[dict[str, str]]:
    source = resolve_source("flywire", "proofread-connections")
    rows = setup_file_source_rows(source, bulk_dir, store, replace, skip_current=skip_current)
    rows.extend(setup_optimize_rows(source.dataset, source.table_name, "flywire", store, skip_current=skip_current))
    annotations = resolve_source("flywire", "neuron-annotations")
    rows.extend(setup_label_source_rows(annotations, bulk_dir, store, replace, skip_current=skip_current))
    return rows


def setup_label_source_rows(
    source: BulkSource,
    bulk_dir: Path,
    store: Path,
    replace: bool,
    *,
    skip_current: bool = False,
) -> list[dict[str, str]]:
    """Download and import a label source; a failure gives an ``error`` row instead of stopping setup."""
    try:
        return setup_file_source_rows(source, bulk_dir, store, replace, skip_current=skip_current)
    except (OSError, ValueError) as error:
        # Only the whole-brain commands need the labels, so the graph and olf stages still run.
        path = source_path(source, bulk_dir / "raw")
        return [setup_row(source.dataset, "download", source.kind, f"error: {error}", path, store)]


def setup_file_source_rows(
    source: BulkSource,
    bulk_dir: Path,
    store: Path,
    replace: bool,
    *,
    skip_current: bool = False,
) -> list[dict[str, str]]:
    download_was_current = source_path(source, bulk_dir / "raw").exists()
    path = download_source(
        dataset=source.dataset,
        kind=source.kind,
        output_dir=bulk_dir / "raw",
    )
    imported = import_to_duckdb(
        path=path,
        table_name=source.table_name,
        store=store,
        replace=replace,
        skip_current=skip_current,
    )
    return [
        setup_row(source.dataset, "download", source.kind, "current" if download_was_current else "ok", path, store),
        setup_row(source.dataset, "import", imported["table"], setup_stage_status(imported), path, store),
    ]


def setup_hemibrain_bulk(
    bulk_dir: Path,
    store: Path,
    replace: bool,
    *,
    skip_current: bool = False,
) -> list[dict[str, str]]:
    source = resolve_source("hemibrain", "compact-adjacencies")
    rows = []
    expected_archive = source_path(source, bulk_dir / "raw")
    download_was_current = expected_archive.exists()
    archive = download_source(
        dataset=source.dataset,
        kind=source.kind,
        output_dir=bulk_dir / "raw",
    )
    rows.append(setup_row(source.dataset, "download", source.kind, "current" if download_was_current else "ok", archive, store))
    extract_key = f"extract:{archive_stem(archive)}"
    extract_fingerprint = file_fingerprint(archive)
    extract_current = setup_state_is_current(store, extract_key, extract_fingerprint) if skip_current else False
    extracted = extract_archive_csvs(
        archive,
        output_dir=bulk_dir / "extracted" / archive_stem(archive),
        force=not extract_current,
    )
    if skip_current and extracted:
        write_setup_state_for_store(
            store,
            extract_key,
            extract_fingerprint,
            str(len(extracted)),
        )
    rows.append(
        setup_row(
            source.dataset,
            "extract",
            archive_stem(archive),
            f"current:{len(extracted)}" if extract_current else str(len(extracted)),
            archive,
            store,
        )
    )
    paths = {path.name: path for path in extracted}
    for filename, table in HEMIBRAIN_COMPACT_IMPORTS.items():
        try:
            path = paths[filename]
        except KeyError as exc:
            raise FileNotFoundError(f"missing {filename} in {archive}") from exc
        imported = import_to_duckdb(
            path=path,
            table_name=table,
            store=store,
            replace=replace,
            skip_current=skip_current,
        )
        rows.append(setup_row(source.dataset, "import", imported["table"], setup_stage_status(imported), path, store))
    rows.extend(setup_optimize_rows(source.dataset, source.table_name, "hemibrain", store, skip_current=skip_current))
    transmitters = resolve_source("hemibrain", "body-neurotransmitters")
    rows.extend(setup_label_source_rows(transmitters, bulk_dir, store, replace, skip_current=skip_current))
    return rows


def setup_optimize_rows(
    dataset: str,
    table: str,
    prefix: str,
    store: Path,
    *,
    skip_current: bool = False,
) -> list[dict[str, str]]:
    rows = []
    for row in optimize_connection_table(store, table, prefix=prefix, skip_current=skip_current):
        rows.append(
            setup_row(
                dataset,
                "optimize",
                row.get("name", table),
                row.get("action", "ok"),
                "",
                store,
            )
        )
    return rows


def setup_stage_status(row: dict[str, str]) -> str:
    if row.get("status") == "current":
        return f"current:{row.get('rows', '')}"
    if row.get("status") == "existing":
        return f"existing:{row.get('rows', '')}"
    return row.get("rows", "")


def resolve_source(dataset: str, kind: str) -> BulkSource:
    try:
        return BULK_SOURCES[(dataset, kind)]
    except KeyError as exc:
        available = ", ".join(f"{ds}:{k}" for ds, k in sorted(BULK_SOURCES))
        raise ValueError(f"unknown bulk source {dataset}:{kind}; available: {available}") from exc


def download_source(
    dataset: str,
    kind: str,
    output_dir: Path = DEFAULT_BULK_DIR / "raw",
    force: bool = False,
) -> Path:
    source = resolve_source(dataset, kind)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = source_path(source, output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        verify_source_sha256(path, source)
        return path
    tmp_path = path.with_suffix(path.suffix + ".part")
    with urllib.request.urlopen(source.url) as response, tmp_path.open("wb") as handle:
        shutil.copyfileobj(response, handle)
    try:
        verify_source_sha256(tmp_path, source)
    except ValueError:
        tmp_path.unlink()
        raise
    tmp_path.replace(path)
    write_download_metadata(path, source)
    return path


def verify_source_sha256(path: Path, source: BulkSource) -> None:
    if not source.sha256:
        return
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    if digest.hexdigest() != source.sha256:
        raise ValueError(
            f"sha256 mismatch for {source.dataset}:{source.kind} at {path}: "
            f"expected {source.sha256}, got {digest.hexdigest()}; delete the file and rerun setup"
        )


def source_path(source: BulkSource, output_dir: Path) -> Path:
    return output_dir / source.dataset / source.filename


def write_download_metadata(path: Path, source: BulkSource) -> None:
    payload = asdict(source)
    payload["path"] = str(path)
    path.with_suffix(path.suffix + ".json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n"
    )


def table_summary(store: Path) -> list[dict[str, str]]:
    duckdb = require_duckdb("tables")
    if not store.exists():
        return []
    with duckdb.connect(str(store), read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name <> ?
            ORDER BY table_name
            """,
            [SETUP_STATE_TABLE],
        ).fetchall()
        out = []
        for (table_name,) in rows:
            count = connection.execute(
                f"SELECT count(*) FROM {safe_identifier(table_name)}"
            ).fetchone()[0]
            out.append({"table": table_name, "rows": str(count), "store": str(store)})
        return out
