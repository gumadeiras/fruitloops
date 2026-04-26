from __future__ import annotations

import json
import shutil
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path


DEFAULT_BULK_DIR = Path("bulk")
DEFAULT_DUCKDB_PATH = DEFAULT_BULK_DIR / "fruitloops.duckdb"
PRE_COLUMNS = (
    "pre_pt_root_id",
    "pre_root_id",
    "upstream_bodyId",
    "upstream_bodyid",
    "bodyId_pre",
    "bodyid_pre",
    "pre",
)
POST_COLUMNS = (
    "post_pt_root_id",
    "post_root_id",
    "downstream_bodyId",
    "downstream_bodyid",
    "bodyId_post",
    "bodyid_post",
    "post",
)
WEIGHT_COLUMNS = ("n_synapses", "syn_count", "weight", "count", "synapses")
ROI_COLUMNS = ("neuropil", "roi", "ROI", "region")


@dataclass(frozen=True)
class BulkSource:
    dataset: str
    kind: str
    url: str
    filename: str
    format: str
    table_name: str
    description: str


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
            description="Hemibrain v1.2 neuPrint Neo4j import CSV bundle.",
        ),
    ]
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
        }
        for source in sorted(BULK_SOURCES.values(), key=lambda item: (item.dataset, item.kind))
    ]


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
    path = output_dir / source.dataset / source.filename
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        return path
    tmp_path = path.with_suffix(path.suffix + ".part")
    with urllib.request.urlopen(source.url) as response, tmp_path.open("wb") as handle:
        shutil.copyfileobj(response, handle)
    tmp_path.replace(path)
    write_download_metadata(path, source)
    return path


def write_download_metadata(path: Path, source: BulkSource) -> None:
    payload = asdict(source)
    payload["path"] = str(path)
    path.with_suffix(path.suffix + ".json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n"
    )


def import_to_duckdb(
    path: Path,
    table_name: str,
    store: Path = DEFAULT_DUCKDB_PATH,
    replace: bool = False,
) -> dict[str, str]:
    try:
        import duckdb
    except ImportError as exc:
        raise SystemExit(
            "bulk import/query requires duckdb. Install with `python -m pip install -e '.[bulk]'`."
        ) from exc
    store.parent.mkdir(parents=True, exist_ok=True)
    table_name = safe_identifier(table_name)
    with duckdb.connect(str(store)) as connection:
        if replace:
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        if path.suffix == ".csv":
            connection.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto(?)",
                [str(path)],
            )
        elif path.suffix == ".parquet":
            connection.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet(?)",
                [str(path)],
            )
        elif path.suffix == ".feather":
            import_feather(connection, path, table_name)
        else:
            raise ValueError(f"unsupported import format: {path.suffix}")
        rows = connection.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
    return {"store": str(store), "table": table_name, "rows": str(rows)}


def import_feather(connection, path: Path, table_name: str) -> None:
    try:
        import pyarrow.feather as feather
    except ImportError as exc:
        raise SystemExit(
            "Feather import requires pyarrow. Install with `python -m pip install -e '.[bulk]'`."
        ) from exc
    table = feather.read_table(path)
    connection.register("_fruitloops_arrow_import", table)
    connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM _fruitloops_arrow_import")
    connection.unregister("_fruitloops_arrow_import")


def query_duckdb(
    store: Path,
    table: str,
    select: list[str],
    where: list[tuple[str, str]],
    limit: int,
) -> list[dict[str, str]]:
    try:
        import duckdb
    except ImportError as exc:
        raise SystemExit(
            "bulk query requires duckdb. Install with `python -m pip install -e '.[bulk]'`."
        ) from exc
    table = safe_identifier(table)
    select_sql = ", ".join(safe_identifier(column) for column in select) if select else "*"
    where_sql, params = where_clause(where)
    sql = f"SELECT {select_sql} FROM {table}{where_sql} LIMIT ?"
    params.append(int(limit))
    with duckdb.connect(str(store), read_only=True) as connection:
        result = connection.execute(sql, params)
        columns = [item[0] for item in result.description]
        return [
            {column: "" if value is None else str(value) for column, value in zip(columns, row)}
            for row in result.fetchall()
        ]


def schema_duckdb(store: Path, table: str) -> list[dict[str, str]]:
    try:
        import duckdb
    except ImportError as exc:
        raise SystemExit(
            "bulk schema requires duckdb. Install with `python -m pip install -e '.[bulk]'`."
        ) from exc
    table = safe_identifier(table)
    with duckdb.connect(str(store), read_only=True) as connection:
        rows = connection.execute(f"DESCRIBE {table}").fetchall()
    return [
        {
            "column": str(row[0]),
            "type": str(row[1]),
            "nullable": str(row[2]),
        }
        for row in rows
    ]


def table_summary(store: Path) -> list[dict[str, str]]:
    try:
        import duckdb
    except ImportError as exc:
        raise SystemExit(
            "bulk tables requires duckdb. Install with `python -m pip install -e '.[bulk]'`."
        ) from exc
    if not store.exists():
        return []
    with duckdb.connect(str(store), read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
            """
        ).fetchall()
        out = []
        for (table_name,) in rows:
            count = connection.execute(
                f"SELECT count(*) FROM {safe_identifier(table_name)}"
            ).fetchone()[0]
            out.append({"table": table_name, "rows": str(count), "store": str(store)})
        return out


def connection_columns(store: Path, table: str) -> dict[str, str]:
    columns = [row["column"] for row in schema_duckdb(store, table)]
    return {
        "pre": choose_column(columns, PRE_COLUMNS),
        "post": choose_column(columns, POST_COLUMNS),
        "weight": choose_column(columns, WEIGHT_COLUMNS),
        "roi": choose_column(columns, ROI_COLUMNS, required=False),
    }


def connection_rows(
    store: Path,
    table: str,
    pre_id: str | None = None,
    post_id: str | None = None,
    min_weight: int = 1,
    limit: int = 50,
) -> list[dict[str, str]]:
    cols = connection_columns(store, table)
    where = []
    params: list[str | int] = []
    if pre_id:
        where.append(f"{safe_identifier(cols['pre'])} = ?")
        params.append(pre_id)
    if post_id:
        where.append(f"{safe_identifier(cols['post'])} = ?")
        params.append(post_id)
    if cols["weight"]:
        where.append(f"{safe_identifier(cols['weight'])} >= ?")
        params.append(int(min_weight))
    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
    order_sql = f" ORDER BY {safe_identifier(cols['weight'])} DESC" if cols["weight"] else ""
    return run_sql(store, f"SELECT * FROM {safe_identifier(table)}{where_sql}{order_sql} LIMIT ?", params + [int(limit)])


def partner_rows(
    store: Path,
    table: str,
    body_id: str,
    direction: str,
    min_weight: int = 1,
    limit: int = 50,
) -> list[dict[str, str]]:
    cols = connection_columns(store, table)
    if not cols["weight"]:
        raise ValueError(f"could not infer weight column for {table}")
    if direction == "inputs":
        body_col = cols["post"]
        partner_col = cols["pre"]
    elif direction == "outputs":
        body_col = cols["pre"]
        partner_col = cols["post"]
    else:
        raise ValueError(f"unsupported direction: {direction}")
    roi_select = f", {safe_identifier(cols['roi'])} AS roi" if cols["roi"] else ""
    roi_group = f", {safe_identifier(cols['roi'])}" if cols["roi"] else ""
    sql = f"""
    SELECT {safe_identifier(partner_col)} AS partner_id{roi_select},
           sum({safe_identifier(cols['weight'])}) AS total_weight,
           count(*) AS connection_rows
    FROM {safe_identifier(table)}
    WHERE {safe_identifier(body_col)} = ?
      AND {safe_identifier(cols['weight'])} >= ?
    GROUP BY {safe_identifier(partner_col)}{roi_group}
    ORDER BY total_weight DESC
    LIMIT ?
    """
    return run_sql(store, sql, [body_id, int(min_weight), int(limit)])


def create_common_views(store: Path, table: str, prefix: str | None = None) -> list[dict[str, str]]:
    try:
        import duckdb
    except ImportError as exc:
        raise SystemExit(
            "bulk views requires duckdb. Install with `python -m pip install -e '.[bulk]'`."
        ) from exc
    table = safe_identifier(table)
    prefix = safe_identifier(prefix or table)
    cols = connection_columns(store, table)
    partner_roi_select = ", roi" if cols["roi"] else ""
    partner_roi_group = ", roi" if cols["roi"] else ""
    created = []
    with duckdb.connect(str(store)) as connection:
        connection.execute(
            f"""
            CREATE OR REPLACE VIEW {prefix}_edges AS
            SELECT {safe_identifier(cols['pre'])} AS pre_id,
                   {safe_identifier(cols['post'])} AS post_id,
                   {safe_identifier(cols['weight']) if cols['weight'] else '1'} AS weight
                   {view_roi_projection(cols['roi'])}
            FROM {table}
            """
        )
        created.append({"view": f"{prefix}_edges", "store": str(store)})
        connection.execute(
            f"""
            CREATE OR REPLACE VIEW {prefix}_partners AS
            SELECT pre_id AS body_id, post_id AS partner_id, 'output' AS direction{partner_roi_select},
                   sum(weight) AS total_weight, count(*) AS connection_rows
            FROM {prefix}_edges
            GROUP BY pre_id, post_id{partner_roi_group}
            UNION ALL
            SELECT post_id AS body_id, pre_id AS partner_id, 'input' AS direction{partner_roi_select},
                   sum(weight) AS total_weight, count(*) AS connection_rows
            FROM {prefix}_edges
            GROUP BY post_id, pre_id{partner_roi_group}
            """
        )
        created.append({"view": f"{prefix}_partners", "store": str(store)})
    return created


def run_sql(store: Path, sql: str, params: list[str | int]) -> list[dict[str, str]]:
    try:
        import duckdb
    except ImportError as exc:
        raise SystemExit(
            "bulk query requires duckdb. Install with `python -m pip install -e '.[bulk]'`."
        ) from exc
    with duckdb.connect(str(store), read_only=True) as connection:
        result = connection.execute(sql, params)
        columns = [item[0] for item in result.description]
        return [
            {column: "" if value is None else str(value) for column, value in zip(columns, row)}
            for row in result.fetchall()
        ]


def extract_archive_csvs(path: Path, output_dir: Path, force: bool = False) -> list[Path]:
    if path.suffix == ".zip":
        return extract_zip_csvs(path, output_dir, force)
    if path.suffixes[-2:] == [".tar", ".gz"] or path.suffix == ".tgz":
        return extract_tar_csvs(path, output_dir, force)
    raise ValueError(f"unsupported archive format: {path}")


def archive_stem(path: Path) -> str:
    if path.suffixes[-2:] == [".tar", ".gz"]:
        return path.name[: -len(".tar.gz")]
    if path.suffix == ".tgz":
        return path.name[: -len(".tgz")]
    return path.stem


def extract_zip_csvs(path: Path, output_dir: Path, force: bool = False) -> list[Path]:
    import zipfile

    output_dir.mkdir(parents=True, exist_ok=True)
    written = []
    with zipfile.ZipFile(path) as archive:
        for member in archive.namelist():
            if not member.endswith(".csv"):
                continue
            target = output_dir / Path(member).name
            if target.exists() and not force:
                written.append(target)
                continue
            with archive.open(member) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            written.append(target)
    return written


def extract_tar_csvs(path: Path, output_dir: Path, force: bool = False) -> list[Path]:
    import tarfile

    output_dir.mkdir(parents=True, exist_ok=True)
    written = []
    with tarfile.open(path) as archive:
        for member in archive.getmembers():
            if not member.isfile() or not member.name.endswith(".csv"):
                continue
            target = output_dir / Path(member.name).name
            if target.exists() and not force:
                written.append(target)
                continue
            source = archive.extractfile(member)
            if source is None:
                continue
            with source, target.open("wb") as dst:
                shutil.copyfileobj(source, dst)
            written.append(target)
    return written


def safe_identifier(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char == "_" else "_" for char in value)
    if not cleaned or cleaned[0].isdigit():
        cleaned = f"t_{cleaned}"
    return cleaned


def choose_column(
    columns: list[str],
    candidates: tuple[str, ...],
    required: bool = True,
) -> str:
    lookup = {column.lower(): column for column in columns}
    for candidate in candidates:
        if candidate.lower() in lookup:
            return lookup[candidate.lower()]
    if required:
        raise ValueError(f"could not infer column from candidates {candidates}; columns={columns}")
    return ""


def view_roi_projection(column: str) -> str:
    if not column:
        return ""
    return f", {safe_identifier(column)} AS roi"


def where_clause(where: list[tuple[str, str]]) -> tuple[str, list[str]]:
    if not where:
        return "", []
    clauses = []
    params = []
    for column, value in where:
        clauses.append(f"{safe_identifier(column)} = ?")
        params.append(value)
    return " WHERE " + " AND ".join(clauses), params
