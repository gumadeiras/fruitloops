from __future__ import annotations

import json
from pathlib import Path

from .bulk import DEFAULT_DUCKDB_PATH, require_duckdb, safe_identifier
from .olfaction import (
    CONNECTION_SPECS,
    FLYWIRE_HIERARCHICAL_TABLE,
    FLYWIRE_NEURON_INFO_TABLE,
    FLYWIRE_PROOFREAD_NEURON_TABLE,
    HEMIBRAIN_OLFACTION_ANNOTATION_TABLE,
    OLFACTION_PREFIX,
    build_olfaction_cache,
    table_exists,
)


def cache_olfaction_annotations(
    store: Path = DEFAULT_DUCKDB_PATH,
    datasets: list[str] | None = None,
    chunk_size: int = 2000,
    rebuild: bool = True,
    prefix: str = OLFACTION_PREFIX,
) -> list[dict[str, str]]:
    duckdb = require_duckdb("olfaction annotation cache")
    selected = tuple(datasets or CONNECTION_SPECS.keys())
    prefix = safe_identifier(prefix)
    if not store.exists():
        raise ValueError(f"missing DuckDB store: {store}")
    with duckdb.connect(str(store)) as connection:
        if not table_exists(connection, f"{prefix}_neurons"):
            build_olfaction_cache(store=store, datasets=list(selected), replace=True, prefix=prefix)
        rows = []
        if "hemibrain" in selected:
            rows.append(cache_hemibrain_annotations(connection, store, chunk_size, prefix))
        if "flywire" in selected:
            rows.extend(cache_flywire_annotations(connection, store, chunk_size, prefix))
    if rebuild:
        rows.extend(build_olfaction_cache(store=store, datasets=None, replace=True, prefix=prefix))
    return rows


def cache_hemibrain_annotations(connection, store: Path, chunk_size: int, prefix: str) -> dict[str, str]:
    from .live import fetch_hemibrain_custom, hemibrain_client

    client = hemibrain_client()
    ids = olfaction_body_ids(connection, prefix, "hemibrain")
    frames = []
    for chunk in chunks(ids, chunk_size):
        query = f"""
        MATCH (n:Neuron)
        WHERE n.bodyId IN {json.dumps([int(value) for value in chunk])}
        RETURN n.bodyId AS bodyId, n.type AS type, n.instance AS instance,
               n.status AS status, n.cropped AS cropped, n.size AS size
        ORDER BY n.bodyId
        """
        frames.append(fetch_hemibrain_custom(client, query))
    rows = replace_table_from_frames(connection, HEMIBRAIN_OLFACTION_ANNOTATION_TABLE, frames)
    return annotation_row("hemibrain", HEMIBRAIN_OLFACTION_ANNOTATION_TABLE, rows, store)


def cache_flywire_annotations(connection, store: Path, chunk_size: int, prefix: str) -> list[dict[str, str]]:
    from .live import flywire_client, flywire_config

    client = flywire_client()
    config = flywire_config()
    ids = olfaction_body_ids(connection, prefix, "flywire")
    proofread_rows = replace_table_from_frames(
        connection,
        FLYWIRE_PROOFREAD_NEURON_TABLE,
        flywire_table_chunks(
            client,
            "proofread_neurons",
            "pt_root_id",
            ids,
            chunk_size,
            config.materialization_version,
        ),
    )
    target_ids = [
        str(row[0])
        for row in connection.execute(
            f"SELECT id FROM {FLYWIRE_PROOFREAD_NEURON_TABLE} ORDER BY id"
        ).fetchall()
    ]
    hierarchical_rows = replace_table_from_frames(
        connection,
        FLYWIRE_HIERARCHICAL_TABLE,
        flywire_table_chunks(
            client,
            "hierarchical_neuron_annotations",
            "target_id",
            target_ids,
            chunk_size,
            config.materialization_version,
        ),
    )
    info_rows = replace_table_from_frames(
        connection,
        FLYWIRE_NEURON_INFO_TABLE,
        flywire_table_chunks(
            client,
            "neuron_information_v2",
            "pt_root_id",
            ids,
            chunk_size,
            config.materialization_version,
        ),
    )
    return [
        annotation_row("flywire", FLYWIRE_PROOFREAD_NEURON_TABLE, proofread_rows, store),
        annotation_row("flywire", FLYWIRE_HIERARCHICAL_TABLE, hierarchical_rows, store),
        annotation_row("flywire", FLYWIRE_NEURON_INFO_TABLE, info_rows, store),
    ]


def flywire_table_chunks(
    client,
    table: str,
    column: str,
    values: list[str],
    chunk_size: int,
    materialization_version: int | None,
) -> list[object]:
    frames = []
    for chunk in chunks(values, chunk_size):
        kwargs = {
            "filter_in_dict": {column: [int(value) for value in chunk]},
            "metadata": False,
            "log_warning": False,
        }
        if materialization_version is not None:
            kwargs["materialization_version"] = materialization_version
        frames.append(client.materialize.query_table(table, **kwargs))
    return frames


def olfaction_body_ids(connection, prefix: str, dataset: str) -> list[str]:
    return [
        row[0]
        for row in connection.execute(
            f"""
            SELECT body_id
            FROM {safe_identifier(prefix)}_neurons
            WHERE dataset = ?
            ORDER BY body_id
            """,
            [dataset],
        ).fetchall()
    ]


def replace_table_from_frames(connection, table: str, frames: list[object]) -> int:
    nonempty = [frame for frame in frames if frame is not None and len(frame) > 0]
    connection.execute(f"DROP TABLE IF EXISTS {safe_identifier(table)}")
    if not nonempty:
        create_empty_annotation_table(connection, table)
        return 0
    try:
        import pandas as pd
    except ImportError as exc:
        raise SystemExit(
            "annotation caching requires pandas from the live dependencies. "
            "Install with `python -m pip install -e '.[live,bulk]'`."
        ) from exc
    frame = pd.concat(nonempty, ignore_index=True)
    connection.register("_fruitloops_annotation_import", frame)
    connection.execute(
        f"CREATE TABLE {safe_identifier(table)} AS SELECT * FROM _fruitloops_annotation_import"
    )
    connection.unregister("_fruitloops_annotation_import")
    return int(connection.execute(f"SELECT count(*) FROM {safe_identifier(table)}").fetchone()[0])


def create_empty_annotation_table(connection, table: str) -> None:
    if table == HEMIBRAIN_OLFACTION_ANNOTATION_TABLE:
        schema = "bodyId BIGINT, type VARCHAR, instance VARCHAR, status VARCHAR, cropped BOOLEAN, size BIGINT"
    elif table == FLYWIRE_HIERARCHICAL_TABLE:
        schema = "pt_root_id BIGINT, classification_system VARCHAR, cell_type VARCHAR"
    elif table == FLYWIRE_NEURON_INFO_TABLE:
        schema = "pt_root_id BIGINT, tag VARCHAR"
    else:
        schema = "id BIGINT, pt_root_id BIGINT"
    connection.execute(f"CREATE TABLE {safe_identifier(table)} ({schema})")


def annotation_row(dataset: str, table: str, rows: int, store: Path) -> dict[str, str]:
    return {
        "dataset": dataset,
        "table": table,
        "rows": str(rows),
        "status": "cached",
        "store": str(store),
    }


def chunks(values: list[str], size: int) -> list[list[str]]:
    size = max(1, int(size))
    return [values[index : index + size] for index in range(0, len(values), size)]
