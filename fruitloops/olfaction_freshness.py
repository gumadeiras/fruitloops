from __future__ import annotations

from pathlib import Path

from .bulk import (
    DEFAULT_DUCKDB_PATH,
    SETUP_STATE_TABLE,
    require_duckdb,
    safe_identifier,
    table_exists,
)
from .olfaction import (
    CONNECTION_SPECS,
    FLYWIRE_HIERARCHICAL_TABLE,
    FLYWIRE_NEURON_INFO_TABLE,
    OLFACTION_PREFIX,
    HEMIBRAIN_OLFACTION_ORN_PN_TABLE,
    build_olfaction_cache,
    hemibrain_annotation_table,
    olfaction_cache_exists,
    olfaction_source_fingerprint,
)
from .olfaction_labels import sql_classify, sql_glomerulus


def ensure_olfaction_annotations_applied(
    store: Path = DEFAULT_DUCKDB_PATH,
    datasets: list[str] | None = None,
    prefix: str = OLFACTION_PREFIX,
) -> bool:
    duckdb = require_duckdb("olfaction annotation freshness check")
    selected = tuple(datasets or CONNECTION_SPECS.keys())
    prefix = safe_identifier(prefix)
    if not store.exists():
        return False
    with duckdb.connect(str(store)) as connection:
        if not olfaction_cache_exists(connection, prefix):
            return False
        if olfaction_stored_fingerprint_is_stale(connection, selected, prefix):
            stale = list(selected)
        else:
            stale = []
        all_datasets = tuple(CONNECTION_SPECS.keys())
        if selected != all_datasets and olfaction_stored_fingerprint_is_stale(
            connection,
            all_datasets,
            prefix,
        ):
            stale = list(all_datasets)
        stale = [
            dataset
            for dataset in selected
            if annotation_source_label_count(connection, prefix, dataset) > 0
            and derived_label_count(connection, prefix, dataset) == 0
        ] or stale
    if not stale:
        return False
    build_olfaction_cache(
        store=store,
        datasets=None,
        replace=True,
        prefix=prefix,
        skip_current=True,
    )
    return True


def olfaction_stored_fingerprint_is_stale(
    connection,
    selected: tuple[str, ...],
    prefix: str,
) -> bool:
    if not table_exists(connection, SETUP_STATE_TABLE):
        return False
    stage_key = f"olfaction:{prefix}:{','.join(sorted(selected))}"
    row = connection.execute(
        f"""
        SELECT source_fingerprint
        FROM {SETUP_STATE_TABLE}
        WHERE stage_key = ?
        """,
        [stage_key],
    ).fetchone()
    stored = str(row[0]) if row else ""
    if not stored:
        return False
    return stored != olfaction_source_fingerprint(connection, selected, prefix)


def derived_label_count(connection, prefix: str, dataset: str) -> int:
    if not table_exists(connection, f"{prefix}_neurons"):
        return 0
    return int(
        connection.execute(
            f"""
            SELECT count(*)
            FROM {prefix}_neurons
            WHERE dataset = ?
              AND (cell_class <> '' OR glomerulus <> '')
            """,
            [dataset],
        ).fetchone()[0]
    )


def annotation_source_label_count(connection, prefix: str, dataset: str) -> int:
    if dataset == "hemibrain":
        return hemibrain_annotation_source_label_count(connection, prefix)
    if dataset == "flywire":
        return flywire_annotation_source_label_count(connection, prefix)
    return 0


def hemibrain_annotation_source_label_count(connection, prefix: str) -> int:
    counts = []
    if table_exists(connection, HEMIBRAIN_OLFACTION_ORN_PN_TABLE):
        counts.append(
            int(
                connection.execute(
                    f"""
                    SELECT count(DISTINCT n.body_id)
                    FROM {prefix}_neurons AS n
                    JOIN {HEMIBRAIN_OLFACTION_ORN_PN_TABLE} AS h
                      ON n.body_id IN (CAST(h.bodyId_pre AS VARCHAR), CAST(h.bodyId_post AS VARCHAR))
                    WHERE n.dataset = 'hemibrain'
                      AND (
                        {sql_classify("coalesce(h.pre_type, h.pre_instance, h.post_type, h.post_instance, '')")} <> ''
                        OR {sql_glomerulus("coalesce(h.pre_type, h.pre_instance, h.post_type, h.post_instance, '')")} <> ''
                      )
                    """
                ).fetchone()[0]
            )
        )
    table = hemibrain_annotation_table(connection)
    if not table:
        return max(counts, default=0)
    counts.append(
        int(
            connection.execute(
                f"""
                SELECT count(DISTINCT n.body_id)
                FROM {prefix}_neurons AS n
                JOIN {safe_identifier(table)} AS h
                  ON n.body_id = CAST(h.bodyId AS VARCHAR)
                WHERE n.dataset = 'hemibrain'
                  AND (
                    {sql_classify("coalesce(h.type, h.instance, '')")} <> ''
                    OR {sql_glomerulus("coalesce(h.type, h.instance, '')")} <> ''
                  )
                """
            ).fetchone()[0]
        )
    )
    return max(counts, default=0)


def flywire_annotation_source_label_count(connection, prefix: str) -> int:
    counts = []
    if table_exists(connection, FLYWIRE_HIERARCHICAL_TABLE):
        counts.append(
            int(
                connection.execute(
                    f"""
                    SELECT count(DISTINCT n.body_id)
                    FROM {prefix}_neurons AS n
                    JOIN {FLYWIRE_HIERARCHICAL_TABLE} AS h
                      ON n.body_id = CAST(h.pt_root_id AS VARCHAR)
                    WHERE n.dataset = 'flywire'
                      AND (
                        {sql_classify("coalesce(h.cell_type, '')")} <> ''
                        OR {sql_glomerulus("coalesce(h.cell_type, '')")} <> ''
                      )
                    """
                ).fetchone()[0]
            )
        )
    if table_exists(connection, FLYWIRE_NEURON_INFO_TABLE):
        counts.append(
            int(
                connection.execute(
                    f"""
                    SELECT count(DISTINCT n.body_id)
                    FROM {prefix}_neurons AS n
                    JOIN {FLYWIRE_NEURON_INFO_TABLE} AS h
                      ON n.body_id = CAST(h.pt_root_id AS VARCHAR)
                    WHERE n.dataset = 'flywire'
                      AND (
                        {sql_classify("coalesce(h.tag, '')")} <> ''
                        OR {sql_glomerulus("coalesce(h.tag, '')")} <> ''
                      )
                    """
                ).fetchone()[0]
            )
        )
    return max(counts, default=0)
