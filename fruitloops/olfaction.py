from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .bulk import DEFAULT_DUCKDB_PATH, require_duckdb, result_rows, safe_identifier
from .olfaction_labels import sql_classify, sql_glomerulus, sql_side


OLFACTION_PREFIX = "olf"
OLFACTION_REGIONS = ("AL", "LH", "MB")
HEMIBRAIN_CONNECTION_TABLE = "hemibrain_traced_roi_connections"
HEMIBRAIN_NEURON_TABLE = "hemibrain_traced_neurons"
HEMIBRAIN_OLFACTION_ANNOTATION_TABLE = "hemibrain_olfaction_neuron_annotations"
FLYWIRE_CONNECTION_TABLE = "flywire_proofread_connections"
FLYWIRE_HIERARCHICAL_TABLE = "flywire_hierarchical_neuron_annotations"
FLYWIRE_NEURON_INFO_TABLE = "flywire_neuron_information_v2"
FLYWIRE_PROOFREAD_NEURON_TABLE = "flywire_proofread_neurons"


@dataclass(frozen=True)
class ConnectionSpec:
    dataset: str
    table: str
    pre_column: str
    post_column: str
    roi_column: str
    weight_column: str


CONNECTION_SPECS = {
    "hemibrain": ConnectionSpec(
        dataset="hemibrain",
        table=HEMIBRAIN_CONNECTION_TABLE,
        pre_column="bodyId_pre",
        post_column="bodyId_post",
        roi_column="roi",
        weight_column="weight",
    ),
    "flywire": ConnectionSpec(
        dataset="flywire",
        table=FLYWIRE_CONNECTION_TABLE,
        pre_column="pre_pt_root_id",
        post_column="post_pt_root_id",
        roi_column="neuropil",
        weight_column="syn_count",
    ),
}


def build_olfaction_cache(
    store: Path = DEFAULT_DUCKDB_PATH,
    datasets: list[str] | None = None,
    replace: bool = True,
    prefix: str = OLFACTION_PREFIX,
) -> list[dict[str, str]]:
    duckdb = require_duckdb("olfaction build")
    selected = tuple(datasets or CONNECTION_SPECS.keys())
    prefix = safe_identifier(prefix)
    with duckdb.connect(str(store)) as connection:
        if not replace and table_exists(connection, f"{prefix}_neurons"):
            return table_counts(connection, prefix, store, [])
        if replace:
            drop_olfaction_tables(connection, prefix)
        create_connection_table(connection, prefix)
        create_provenance_table(connection, prefix)
        imported = []
        for dataset in selected:
            spec = CONNECTION_SPECS[dataset]
            if not table_exists(connection, spec.table):
                imported.append(missing_source_row(dataset, spec.table, store))
                continue
            insert_connection_rows(connection, prefix, spec)
            imported.append(source_row(connection, prefix, dataset, spec.table, store))
        create_membership_table(connection, prefix)
        create_neuron_table(connection, prefix)
        insert_hemibrain_annotations(connection, prefix)
        insert_flywire_annotations(connection, prefix)
        create_annotation_table(connection, prefix)
        create_total_edge_table(connection, prefix)
        create_neuron_region_table(connection, prefix)
        create_pathway_edge_table(connection, prefix)
        create_pathway_summary_table(connection, prefix)
        create_cell_type_summary_table(connection, prefix)
        create_indexes(connection, prefix)
        return table_counts(connection, prefix, store, imported)


def drop_olfaction_tables(connection, prefix: str) -> None:
    for table in olfaction_table_names(prefix):
        connection.execute(f"DROP TABLE IF EXISTS {table}")


def olfaction_table_names(prefix: str) -> list[str]:
    return [
        f"{prefix}_provenance",
        f"{prefix}_edges_by_neuropil",
        f"{prefix}_edges_total",
        f"{prefix}_neuropil_membership",
        f"{prefix}_neurons",
        f"{prefix}_annotations",
        f"{prefix}_neuron_regions",
        f"{prefix}_pathway_edges",
        f"{prefix}_pathway_summary",
        f"{prefix}_cell_type_summary",
    ]


def create_connection_table(connection, prefix: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {prefix}_edges_by_neuropil (
            dataset VARCHAR,
            pre_id VARCHAR,
            post_id VARCHAR,
            neuropil VARCHAR,
            region VARCHAR,
            hemisphere VARCHAR,
            synapses BIGINT,
            source_table VARCHAR
        )
        """
    )


def create_provenance_table(connection, prefix: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {prefix}_provenance (
            dataset VARCHAR,
            source_table VARCHAR,
            source_rows BIGINT,
            imported_rows BIGINT,
            status VARCHAR,
            store VARCHAR
        )
        """
    )


def insert_connection_rows(connection, prefix: str, spec: ConnectionSpec) -> None:
    connection.execute(
        f"""
        INSERT INTO {prefix}_edges_by_neuropil
        SELECT
            ? AS dataset,
            CAST({safe_identifier(spec.pre_column)} AS VARCHAR) AS pre_id,
            CAST({safe_identifier(spec.post_column)} AS VARCHAR) AS post_id,
            CAST({safe_identifier(spec.roi_column)} AS VARCHAR) AS neuropil,
            CASE
                WHEN upper(CAST({safe_identifier(spec.roi_column)} AS VARCHAR)) LIKE 'AL%' THEN 'AL'
                WHEN upper(CAST({safe_identifier(spec.roi_column)} AS VARCHAR)) LIKE 'LH%' THEN 'LH'
                WHEN upper(CAST({safe_identifier(spec.roi_column)} AS VARCHAR)) LIKE 'MB%' THEN 'MB'
                ELSE ''
            END AS region,
            CASE
                WHEN upper(CAST({safe_identifier(spec.roi_column)} AS VARCHAR)) LIKE '%(R)%'
                  OR upper(CAST({safe_identifier(spec.roi_column)} AS VARCHAR)) LIKE '%_R'
                  OR upper(CAST({safe_identifier(spec.roi_column)} AS VARCHAR)) LIKE '%_R_%'
                THEN 'R'
                WHEN upper(CAST({safe_identifier(spec.roi_column)} AS VARCHAR)) LIKE '%(L)%'
                  OR upper(CAST({safe_identifier(spec.roi_column)} AS VARCHAR)) LIKE '%_L'
                  OR upper(CAST({safe_identifier(spec.roi_column)} AS VARCHAR)) LIKE '%_L_%'
                THEN 'L'
                ELSE ''
            END AS hemisphere,
            CAST({safe_identifier(spec.weight_column)} AS BIGINT) AS synapses,
            ? AS source_table
        FROM {safe_identifier(spec.table)}
        WHERE (
            upper(CAST({safe_identifier(spec.roi_column)} AS VARCHAR)) LIKE 'AL%'
            OR upper(CAST({safe_identifier(spec.roi_column)} AS VARCHAR)) LIKE 'LH%'
            OR upper(CAST({safe_identifier(spec.roi_column)} AS VARCHAR)) LIKE 'MB%'
        )
        AND CAST({safe_identifier(spec.weight_column)} AS BIGINT) > 0
        """,
        [spec.dataset, spec.table],
    )


def create_membership_table(connection, prefix: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE {prefix}_neuropil_membership AS
        WITH rows AS (
            SELECT dataset, post_id AS body_id, neuropil, region, hemisphere,
                   synapses AS input_synapses, 0::BIGINT AS output_synapses
            FROM {prefix}_edges_by_neuropil
            UNION ALL
            SELECT dataset, pre_id AS body_id, neuropil, region, hemisphere,
                   0::BIGINT AS input_synapses, synapses AS output_synapses
            FROM {prefix}_edges_by_neuropil
        )
        SELECT dataset, body_id, neuropil, region, hemisphere,
               sum(input_synapses) AS input_synapses,
               sum(output_synapses) AS output_synapses,
               sum(input_synapses + output_synapses) AS total_synapses
        FROM rows
        GROUP BY dataset, body_id, neuropil, region, hemisphere
        """
    )


def create_neuron_table(connection, prefix: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE {prefix}_neurons AS
        WITH per_body AS (
            SELECT dataset, body_id,
                   string_agg(DISTINCT region, ',' ORDER BY region) AS regions,
                   string_agg(DISTINCT hemisphere, ',' ORDER BY hemisphere) AS hemispheres,
                   sum(input_synapses) AS input_synapses,
                   sum(output_synapses) AS output_synapses,
                   sum(total_synapses) AS total_synapses
            FROM {prefix}_neuropil_membership
            GROUP BY dataset, body_id
        )
        SELECT dataset, body_id, regions, hemispheres,
               input_synapses, output_synapses, total_synapses,
               '' AS primary_name,
               '' AS instance,
               '' AS cell_class,
               '' AS glomerulus,
               '' AS side,
               '' AS aliases,
               '' AS top1_nt,
               NULL::DOUBLE AS top1_probability,
               '' AS top2_nt,
               NULL::DOUBLE AS top2_probability,
               '' AS top3_nt,
               NULL::DOUBLE AS top3_probability
        FROM per_body
        """
    )


def insert_hemibrain_annotations(connection, prefix: str) -> None:
    table = hemibrain_annotation_table(connection)
    if not table:
        return
    connection.execute(
        f"""
        UPDATE {prefix}_neurons AS n
        SET primary_name = coalesce(h.type, ''),
            instance = coalesce(h.instance, ''),
            aliases = coalesce(h.type, ''),
            cell_class = {sql_classify("coalesce(h.type, h.instance, '')")},
            glomerulus = {sql_glomerulus("coalesce(h.type, h.instance, '')")},
            side = {sql_side("coalesce(h.instance, h.type, '')")}
        FROM {table} AS h
        WHERE n.dataset = 'hemibrain'
          AND n.body_id = CAST(h.bodyId AS VARCHAR)
        """
    )


def hemibrain_annotation_table(connection) -> str:
    if table_exists(connection, HEMIBRAIN_OLFACTION_ANNOTATION_TABLE):
        return HEMIBRAIN_OLFACTION_ANNOTATION_TABLE
    if table_exists(connection, HEMIBRAIN_NEURON_TABLE):
        return HEMIBRAIN_NEURON_TABLE
    return ""


def insert_flywire_annotations(connection, prefix: str) -> None:
    if table_exists(connection, FLYWIRE_HIERARCHICAL_TABLE):
        connection.execute(
            f"""
            WITH cell_types AS (
                SELECT CAST(pt_root_id AS VARCHAR) AS body_id,
                       string_agg(DISTINCT cell_type, '; ' ORDER BY cell_type) AS aliases,
                       min(CASE WHEN classification_system = 'cell_type' THEN cell_type ELSE NULL END) AS primary_name,
                       min(CASE WHEN classification_system = 'cell_class' THEN cell_type ELSE NULL END) AS cell_class_label
                FROM {FLYWIRE_HIERARCHICAL_TABLE}
                WHERE pt_root_id IS NOT NULL
                GROUP BY pt_root_id
            )
            UPDATE {prefix}_neurons AS n
                SET primary_name = coalesce(nullif(cell_types.primary_name, ''), n.primary_name),
                aliases = coalesce(nullif(cell_types.aliases, ''), n.aliases),
                cell_class = coalesce(
                    nullif({sql_classify("coalesce(cell_types.cell_class_label, cell_types.primary_name, cell_types.aliases, '')")}, ''),
                    n.cell_class
                ),
                glomerulus = coalesce(nullif({sql_glomerulus("coalesce(cell_types.primary_name, cell_types.aliases, '')")}, ''), n.glomerulus),
                side = coalesce(nullif({sql_side("coalesce(cell_types.primary_name, cell_types.aliases, '')")}, ''), n.side)
            FROM cell_types
            WHERE n.dataset = 'flywire'
              AND n.body_id = cell_types.body_id
            """
        )
    if table_exists(connection, FLYWIRE_NEURON_INFO_TABLE):
        connection.execute(
            f"""
            WITH tags AS (
                SELECT CAST(pt_root_id AS VARCHAR) AS body_id,
                       string_agg(DISTINCT tag, '; ' ORDER BY tag) AS tag_aliases
                FROM {FLYWIRE_NEURON_INFO_TABLE}
                WHERE pt_root_id IS NOT NULL
                GROUP BY pt_root_id
            )
            UPDATE {prefix}_neurons AS n
            SET aliases = trim(concat_ws('; ', nullif(n.aliases, ''), tags.tag_aliases)),
                primary_name = coalesce(nullif(n.primary_name, ''), split_part(tags.tag_aliases, ';', 1)),
                cell_class = coalesce(nullif(n.cell_class, ''), {sql_classify("tags.tag_aliases")}),
                glomerulus = coalesce(nullif(n.glomerulus, ''), {sql_glomerulus("tags.tag_aliases")}),
                side = coalesce(nullif(n.side, ''), {sql_side("tags.tag_aliases")})
            FROM tags
            WHERE n.dataset = 'flywire'
              AND n.body_id = tags.body_id
            """
        )


def create_annotation_table(connection, prefix: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE {prefix}_annotations AS
        SELECT dataset, body_id, primary_name, instance, cell_class, glomerulus, side,
               aliases, top1_nt, top1_probability, top2_nt, top2_probability,
               top3_nt, top3_probability
        FROM {prefix}_neurons
        """
    )


def create_total_edge_table(connection, prefix: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE {prefix}_edges_total AS
        SELECT dataset, pre_id, post_id,
               string_agg(DISTINCT neuropil, ',' ORDER BY neuropil) AS neuropils,
               string_agg(DISTINCT region, ',' ORDER BY region) AS regions,
               string_agg(DISTINCT hemisphere, ',' ORDER BY hemisphere) AS hemispheres,
               sum(synapses) AS synapses,
               string_agg(DISTINCT source_table, ',' ORDER BY source_table) AS source_tables
        FROM {prefix}_edges_by_neuropil
        GROUP BY dataset, pre_id, post_id
        """
    )


def create_neuron_region_table(connection, prefix: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE {prefix}_neuron_regions AS
        SELECT m.dataset,
               m.body_id,
               n.primary_name,
               n.instance,
               n.cell_class,
               n.glomerulus,
               n.side AS cell_body_side,
               m.neuropil,
               m.region,
               m.hemisphere AS neuropil_side,
               m.input_synapses,
               m.output_synapses,
               m.total_synapses
        FROM {prefix}_neuropil_membership AS m
        JOIN {prefix}_neurons AS n
          ON n.dataset = m.dataset AND n.body_id = m.body_id
        """
    )


def create_pathway_edge_table(connection, prefix: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE {prefix}_pathway_edges AS
        SELECT e.dataset,
               e.pre_id,
               e.post_id,
               pre.primary_name AS pre_name,
               post.primary_name AS post_name,
               pre.cell_class AS pre_class,
               post.cell_class AS post_class,
               pre.glomerulus AS pre_glomerulus,
               post.glomerulus AS post_glomerulus,
               pre.side AS pre_side,
               post.side AS post_side,
               e.neuropil,
               e.region,
               e.hemisphere AS neuropil_side,
               {side_relation_sql("pre.side", "post.side")} AS pre_to_post_relation,
               {side_relation_sql("pre.side", "e.hemisphere")} AS pre_to_neuropil_relation,
               {side_relation_sql("post.side", "e.hemisphere")} AS post_to_neuropil_relation,
               e.synapses,
               e.source_table
        FROM {prefix}_edges_by_neuropil AS e
        LEFT JOIN {prefix}_neurons AS pre
          ON pre.dataset = e.dataset AND pre.body_id = e.pre_id
        LEFT JOIN {prefix}_neurons AS post
          ON post.dataset = e.dataset AND post.body_id = e.post_id
        """
    )


def create_pathway_summary_table(connection, prefix: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE {prefix}_pathway_summary AS
        SELECT dataset,
               pre_class,
               post_class,
               pre_glomerulus,
               post_glomerulus,
               region,
               neuropil_side,
               pre_to_post_relation,
               pre_to_neuropil_relation,
               post_to_neuropil_relation,
               count(DISTINCT pre_id) AS pre_neurons,
               count(DISTINCT post_id) AS post_neurons,
               sum(synapses) AS synapses
        FROM {prefix}_pathway_edges
        GROUP BY dataset, pre_class, post_class, pre_glomerulus, post_glomerulus,
                 region, neuropil_side, pre_to_post_relation, pre_to_neuropil_relation,
                 post_to_neuropil_relation
        """
    )


def create_cell_type_summary_table(connection, prefix: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE {prefix}_cell_type_summary AS
        SELECT dataset,
               region,
               neuropil_side,
               cell_class,
               glomerulus,
               cell_body_side,
               count(DISTINCT body_id) AS neurons,
               sum(input_synapses) AS input_synapses,
               sum(output_synapses) AS output_synapses,
               sum(total_synapses) AS total_synapses
        FROM {prefix}_neuron_regions
        GROUP BY dataset, region, neuropil_side, cell_class, glomerulus, cell_body_side
        """
    )


def side_relation_sql(source: str, target: str) -> str:
    return f"""
    CASE
        WHEN coalesce({source}, '') = '' OR coalesce({target}, '') = '' THEN 'unknown'
        WHEN {source} = {target} THEN 'ipsi'
        ELSE 'contra'
    END
    """


def create_indexes(connection, prefix: str) -> None:
    index_specs = [
        (f"{prefix}_neurons", "dataset", "body_id"),
        (f"{prefix}_neurons", "dataset", "cell_class"),
        (f"{prefix}_neurons", "dataset", "glomerulus"),
        (f"{prefix}_annotations", "dataset", "body_id"),
        (f"{prefix}_annotations", "dataset", "cell_class"),
        (f"{prefix}_edges_by_neuropil", "dataset", "pre_id"),
        (f"{prefix}_edges_by_neuropil", "dataset", "post_id"),
        (f"{prefix}_edges_by_neuropil", "dataset", "region"),
        (f"{prefix}_neuropil_membership", "dataset", "body_id"),
        (f"{prefix}_edges_total", "dataset", "pre_id"),
        (f"{prefix}_edges_total", "dataset", "post_id"),
        (f"{prefix}_neuron_regions", "dataset", "cell_class"),
        (f"{prefix}_neuron_regions", "dataset", "region"),
        (f"{prefix}_pathway_edges", "dataset", "pre_class"),
        (f"{prefix}_pathway_edges", "dataset", "post_class"),
        (f"{prefix}_pathway_edges", "dataset", "region"),
        (f"{prefix}_pathway_summary", "dataset", "pre_class"),
        (f"{prefix}_pathway_summary", "dataset", "post_class"),
        (f"{prefix}_cell_type_summary", "dataset", "cell_class"),
    ]
    for table, first, second in index_specs:
        if table_exists(connection, table):
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS {table}_{first}_{second}_idx ON {table} ({first}, {second})"
            )
            connection.execute(f"ANALYZE {table}")


def source_row(connection, prefix: str, dataset: str, table: str, store: Path) -> dict[str, str]:
    source_rows = connection.execute(
        f"SELECT count(*) FROM {safe_identifier(table)}"
    ).fetchone()[0]
    imported_rows = connection.execute(
        f"""
        SELECT count(*) FROM {safe_identifier(prefix)}_edges_by_neuropil
        WHERE dataset = ? AND source_table = ?
        """,
        [dataset, table],
    ).fetchone()[0]
    connection.execute(
        f"""
        INSERT INTO {safe_identifier(prefix)}_provenance
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [dataset, table, source_rows, imported_rows, "imported", str(store)],
    )
    return {
        "dataset": dataset,
        "table": table,
        "rows": str(imported_rows),
        "status": "imported",
        "store": str(store),
    }


def missing_source_row(dataset: str, table: str, store: Path) -> dict[str, str]:
    return {
        "dataset": dataset,
        "table": table,
        "rows": "0",
        "status": "missing",
        "store": str(store),
    }


def table_counts(connection, prefix: str, store: Path, sources: list[dict[str, str]]) -> list[dict[str, str]]:
    rows = list(sources)
    for table in olfaction_table_names(prefix):
        if not table_exists(connection, table):
            continue
        count = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        rows.append(
            {
                "dataset": "all",
                "table": table,
                "rows": str(count),
                "status": "built",
                "store": str(store),
            }
        )
    return rows


def olfaction_tables(store: Path = DEFAULT_DUCKDB_PATH, prefix: str = OLFACTION_PREFIX) -> list[dict[str, str]]:
    duckdb = require_duckdb("olfaction tables")
    prefix = safe_identifier(prefix)
    if not store.exists():
        return []
    with duckdb.connect(str(store), read_only=True) as connection:
        return [
            {
                "table": table,
                "rows": str(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]),
                "store": str(store),
            }
            for table in olfaction_table_names(prefix)
            if table_exists(connection, table)
        ]


def olfaction_neurons(
    store: Path = DEFAULT_DUCKDB_PATH,
    dataset: str | None = None,
    region: str | None = None,
    cell_class: str | None = None,
    glomerulus: str | None = None,
    contains: str | None = None,
    limit: int = 50,
    prefix: str = OLFACTION_PREFIX,
) -> list[dict[str, str]]:
    filters, params = neuron_filters(dataset, region, cell_class, glomerulus, contains)
    sql = f"""
    SELECT dataset, body_id, primary_name, instance, cell_class, glomerulus, side,
           regions, hemispheres, input_synapses, output_synapses, total_synapses,
           aliases, top1_nt, top1_probability, top2_nt, top2_probability, top3_nt, top3_probability
    FROM {safe_identifier(prefix)}_neurons
    {filters}
    ORDER BY total_synapses DESC, dataset, body_id
    LIMIT ?
    """
    return read_sql(store, sql, params + [limit])


def olfaction_edges(
    store: Path = DEFAULT_DUCKDB_PATH,
    dataset: str | None = None,
    region: str | None = None,
    pre_id: str | None = None,
    post_id: str | None = None,
    min_synapses: int = 1,
    limit: int = 50,
    prefix: str = OLFACTION_PREFIX,
) -> list[dict[str, str]]:
    where = ["synapses >= ?"]
    params: list[str | int] = [int(min_synapses)]
    if dataset:
        where.append("dataset = ?")
        params.append(dataset)
    if region:
        where.append("region = ?")
        params.append(region.upper())
    if pre_id:
        where.append("pre_id = ?")
        params.append(pre_id)
    if post_id:
        where.append("post_id = ?")
        params.append(post_id)
    sql = f"""
    SELECT dataset, pre_id, post_id, neuropil, region, hemisphere, synapses, source_table
    FROM {safe_identifier(prefix)}_edges_by_neuropil
    WHERE {" AND ".join(where)}
    ORDER BY synapses DESC, dataset, pre_id, post_id
    LIMIT ?
    """
    return read_sql(store, sql, params + [limit])


def olfaction_pns(
    store: Path = DEFAULT_DUCKDB_PATH,
    dataset: str | None = None,
    glomerulus: str | None = None,
    limit: int = 50,
    prefix: str = OLFACTION_PREFIX,
) -> list[dict[str, str]]:
    return olfaction_neurons(
        store=store,
        dataset=dataset,
        cell_class="PN",
        glomerulus=glomerulus,
        limit=limit,
        prefix=prefix,
    )


def olfaction_class_summary(
    store: Path = DEFAULT_DUCKDB_PATH,
    dataset: str | None = None,
    region: str | None = None,
    cell_class: str | None = None,
    glomerulus: str | None = None,
    limit: int = 100,
    prefix: str = OLFACTION_PREFIX,
) -> list[dict[str, str]]:
    where, params = summary_filters(dataset, region, cell_class, glomerulus)
    sql = f"""
    SELECT dataset, region, neuropil_side, cell_class, glomerulus, cell_body_side,
           neurons, input_synapses, output_synapses, total_synapses
    FROM {safe_identifier(prefix)}_cell_type_summary
    {where}
    ORDER BY total_synapses DESC, neurons DESC, dataset, region, cell_class, glomerulus
    LIMIT ?
    """
    return read_sql(store, sql, params + [limit])


def olfaction_glomerulus_summary(
    store: Path = DEFAULT_DUCKDB_PATH,
    dataset: str | None = None,
    glomerulus: str | None = None,
    limit: int = 100,
    prefix: str = OLFACTION_PREFIX,
) -> list[dict[str, str]]:
    neuron_where = ["glomerulus != ''"]
    pathway_where = ["pre_class = 'ORN'", "post_class = 'PN'", "post_glomerulus != ''"]
    neuron_params: list[str] = []
    pathway_params: list[str] = []
    if dataset:
        neuron_where.append("dataset = ?")
        pathway_where.append("dataset = ?")
        neuron_params.append(dataset)
        pathway_params.append(dataset)
    if glomerulus:
        neuron_where.append("glomerulus = ?")
        pathway_where.append("post_glomerulus = ?")
        neuron_params.append(glomerulus)
        pathway_params.append(glomerulus)
    sql = f"""
    WITH neuron_counts AS (
        SELECT dataset,
               glomerulus,
               count(DISTINCT CASE WHEN cell_class = 'ORN' THEN body_id END) AS orn_count,
               count(DISTINCT CASE WHEN cell_class = 'PN' THEN body_id END) AS pn_count,
               count(DISTINCT CASE WHEN cell_class = 'LN' THEN body_id END) AS ln_count,
               count(DISTINCT body_id) AS total_neurons
        FROM {safe_identifier(prefix)}_neurons
        WHERE {" AND ".join(neuron_where)}
        GROUP BY dataset, glomerulus
    ),
    orn_to_pn AS (
        SELECT dataset,
               post_glomerulus AS glomerulus,
               count(DISTINCT pre_id) AS orn_input_pre_neurons,
               count(DISTINCT post_id) AS pn_target_neurons,
               sum(synapses) AS orn_to_pn_synapses
        FROM {safe_identifier(prefix)}_pathway_edges
        WHERE {" AND ".join(pathway_where)}
        GROUP BY dataset, post_glomerulus
    )
    SELECT n.dataset,
           n.glomerulus,
           n.orn_count,
           n.pn_count,
           n.ln_count,
           n.total_neurons,
           coalesce(p.orn_input_pre_neurons, 0) AS orn_input_pre_neurons,
           coalesce(p.pn_target_neurons, 0) AS pn_target_neurons,
           coalesce(p.orn_to_pn_synapses, 0) AS orn_to_pn_synapses
    FROM neuron_counts AS n
    LEFT JOIN orn_to_pn AS p
      ON p.dataset = n.dataset AND p.glomerulus = n.glomerulus
    ORDER BY orn_to_pn_synapses DESC, total_neurons DESC, n.dataset, n.glomerulus
    LIMIT ?
    """
    return read_sql(store, sql, neuron_params + pathway_params + [limit])


def olfaction_pathway_summary(
    store: Path = DEFAULT_DUCKDB_PATH,
    dataset: str | None = None,
    source_class: str | None = None,
    target_class: str | None = None,
    region: str | None = None,
    glomerulus: str | None = None,
    source_glomerulus: str | None = None,
    target_glomerulus: str | None = None,
    by_side: bool = False,
    limit: int = 100,
    prefix: str = OLFACTION_PREFIX,
) -> list[dict[str, str]]:
    where, params = pathway_filters(
        dataset,
        source_class,
        target_class,
        region,
        glomerulus,
        source_glomerulus,
        target_glomerulus,
    )
    side_columns = ""
    group_side_columns = ""
    if by_side:
        side_fields = "neuropil_side, pre_to_post_relation, pre_to_neuropil_relation, post_to_neuropil_relation"
        side_columns = f", {side_fields}"
        group_side_columns = f", {side_fields}"
    sql = f"""
    SELECT dataset, pre_class AS source_class, post_class AS target_class,
           pre_glomerulus AS source_glomerulus, post_glomerulus AS target_glomerulus,
           region{side_columns},
           count(DISTINCT pre_id) AS source_neurons,
           count(DISTINCT post_id) AS target_neurons,
           sum(synapses) AS synapses
    FROM {safe_identifier(prefix)}_pathway_edges
    {where}
    GROUP BY dataset, pre_class, post_class, pre_glomerulus, post_glomerulus, region
             {group_side_columns}
    ORDER BY synapses DESC, dataset, source_class, target_class
    LIMIT ?
    """
    return read_sql(store, sql, params + [limit])


def olfaction_input_summary(
    store: Path = DEFAULT_DUCKDB_PATH,
    dataset: str | None = None,
    target_class: str | None = None,
    source_class: str | None = None,
    target_id: str | None = None,
    glomerulus: str | None = None,
    region: str | None = None,
    by_side: bool = False,
    limit: int = 100,
    prefix: str = OLFACTION_PREFIX,
) -> list[dict[str, str]]:
    where = []
    params: list[str | int] = []
    if dataset:
        where.append("dataset = ?")
        params.append(dataset)
    if target_class:
        where.append("post_class = ?")
        params.append(target_class.upper())
    if source_class:
        where.append("pre_class = ?")
        params.append(source_class.upper())
    if target_id:
        where.append("post_id = ?")
        params.append(target_id)
    if glomerulus:
        where.append("post_glomerulus = ?")
        params.append(glomerulus)
    if region:
        where.append("region = ?")
        params.append(region.upper())
    filters = f"WHERE {' AND '.join(where)}" if where else ""
    side_columns = ""
    group_side_columns = ""
    if by_side:
        side_columns = (
            ", post_side AS target_side, pre_side AS source_side, neuropil_side,"
            " pre_to_post_relation, pre_to_neuropil_relation, post_to_neuropil_relation"
        )
        group_side_columns = (
            ", post_side, pre_side, neuropil_side,"
            " pre_to_post_relation, pre_to_neuropil_relation, post_to_neuropil_relation"
        )
    sql = f"""
    SELECT dataset,
           post_id AS target_id,
           post_name AS target_name,
           post_class AS target_class,
           post_glomerulus AS target_glomerulus,
           pre_class AS source_class,
           pre_glomerulus AS source_glomerulus,
           count(DISTINCT pre_id) AS source_neurons,
           sum(synapses) AS synapses{side_columns}
    FROM {safe_identifier(prefix)}_pathway_edges
    {filters}
    GROUP BY dataset, post_id, post_name, post_class, post_glomerulus, pre_class, pre_glomerulus
             {group_side_columns}
    ORDER BY synapses DESC, dataset, target_id, source_class
    LIMIT ?
    """
    return read_sql(store, sql, params + [limit])


def olfaction_output_summary(
    store: Path = DEFAULT_DUCKDB_PATH,
    dataset: str | None = None,
    source_class: str | None = None,
    target_class: str | None = None,
    source_id: str | None = None,
    glomerulus: str | None = None,
    region: str | None = None,
    by_side: bool = False,
    limit: int = 100,
    prefix: str = OLFACTION_PREFIX,
) -> list[dict[str, str]]:
    where = []
    params: list[str | int] = []
    if dataset:
        where.append("dataset = ?")
        params.append(dataset)
    if source_class:
        where.append("pre_class = ?")
        params.append(source_class.upper())
    if target_class:
        where.append("post_class = ?")
        params.append(target_class.upper())
    if source_id:
        where.append("pre_id = ?")
        params.append(source_id)
    if glomerulus:
        where.append("pre_glomerulus = ?")
        params.append(glomerulus)
    if region:
        where.append("region = ?")
        params.append(region.upper())
    filters = f"WHERE {' AND '.join(where)}" if where else ""
    side_columns = ""
    group_side_columns = ""
    if by_side:
        side_columns = (
            ", pre_side AS source_side, post_side AS target_side, neuropil_side,"
            " pre_to_post_relation, pre_to_neuropil_relation, post_to_neuropil_relation"
        )
        group_side_columns = (
            ", pre_side, post_side, neuropil_side,"
            " pre_to_post_relation, pre_to_neuropil_relation, post_to_neuropil_relation"
        )
    sql = f"""
    SELECT dataset,
           pre_id AS source_id,
           pre_name AS source_name,
           pre_class AS source_class,
           pre_glomerulus AS source_glomerulus,
           post_class AS target_class,
           post_glomerulus AS target_glomerulus,
           count(DISTINCT post_id) AS target_neurons,
           sum(synapses) AS synapses{side_columns}
    FROM {safe_identifier(prefix)}_pathway_edges
    {filters}
    GROUP BY dataset, pre_id, pre_name, pre_class, pre_glomerulus, post_class, post_glomerulus
             {group_side_columns}
    ORDER BY synapses DESC, dataset, source_id, target_class
    LIMIT ?
    """
    return read_sql(store, sql, params + [limit])


def olfaction_orn_inputs(
    store: Path = DEFAULT_DUCKDB_PATH,
    dataset: str | None = None,
    glomerulus: str | None = None,
    pn_type: str | None = None,
    by_side: bool = False,
    limit: int = 50,
    prefix: str = OLFACTION_PREFIX,
) -> list[dict[str, str]]:
    orn_side_sql = "coalesce(nullif(o.side, ''), nullif(e.hemisphere, ''), '')"
    side_sql = f"CASE WHEN p.side = '' OR {orn_side_sql} = '' THEN 'unknown' WHEN p.side = {orn_side_sql} THEN 'ipsi' ELSE 'contra' END"
    group_side = f", p.side AS pn_side, {orn_side_sql} AS orn_side, {side_sql} AS side_relation" if by_side else ""
    group_by = f", p.side, {orn_side_sql}, {side_sql}" if by_side else ""
    filters = ["o.cell_class = 'ORN'", "p.cell_class = 'PN'"]
    params: list[str | int] = []
    if dataset:
        filters.append("e.dataset = ?")
        params.append(dataset)
    if glomerulus:
        filters.append("(o.glomerulus = ? OR p.glomerulus = ?)")
        params.extend([glomerulus, glomerulus])
    if pn_type:
        filters.append("(p.primary_name = ? OR p.instance = ? OR p.aliases ILIKE ?)")
        params.extend([pn_type, pn_type, f"%{pn_type}%"])
    sql = f"""
    SELECT e.dataset,
           p.body_id AS pn_id,
           p.primary_name AS pn_name,
           p.glomerulus,
           count(DISTINCT o.body_id) AS orn_count,
           sum(e.synapses) AS synapses{group_side}
    FROM {safe_identifier(prefix)}_edges_by_neuropil e
    JOIN {safe_identifier(prefix)}_neurons o
      ON o.dataset = e.dataset AND o.body_id = e.pre_id
    JOIN {safe_identifier(prefix)}_neurons p
      ON p.dataset = e.dataset AND p.body_id = e.post_id
    WHERE {" AND ".join(filters)}
    GROUP BY e.dataset, p.body_id, p.primary_name, p.glomerulus{group_by}
    ORDER BY synapses DESC, e.dataset, p.body_id
    LIMIT ?
    """
    return read_sql(store, sql, params + [limit])


def summary_filters(
    dataset: str | None,
    region: str | None,
    cell_class: str | None,
    glomerulus: str | None,
) -> tuple[str, list[str]]:
    where = []
    params = []
    if dataset:
        where.append("dataset = ?")
        params.append(dataset)
    if region:
        where.append("region = ?")
        params.append(region.upper())
    if cell_class:
        where.append("cell_class = ?")
        params.append(cell_class.upper())
    if glomerulus:
        where.append("glomerulus = ?")
        params.append(glomerulus)
    return (f"WHERE {' AND '.join(where)}" if where else ""), params


def pathway_filters(
    dataset: str | None,
    source_class: str | None,
    target_class: str | None,
    region: str | None,
    glomerulus: str | None,
    source_glomerulus: str | None,
    target_glomerulus: str | None,
) -> tuple[str, list[str]]:
    where = []
    params = []
    if dataset:
        where.append("dataset = ?")
        params.append(dataset)
    if source_class:
        where.append("pre_class = ?")
        params.append(source_class.upper())
    if target_class:
        where.append("post_class = ?")
        params.append(target_class.upper())
    if region:
        where.append("region = ?")
        params.append(region.upper())
    if glomerulus:
        where.append("(pre_glomerulus = ? OR post_glomerulus = ?)")
        params.extend([glomerulus, glomerulus])
    if source_glomerulus:
        where.append("pre_glomerulus = ?")
        params.append(source_glomerulus)
    if target_glomerulus:
        where.append("post_glomerulus = ?")
        params.append(target_glomerulus)
    return (f"WHERE {' AND '.join(where)}" if where else ""), params


def neuron_filters(
    dataset: str | None,
    region: str | None,
    cell_class: str | None,
    glomerulus: str | None,
    contains: str | None,
) -> tuple[str, list[str]]:
    where = []
    params: list[str] = []
    if dataset:
        where.append("dataset = ?")
        params.append(dataset)
    if region:
        where.append("regions ILIKE ?")
        params.append(f"%{region.upper()}%")
    if cell_class:
        where.append("cell_class = ?")
        params.append(cell_class.upper())
    if glomerulus:
        where.append("glomerulus = ?")
        params.append(glomerulus)
    if contains:
        where.append("(body_id ILIKE ? OR primary_name ILIKE ? OR instance ILIKE ? OR aliases ILIKE ?)")
        params.extend([f"%{contains}%"] * 4)
    return (f"WHERE {' AND '.join(where)}" if where else ""), params


def read_sql(store: Path, sql: str, params: list[str | int]) -> list[dict[str, str]]:
    duckdb = require_duckdb("olfaction query")
    with duckdb.connect(str(store), read_only=True) as connection:
        result = connection.execute(sql, params)
        return result_rows(result)


def table_exists(connection, table: str) -> bool:
    return (
        connection.execute(
            """
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [table],
        ).fetchone()[0]
        > 0
    )
