"""Queries, views, and indexes for neuron-to-neuron connection tables."""

from __future__ import annotations

from pathlib import Path

from .duckdb_store import choose_column, require_duckdb, run_sql, safe_identifier, schema_duckdb
from .setup_state import setup_state_matches, table_fingerprint, write_setup_state

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
    duckdb = require_duckdb("views")
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


def optimize_connection_table(
    store: Path,
    table: str,
    prefix: str | None = None,
    skip_current: bool = False,
) -> list[dict[str, str]]:
    duckdb = require_duckdb("optimize")
    table = safe_identifier(table)
    prefix = safe_identifier(prefix or table)
    cols = connection_columns(store, table)
    index_columns = {
        "pre": cols["pre"],
        "post": cols["post"],
        "weight": cols["weight"],
        "roi": cols["roi"],
    }
    actions = []
    with duckdb.connect(str(store)) as connection:
        stage_key = f"optimize:{table}"
        fingerprint = table_fingerprint(connection, table)
        if skip_current and setup_state_matches(connection, stage_key, fingerprint):
            return [{"action": "current", "name": table, "column": "", "store": str(store)}]
        for role, column in index_columns.items():
            if not column:
                continue
            index_name = safe_identifier(f"{prefix}_{role}_idx")
            connection.execute(
                f"CREATE INDEX IF NOT EXISTS {index_name} "
                f"ON {table} ({safe_identifier(column)})"
            )
            actions.append(
                {
                    "action": "index",
                    "name": index_name,
                    "column": column,
                    "store": str(store),
                }
            )
        connection.execute(f"ANALYZE {table}")
        actions.append({"action": "analyze", "name": table, "column": "", "store": str(store)})
        if skip_current:
            write_setup_state(connection, stage_key, fingerprint, str(len(actions)))
    return actions


def view_roi_projection(column: str) -> str:
    if not column:
        return ""
    return f", {safe_identifier(column)} AS roi"
