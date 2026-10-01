"""DuckDB access helpers shared by bulk imports, setup state, and queries."""

from __future__ import annotations

from pathlib import Path

from .paths import default_duckdb_path

DEFAULT_DUCKDB_PATH = default_duckdb_path()


def query_duckdb(
    store: Path,
    table: str,
    select: list[str],
    where: list[tuple[str, str]],
    limit: int,
) -> list[dict[str, str]]:
    duckdb = require_duckdb("query")
    table = safe_identifier(table)
    select_sql = ", ".join(safe_identifier(column) for column in select) if select else "*"
    where_sql, params = where_clause(where)
    sql = f"SELECT {select_sql} FROM {table}{where_sql} LIMIT ?"
    params.append(int(limit))
    with duckdb.connect(str(store), read_only=True) as connection:
        result = connection.execute(sql, params)
        return result_rows(result)


def schema_duckdb(store: Path, table: str) -> list[dict[str, str]]:
    duckdb = require_duckdb("schema")
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


def table_exists(connection, table: str) -> bool:
    table = safe_identifier(table)
    row = connection.execute(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table],
    ).fetchone()
    return bool(row and row[0])


def table_row_count(connection, table: str) -> str:
    return str(connection.execute(f"SELECT count(*) FROM {safe_identifier(table)}").fetchone()[0])


def run_sql(store: Path, sql: str, params: list[str | int]) -> list[dict[str, str]]:
    duckdb = require_duckdb("query")
    with duckdb.connect(str(store), read_only=True) as connection:
        result = connection.execute(sql, params)
        return result_rows(result)


def require_duckdb(action: str):
    try:
        import duckdb
    except ImportError as exc:
        raise SystemExit(
            f"bulk {action} requires duckdb. Install or reinstall fruitloops to include runtime dependencies."
        ) from exc
    return duckdb


def result_rows(result) -> list[dict[str, str]]:
    columns = [item[0] for item in result.description]
    return [
        {column: "" if value is None else str(value) for column, value in zip(columns, row)}
        for row in result.fetchall()
    ]


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


def where_clause(where: list[tuple[str, str]]) -> tuple[str, list[str]]:
    if not where:
        return "", []
    clauses = []
    params = []
    for column, value in where:
        clauses.append(f"{safe_identifier(column)} = ?")
        params.append(value)
    return " WHERE " + " AND ".join(clauses), params
