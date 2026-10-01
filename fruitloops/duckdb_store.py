"""DuckDB access helpers shared by bulk imports, setup state, and queries."""

from __future__ import annotations

from contextlib import contextmanager
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
    table = safe_identifier(table)
    select_sql = ", ".join(safe_identifier(column) for column in select) if select else "*"
    where_sql, params = where_clause(where)
    sql = f"SELECT {select_sql} FROM {table}{where_sql} LIMIT ?"
    params.append(int(limit))
    with connect_read_only(store, "query") as connection:
        result = connection.execute(sql, params)
        return result_rows(result)


def schema_duckdb(store: Path, table: str) -> list[dict[str, str]]:
    table = safe_identifier(table)
    with connect_read_only(store, "schema") as connection:
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
    with connect_read_only(store, "query") as connection:
        result = connection.execute(sql, params)
        return result_rows(result)


@contextmanager
def connect_read_only(store: Path, action: str):
    """Open ``store`` read-only; a lock held by another process stops the command with a clear message."""
    duckdb = require_duckdb(action)
    try:
        connection = duckdb.connect(str(store), read_only=True)
    except duckdb.IOException as error:
        raise SystemExit(
            f"cannot open {store} for {action}: {error}; another fruitloops process may be writing to it"
        ) from error
    with connection:
        yield connection


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


def where_clause(where: list[tuple[str, str]]) -> tuple[str, list[str]]:
    if not where:
        return "", []
    clauses = []
    params = []
    for column, value in where:
        clauses.append(f"{safe_identifier(column)} = ?")
        params.append(value)
    return " WHERE " + " AND ".join(clauses), params
