"""Fingerprinted setup stages stored in the DuckDB setup-state table."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from .duckdb_store import require_duckdb, safe_identifier, table_exists, table_row_count

SETUP_STATE_TABLE = "_fruitloops_setup_state"


def ensure_setup_state(connection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SETUP_STATE_TABLE} (
            stage_key VARCHAR PRIMARY KEY,
            source_fingerprint VARCHAR,
            rows VARCHAR,
            updated_at VARCHAR
        )
        """
    )


def setup_state_matches(connection, stage_key: str, source_fingerprint: str) -> bool:
    stored = setup_state_fingerprint(connection, stage_key)
    return stored == source_fingerprint


def setup_state_fingerprint(connection, stage_key: str) -> str:
    if not table_exists(connection, SETUP_STATE_TABLE):
        return ""
    row = connection.execute(
        f"""
        SELECT source_fingerprint
        FROM {SETUP_STATE_TABLE}
        WHERE stage_key = ?
        """,
        [stage_key],
    ).fetchone()
    return str(row[0]) if row else ""


def write_setup_state(
    connection,
    stage_key: str,
    source_fingerprint: str,
    rows: str,
) -> None:
    ensure_setup_state(connection)
    connection.execute(
        f"""
        INSERT OR REPLACE INTO {SETUP_STATE_TABLE}
        VALUES (?, ?, ?, ?)
        """,
        [
            stage_key,
            source_fingerprint,
            str(rows),
            datetime.now(timezone.utc).isoformat(),
        ],
    )


def setup_state_is_current(store: Path, stage_key: str, source_fingerprint: str) -> bool:
    duckdb = require_duckdb("setup state")
    store.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(store)) as connection:
        return setup_state_matches(connection, stage_key, source_fingerprint)


def write_setup_state_for_store(
    store: Path,
    stage_key: str,
    source_fingerprint: str,
    rows: str,
) -> None:
    duckdb = require_duckdb("setup state")
    store.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(store)) as connection:
        write_setup_state(connection, stage_key, source_fingerprint, rows)


def file_fingerprint(path: Path) -> str:
    stat = path.stat()
    payload = {
        "path": str(path.resolve()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }
    return sha256_json(payload)


def table_fingerprint(connection, table: str) -> str:
    table = safe_identifier(table)
    if not table_exists(connection, table):
        return sha256_json({"table": table, "missing": True})
    columns = connection.execute(f"DESCRIBE {table}").fetchall()
    import_fingerprint = setup_state_fingerprint(connection, f"import:{table}")
    return sha256_json(
        {
            "table": table,
            "rows": table_row_count(connection, table),
            "columns": [[str(value) for value in row] for row in columns],
            "import_source_fingerprint": import_fingerprint,
        }
    )


def sha256_json(payload: dict | list) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
