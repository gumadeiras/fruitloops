from __future__ import annotations

from pathlib import Path

from .duckdb_store import DEFAULT_DUCKDB_PATH, require_duckdb, safe_identifier
from .olfaction import (
    OLFACTION_PREFIX,
    build_olfaction_cache,
    olfaction_built_datasets,
    olfaction_cache_exists,
    olfaction_cache_is_current,
    olfaction_source_fingerprint,
)


def refresh_stale_olfaction_cache(
    store: Path = DEFAULT_DUCKDB_PATH,
    prefix: str = OLFACTION_PREFIX,
) -> bool:
    duckdb = require_duckdb("olfaction freshness check")
    prefix = safe_identifier(prefix)
    if not store.exists():
        return False
    with duckdb.connect(str(store), read_only=True) as connection:
        if not olfaction_cache_exists(connection, prefix):
            return False
        selected = olfaction_built_datasets(connection, prefix)
        fingerprint = olfaction_source_fingerprint(connection, selected, prefix)
        if olfaction_cache_is_current(connection, prefix, selected, fingerprint):
            return False
    rows = build_olfaction_cache(
        store=store,
        datasets=list(selected),
        replace=True,
        prefix=prefix,
        skip_current=True,
    )
    return any(row["status"] == "built" for row in rows)
