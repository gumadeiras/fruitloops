from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


DEFAULT_CACHE_DIR = Path("cache/live")


@dataclass(frozen=True)
class CacheEntry:
    key: str
    dataset: str
    action: str
    path: Path
    metadata_path: Path
    rows: int
    created_at: str
    query: dict[str, object]


def cache_key(dataset: str, action: str, query: dict[str, object]) -> str:
    payload = json.dumps(
        {"dataset": dataset, "action": action, "query": query},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:20]


def cache_paths(
    cache_dir: Path,
    dataset: str,
    action: str,
    query: dict[str, object],
) -> tuple[str, Path, Path]:
    key = cache_key(dataset, action, query)
    base = cache_dir / dataset / action / key
    return key, base.with_suffix(".csv"), base.with_suffix(".json")


def get_or_fetch(
    cache_dir: Path,
    dataset: str,
    action: str,
    query: dict[str, object],
    fetch: Callable[[], list[dict[str, str]]],
    refresh: bool = False,
    offline_only: bool = False,
) -> tuple[list[dict[str, str]], CacheEntry | None, str]:
    key, path, metadata_path = cache_paths(cache_dir, dataset, action, query)
    if path.exists() and not refresh:
        rows = read_rows(path)
        return rows, read_metadata(metadata_path, path, key), "cache"
    if offline_only:
        return [], None, "miss"
    rows = fetch()
    write_rows(path, rows)
    entry = write_metadata(
        metadata_path,
        path,
        key=key,
        dataset=dataset,
        action=action,
        query=query,
        rows=len(rows),
    )
    return rows, entry, "live"


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        if fieldnames:
            writer.writerows(rows)


def write_metadata(
    metadata_path: Path,
    data_path: Path,
    key: str,
    dataset: str,
    action: str,
    query: dict[str, object],
    rows: int,
) -> CacheEntry:
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "key": key,
        "dataset": dataset,
        "action": action,
        "path": str(data_path),
        "rows": rows,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "query": query,
    }
    metadata_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return metadata_to_entry(payload, metadata_path)


def read_metadata(metadata_path: Path, data_path: Path, key: str) -> CacheEntry | None:
    if not metadata_path.exists():
        return None
    payload = json.loads(metadata_path.read_text())
    payload.setdefault("key", key)
    payload.setdefault("path", str(data_path))
    return metadata_to_entry(payload, metadata_path)


def metadata_to_entry(payload: dict[str, object], metadata_path: Path) -> CacheEntry:
    return CacheEntry(
        key=str(payload["key"]),
        dataset=str(payload["dataset"]),
        action=str(payload["action"]),
        path=Path(str(payload["path"])),
        metadata_path=metadata_path,
        rows=int(payload["rows"]),
        created_at=str(payload["created_at"]),
        query=dict(payload["query"]),
    )


def list_cache(cache_dir: Path) -> list[CacheEntry]:
    entries = []
    for metadata_path in sorted(cache_dir.rglob("*.json")):
        try:
            payload = json.loads(metadata_path.read_text())
            entries.append(metadata_to_entry(payload, metadata_path))
        except Exception:
            continue
    return entries
