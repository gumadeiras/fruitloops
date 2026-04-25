from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class TableInfo:
    dataset: str
    collection: str
    file_id: str
    relative_path: str
    rows: int
    columns: tuple[str, ...]
    size_bytes: int
    sha256: str

    @property
    def path_without_suffix(self) -> str:
        path = self.relative_path
        return path[:-4] if path.endswith(".csv") else path


class FruitloopsData:
    def __init__(self, data_dir: Path | None = None) -> None:
        self.data_dir = data_dir or default_data_dir()
        self.manifest_path = self.data_dir / "manifest.csv"
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"missing manifest: {self.manifest_path}")
        self._tables = self._load_manifest()

    def _load_manifest(self) -> list[TableInfo]:
        with self.manifest_path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            tables = []
            for row in reader:
                columns = tuple(filter(None, row["columns"].split("|")))
                tables.append(
                    TableInfo(
                        dataset=row["dataset"],
                        collection=row["collection"],
                        file_id=row["file_id"],
                        relative_path=row["relative_path"],
                        rows=int(row["rows"]),
                        columns=columns,
                        size_bytes=int(row["size_bytes"]),
                        sha256=row["sha256"],
                    )
                )
            return tables

    def datasets(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for table in self._tables:
            counts[table.dataset] = counts.get(table.dataset, 0) + 1
        return dict(sorted(counts.items()))

    def tables(
        self,
        dataset: str | None = None,
        contains: str | None = None,
    ) -> list[TableInfo]:
        needle = contains.lower() if contains else None
        out = []
        for table in self._tables:
            if dataset and table.dataset != dataset:
                continue
            haystack = f"{table.file_id} {table.relative_path} {table.collection}".lower()
            if needle and needle not in haystack:
                continue
            out.append(table)
        return out

    def resolve(self, reference: str) -> TableInfo:
        normalized = reference[:-4] if reference.endswith(".csv") else reference
        candidates = []
        dataset_prefix = ""
        suffix = normalized
        if ":" in normalized:
            dataset_prefix, suffix = normalized.split(":", 1)
        for table in self._tables:
            keys = {
                table.file_id,
                f"{table.dataset}:{table.path_without_suffix}",
                f"{table.dataset}:{table.collection}/{Path(table.path_without_suffix).name}",
                table.path_without_suffix,
            }
            suffix_match = (
                bool(dataset_prefix)
                and dataset_prefix == table.dataset
                and table.path_without_suffix.endswith(suffix)
            )
            if normalized in keys or suffix_match:
                candidates.append(table)
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            refs = ", ".join(table.file_id for table in candidates[:10])
            raise ValueError(f"ambiguous table reference {reference!r}; matches: {refs}")
        raise KeyError(f"unknown table: {reference}")

    def open_table(self, table: TableInfo) -> Iterable[dict[str, str]]:
        path = self.data_dir / table.relative_path
        with path.open(newline="") as handle:
            yield from csv.DictReader(handle)

    def table_path(self, table: TableInfo) -> Path:
        return self.data_dir / table.relative_path


def default_data_dir() -> Path:
    configured = os.environ.get("FRUITLOOPS_DATA_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parents[1] / "data"
