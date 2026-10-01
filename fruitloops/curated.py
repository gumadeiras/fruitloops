"""Curated reference tables packaged with fruitloops.

Each assignment in these tables cites a primary source. Rows marked
`verified=false` are kept for reference but never used to build seeds.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources

GLOMERULUS_FAMILIES_FILE = "glomerulus_receptor_families.csv"
FAMILY_ORDER = ("orco", "ir", "gr", "amt", "thermo", "hygro")
TRANSMITTER_OVERRIDES_FILE = "transmitter_overrides.csv"


@dataclass(frozen=True)
class GlomerulusFamily:
    glomerulus: str
    family: str
    receptor: str
    verified: bool
    source: str
    doi: str
    evidence: str
    note: str


@dataclass(frozen=True)
class TransmitterOverride:
    dataset: str
    field: str
    value: str
    transmitter: str
    source: str
    doi: str
    note: str


def read_curated_rows(name: str) -> list[dict[str, str]]:
    path = resources.files("fruitloops").joinpath("curated", name)
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


@lru_cache(maxsize=1)
def glomerulus_families() -> dict[str, GlomerulusFamily]:
    rows = {}
    for row in read_curated_rows(GLOMERULUS_FAMILIES_FILE):
        entry = GlomerulusFamily(
            glomerulus=row["glomerulus"],
            family=row["family"],
            receptor=row["receptor"],
            verified=row["verified"].strip().lower() == "true",
            source=row["source"],
            doi=row["doi"],
            evidence=row["evidence"],
            note=row["note"],
        )
        if entry.glomerulus in rows:
            raise ValueError(f"duplicate glomerulus in {GLOMERULUS_FAMILIES_FILE}: {entry.glomerulus}")
        if entry.family not in FAMILY_ORDER:
            raise ValueError(f"unknown family {entry.family!r} for {entry.glomerulus} in {GLOMERULUS_FAMILIES_FILE}")
        rows[entry.glomerulus] = entry
    return rows


def receptor_families() -> tuple[str, ...]:
    """Families with at least one verified glomerulus, so every choice can seed a query."""
    present = {entry.family for entry in glomerulus_families().values() if entry.verified}
    return tuple(family for family in FAMILY_ORDER if family in present)


def family_glomeruli(family: str) -> set[str]:
    """Glomeruli whose family assignment is verified; unverified rows are excluded."""
    return {
        entry.glomerulus
        for entry in glomerulus_families().values()
        if entry.family == family and entry.verified
    }


@lru_cache(maxsize=1)
def transmitter_overrides() -> tuple[TransmitterOverride, ...]:
    return tuple(
        TransmitterOverride(
            dataset=row["dataset"],
            field=row["field"],
            value=row["value"],
            transmitter=row["transmitter"],
            source=row["source"],
            doi=row["doi"],
            note=row["note"],
        )
        for row in read_curated_rows(TRANSMITTER_OVERRIDES_FILE)
    )
