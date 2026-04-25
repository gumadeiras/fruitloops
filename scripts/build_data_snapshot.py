#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import shutil
from pathlib import Path


CONNECTOME_REL = Path("figures/figure5/_analysis/connectome")


def main() -> int:
    parser = argparse.ArgumentParser(description="Copy generated connectome CSVs into fruitloops/data.")
    parser.add_argument("--source", type=Path, required=True, help="widespread-direction-selectivity root.")
    parser.add_argument("--dest", type=Path, required=True, help="fruitloops data directory.")
    args = parser.parse_args()

    source = args.source.expanduser().resolve()
    dest = args.dest.expanduser().resolve()
    connectome = source / CONNECTOME_REL
    if not connectome.exists():
        raise FileNotFoundError(connectome)

    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)

    copied: list[Path] = []
    copied += copy_tree_csv(connectome / "pan_LN_analysis", dest / "hemibrain" / "pan_ln_analysis")
    copied += copy_named_root_csvs(connectome, dest / "hemibrain" / "connectome_root")
    copied += copy_optional_tree(connectome / "LN_ORN_by_glomerulus", dest / "hemibrain" / "ln_orn_by_glomerulus")
    copied += copy_optional_tree(connectome / "LN_ORN_by_glomerulus_individual_LNs", dest / "hemibrain" / "ln_orn_by_glomerulus_individual_lns")
    copied += copy_optional_tree(connectome / "LN_to_PN_connections", dest / "hemibrain" / "ln_to_pn_connections")
    copied += copy_optional_tree(connectome / "LN_to_PN_connections_individual_LNs", dest / "hemibrain" / "ln_to_pn_connections_individual_lns")
    copied += copy_optional_tree(connectome / "PN_from_LN_connections", dest / "hemibrain" / "pn_from_ln_connections")
    copied += copy_optional_tree(connectome / "PN_from_LN_connections_individual_LNs", dest / "hemibrain" / "pn_from_ln_connections_individual_lns")

    flywire = connectome / "flywire_pan_LN_analysis"
    copied += copy_tree_csv(flywire, dest / "flywire" / "pan_ln_analysis", skip_parts={"comparison"})
    copied += copy_optional_tree(flywire / "comparison", dest / "comparison" / "flywire_vs_hemibrain")

    write_manifest(dest, copied)
    print(f"copied {len(copied)} csv files into {dest}")
    return 0


def copy_tree_csv(source: Path, dest: Path, skip_parts: set[str] | None = None) -> list[Path]:
    if not source.exists():
        return []
    copied = []
    skip_parts = skip_parts or set()
    for path in sorted(source.rglob("*.csv")):
        rel = path.relative_to(source)
        if any(part in skip_parts for part in rel.parts):
            continue
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        copied.append(target)
    return copied


def copy_optional_tree(source: Path, dest: Path) -> list[Path]:
    return copy_tree_csv(source, dest)


def copy_named_root_csvs(source: Path, dest: Path) -> list[Path]:
    copied = []
    for path in sorted(source.glob("*.csv")):
        target = dest / path.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        copied.append(target)
    return copied


def write_manifest(data_dir: Path, copied: list[Path]) -> None:
    rows = []
    for path in sorted(copied):
        rel = path.relative_to(data_dir)
        dataset = rel.parts[0]
        collection = rel.parts[1] if len(rel.parts) > 2 else "root"
        header, row_count = inspect_csv(path)
        rows.append(
            {
                "dataset": dataset,
                "collection": collection,
                "file_id": make_file_id(rel),
                "relative_path": rel.as_posix(),
                "rows": row_count,
                "columns": "|".join(header),
                "size_bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
        )

    manifest = data_dir / "manifest.csv"
    with manifest.open("w", newline="") as handle:
        fieldnames = [
            "dataset",
            "collection",
            "file_id",
            "relative_path",
            "rows",
            "columns",
            "size_bytes",
            "sha256",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def inspect_csv(path: Path) -> tuple[list[str], int]:
    with path.open(newline="", errors="replace") as handle:
        reader = csv.reader(handle)
        header = next(reader, [])
        row_count = sum(1 for _ in reader)
    return header, row_count


def make_file_id(rel: Path) -> str:
    stem = rel.with_suffix("").as_posix()
    safe = "".join(char if char.isalnum() else "_" for char in stem)
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_").lower()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
