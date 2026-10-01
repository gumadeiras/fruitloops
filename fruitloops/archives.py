"""Extract CSV members from bulk-download archives."""

from __future__ import annotations

import shutil
from pathlib import Path

def extract_archive_csvs(path: Path, output_dir: Path, force: bool = False) -> list[Path]:
    if path.suffix == ".zip":
        return extract_zip_csvs(path, output_dir, force)
    if path.suffixes[-2:] == [".tar", ".gz"] or path.suffix == ".tgz":
        return extract_tar_csvs(path, output_dir, force)
    raise ValueError(f"unsupported archive format: {path}")


def archive_stem(path: Path) -> str:
    if path.suffixes[-2:] == [".tar", ".gz"]:
        return path.name[: -len(".tar.gz")]
    if path.suffix == ".tgz":
        return path.name[: -len(".tgz")]
    return path.stem


def extract_zip_csvs(path: Path, output_dir: Path, force: bool = False) -> list[Path]:
    import zipfile

    output_dir.mkdir(parents=True, exist_ok=True)
    written = []
    with zipfile.ZipFile(path) as archive:
        for member in archive.namelist():
            if not member.endswith(".csv"):
                continue
            target = output_dir / Path(member).name
            if target.exists() and not force:
                written.append(target)
                continue
            with archive.open(member) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            written.append(target)
    return written


def extract_tar_csvs(path: Path, output_dir: Path, force: bool = False) -> list[Path]:
    import tarfile

    output_dir.mkdir(parents=True, exist_ok=True)
    written = []
    with tarfile.open(path) as archive:
        for member in archive.getmembers():
            if not member.isfile() or not member.name.endswith(".csv"):
                continue
            target = output_dir / Path(member.name).name
            if target.exists() and not force:
                written.append(target)
                continue
            source = archive.extractfile(member)
            if source is None:
                continue
            with source, target.open("wb") as dst:
                shutil.copyfileobj(source, dst)
            written.append(target)
    return written
