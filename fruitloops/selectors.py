"""Neuron selectors for whole-brain commands.

Each flag has one vocabulary:

- ``--type``: cell type names (FlyWire ``cell_type``, hemibrain ``type``), exact
  or shell-style wildcards such as ``'DNa*'``.
- ``--class``: FlyWire ``cell_class`` names such as ``ALPN`` or ``Kenyon_Cell``.
- ``--super-class``: FlyWire ``super_class`` names such as ``descending``.
- ``--id``: FlyWire root ids or hemibrain body ids.

Values inside one flag are combined with OR (repeat the flag or separate values
with commas). Different flags are combined with AND.
"""

from __future__ import annotations

import argparse
import difflib
import fnmatch
from dataclasses import dataclass

import numpy as np

from .neuron_labels import NeuronLabels

SELECTOR_KINDS = ("type", "class", "super-class", "id")
WILDCARDS = set("*?[")
OLF_CLASS_EQUIVALENTS = {
    "ORN": "--{prefix}class olfactory (or thermosensory, hygrosensory)",
    "PN": "--{prefix}class ALPN",
    "LN": "--{prefix}class ALLN",
    "LHN": "--{prefix}class LHLN or --{prefix}class LHCENT",
    "KC": "--{prefix}class Kenyon_Cell",
    "MBON": "--{prefix}class MBON",
    "APL": "--{prefix}type APL",
    "DAN": "--{prefix}class DAN",
}


@dataclass(frozen=True)
class Selection:
    rows: np.ndarray
    description: str


def add_selector_args(parser: argparse.ArgumentParser, prefix: str = "", role: str = "") -> None:
    group = parser.add_argument_group(f"{role or 'neuron'} selectors")
    label = f"{role} " if role else ""
    group.add_argument(
        f"--{prefix}type",
        dest=f"{prefix.replace('-', '_')}type",
        action="append",
        default=[],
        metavar="TYPE",
        help=f"{label}cell type: FlyWire cell_type or hemibrain type; exact or wildcard such as 'DNa*'.",
    )
    group.add_argument(
        f"--{prefix}class",
        dest=f"{prefix.replace('-', '_')}class",
        action="append",
        default=[],
        metavar="CLASS",
        help=f"{label}FlyWire cell_class, for example ALPN, Kenyon_Cell, MBON.",
    )
    group.add_argument(
        f"--{prefix}super-class",
        dest=f"{prefix.replace('-', '_')}super_class",
        action="append",
        default=[],
        metavar="SUPER_CLASS",
        help=f"{label}FlyWire super_class, for example descending, central, sensory.",
    )
    group.add_argument(
        f"--{prefix}id",
        dest=f"{prefix.replace('-', '_')}id",
        action="append",
        default=[],
        metavar="ID",
        help=f"{label}FlyWire root id or hemibrain body id.",
    )


def selector_values(args: argparse.Namespace, prefix: str = "") -> dict[str, list[str]]:
    base = prefix.replace("-", "_")
    return {
        kind: split_values(getattr(args, f"{base}{kind.replace('-', '_')}", []))
        for kind in SELECTOR_KINDS
    }


def split_values(values: list[str]) -> list[str]:
    return [item.strip() for value in values for item in value.split(",") if item.strip()]


def select_neurons(labels: NeuronLabels, values: dict[str, list[str]], prefix: str = "") -> Selection:
    flags = [f"--{prefix}{kind}" for kind in SELECTOR_KINDS]
    active = {kind: items for kind, items in values.items() if items}
    if not active:
        raise SystemExit(f"select neurons with {', '.join(flags)}")
    mask = np.ones(len(labels), dtype=bool)
    parts = []
    for kind, items in active.items():
        mask &= match_kind(labels, kind, items, prefix)
        parts.append(f"--{prefix}{kind} {','.join(items)}")
    description = " AND ".join(parts)
    if not mask.any():
        raise SystemExit(
            f"{description} selects no {labels.dataset} neurons; each flag matches on its own, "
            "but together they have no neurons in common"
        )
    return Selection(rows=np.flatnonzero(mask), description=description)


def match_kind(labels: NeuronLabels, kind: str, items: list[str], prefix: str) -> np.ndarray:
    flag = f"--{prefix}{kind}"
    if kind == "id":
        return match_ids(labels, items, flag)
    if kind == "type":
        return match_names(labels, labels.field("type"), items, flag, "cell type", prefix)
    if not labels.has_classes:
        raise SystemExit(
            f"{flag} is not available for {labels.dataset}: the compact export has no class annotations; "
            f"select types with --{prefix}type, for example --{prefix}type '*_*PN*' for antennal-lobe PN types"
        )
    field = "cell_class" if kind == "class" else "super_class"
    return match_names(labels, labels.field(field), items, flag, field, prefix, wildcards=False)


def match_ids(labels: NeuronLabels, items: list[str], flag: str) -> np.ndarray:
    try:
        ids = np.array([int(item) for item in items], dtype=np.int64)
    except ValueError as exc:
        raise SystemExit(f"{flag} expects integer ids: {', '.join(items)}") from exc
    rows = labels.rows_for_ids(ids)
    missing = [str(value) for value, row in zip(ids.tolist(), rows.tolist()) if row < 0]
    if missing:
        raise SystemExit(f"{flag}: unknown {labels.dataset} ids: {', '.join(missing)}")
    mask = np.zeros(len(labels), dtype=bool)
    mask[rows] = True
    return mask


def match_names(
    labels: NeuronLabels,
    column: np.ndarray,
    items: list[str],
    flag: str,
    vocabulary_name: str,
    prefix: str,
    *,
    wildcards: bool = True,
) -> np.ndarray:
    vocabulary = sorted({value for value in column.tolist() if value})
    known = set(vocabulary)
    selected = set()
    for item in items:
        if wildcards and WILDCARDS & set(item):
            matches = fnmatch.filter(vocabulary, item)
            if not matches:
                raise SystemExit(
                    f"{flag} '{item}' matches no {labels.dataset} {vocabulary_name} names"
                    f"{olf_hint(item, prefix)}"
                )
            selected.update(matches)
        elif item in known:
            selected.add(item)
        else:
            raise SystemExit(unknown_name_message(labels.dataset, flag, item, vocabulary, vocabulary_name, prefix))
    return np.isin(column, list(selected))


def unknown_name_message(
    dataset: str,
    flag: str,
    item: str,
    vocabulary: list[str],
    vocabulary_name: str,
    prefix: str,
) -> str:
    folded = [value for value in vocabulary if value.lower() == item.lower()]
    close = folded + [
        value for value in difflib.get_close_matches(item, vocabulary, n=5, cutoff=0.6) if value not in folded
    ]
    message = f"{flag} '{item}' is not a {dataset} {vocabulary_name}"
    hint = olf_hint(item, prefix)
    if hint:
        return message + hint
    if close:
        return f"{message}; close matches: {', '.join(close[:5])}"
    return f"{message}; no close matches (names are case-sensitive)"


def olf_hint(item: str, prefix: str) -> str:
    equivalent = OLF_CLASS_EQUIVALENTS.get(item.upper())
    if not equivalent:
        return ""
    return (
        f"; {item} is an `olf` class name. The whole-brain equivalent is "
        f"{equivalent.format(prefix=prefix)}"
    )
