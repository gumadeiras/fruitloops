"""Whole-brain neuron labels for path and reach queries.

FlyWire labels come from the pinned Schlegel et al. (2024) annotation table.
Hemibrain labels come from the compact traced-neuron export (type and instance),
the pinned per-body transmitter predictions, and classes per hemibrain type from
the FlyWire neurons that the same table matches to that type (``hemibrain_type``).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .curated import transmitter_overrides
from .duckdb_store import connect_read_only, table_exists
from .graph_cache import sorted_positions
from .olfaction_labels import infer_side

FLYWIRE_ANNOTATION_TABLE = "flywire_neuron_annotations"
FLYWIRE_ANNOTATION_COLUMNS = (
    "root_id",
    "cell_type",
    "side",
    "super_class",
    "cell_class",
    "cell_sub_class",
    "hemibrain_type",
    "top_nt",
)
HEMIBRAIN_LABEL_TABLE = "hemibrain_traced_neurons"
HEMIBRAIN_TRANSMITTER_TABLE = "hemibrain_body_neurotransmitters"
# Classifier classes in the column order of the pinned file; an exact tie goes to the earlier class.
HEMIBRAIN_TRANSMITTER_CLASSES = (
    "gaba",
    "acetylcholine",
    "glutamate",
    "serotonin",
    "octopamine",
    "dopamine",
    "neither",
)
TRANSMITTER_SIGNS = {"acetylcholine": 1, "gaba": -1, "glutamate": -1}
SENSORY_TYPE_PREFIXES = ("ORN_", "TRN_", "HRN_")
LABEL_FIELDS = (
    "type",
    "side",
    "super_class",
    "cell_class",
    "cell_sub_class",
    "hemibrain_type",
    "top_nt",
    "transmitter",
)


@dataclass
class NeuronLabels:
    dataset: str
    ids: np.ndarray
    fields: dict[str, np.ndarray]
    sign: np.ndarray

    def __len__(self) -> int:
        return len(self.ids)

    def field(self, name: str) -> np.ndarray:
        return self.fields[name]

    def rows_for_ids(self, ids: np.ndarray) -> np.ndarray:
        """Label row for each id, or -1 when the id has no label row."""
        return sorted_positions(self.ids, ids)

    def display_types(self) -> np.ndarray:
        """Type name, or a bracketed class label for untyped neurons."""
        return np.array(
            [
                cell_type or f"[{cell_class or super_class or 'untyped'}]"
                for cell_type, cell_class, super_class in zip(
                    self.fields["type"], self.fields["cell_class"], self.fields["super_class"]
                )
            ],
            dtype=object,
        )

    def kenyon_cells(self) -> np.ndarray:
        if self.dataset == "flywire":
            return self.fields["cell_class"] == "Kenyon_Cell"
        return np.array([cell_type.startswith("KC") for cell_type in self.fields["type"]], dtype=bool)

    def sensory_glomeruli(self) -> np.ndarray:
        """Glomerulus of FlyWire ORN_/TRN_/HRN_ sensory neurons; '' otherwise."""
        return np.array(
            [
                cell_type.split("_", 1)[1]
                if super_class == "sensory" and cell_type.startswith(SENSORY_TYPE_PREFIXES)
                else ""
                for cell_type, super_class in zip(self.fields["type"], self.fields["super_class"])
            ],
            dtype=object,
        )

    def sign_conflict_types(self) -> set[str]:
        """Cell types whose neurons have different transmitter signs."""
        signs: dict[str, set[int]] = {}
        for cell_type, sign in zip(self.fields["type"], self.sign.tolist()):
            if cell_type:
                signs.setdefault(cell_type, set()).add(sign)
        return {cell_type for cell_type, values in signs.items() if len(values) > 1}


def load_neuron_labels(store: Path, dataset: str) -> NeuronLabels:
    if not store.exists():
        raise SystemExit(f"missing DuckDB store {store}; run `fruitloops setup --{dataset}`")
    with connect_read_only(store, "neuron labels") as connection:
        if dataset == "flywire":
            return load_flywire_labels(connection)
        if dataset == "hemibrain":
            return load_hemibrain_labels(connection)
    raise ValueError(f"unsupported dataset: {dataset}")


def load_flywire_labels(connection) -> NeuronLabels:
    require_flywire_annotations(connection, "flywire")
    result = connection.execute(
        f"""
        SELECT CAST(root_id AS BIGINT) AS id,
               coalesce(cell_type, '') AS type,
               coalesce(side, '') AS side,
               coalesce(super_class, '') AS super_class,
               coalesce(cell_class, '') AS cell_class,
               coalesce(cell_sub_class, '') AS cell_sub_class,
               coalesce(hemibrain_type, '') AS hemibrain_type,
               coalesce(top_nt, '') AS top_nt
        FROM {FLYWIRE_ANNOTATION_TABLE}
        WHERE root_id IS NOT NULL
        ORDER BY root_id
        """
    ).fetchnumpy()
    fields = {name: np.asarray(result[name], dtype=object) for name in LABEL_FIELDS if name in result}
    fields["transmitter"] = apply_transmitter_overrides("flywire", fields)
    return NeuronLabels(
        dataset="flywire",
        ids=np.asarray(result["id"], dtype=np.int64),
        fields=fields,
        sign=transmitter_signs(fields["transmitter"]),
    )


def require_flywire_annotations(connection, dataset: str) -> None:
    """The FlyWire table gives FlyWire labels and, through ``hemibrain_type``, hemibrain classes."""
    use = "whole-brain annotations" if dataset == "flywire" else "annotations that give hemibrain classes"
    hint = f"run `fruitloops setup --{dataset}` to import the pinned FlyWire {use}"
    if not table_exists(connection, FLYWIRE_ANNOTATION_TABLE):
        raise SystemExit(f"missing {FLYWIRE_ANNOTATION_TABLE}; {hint}")
    columns = {row[0] for row in connection.execute(f"DESCRIBE {FLYWIRE_ANNOTATION_TABLE}").fetchall()}
    missing = [column for column in FLYWIRE_ANNOTATION_COLUMNS if column not in columns]
    if missing:
        raise SystemExit(f"{FLYWIRE_ANNOTATION_TABLE} lacks columns {', '.join(missing)}; {hint}")


def hemibrain_class_sql(field: str) -> str:
    """Per hemibrain type, the ``field`` value of more than half of the FlyWire neurons matched to it.

    A FlyWire neuron whose ``hemibrain_type`` lists several types counts for each of them. An empty
    value counts like any other, so a type whose matched neurons are mostly unclassified gets ''.
    Types without a value above half, for example a 50/50 split, get no row and stay empty.
    """
    return f"""
        SELECT type, {field}
        FROM hemibrain_votes
        GROUP BY type, {field}
        QUALIFY 2 * count(*) > sum(count(*)) OVER (PARTITION BY type)
    """


def load_hemibrain_labels(connection) -> NeuronLabels:
    if not table_exists(connection, HEMIBRAIN_LABEL_TABLE):
        raise SystemExit(f"missing {HEMIBRAIN_LABEL_TABLE}; run `fruitloops setup --hemibrain`")
    require_hemibrain_transmitters(connection)
    require_flywire_annotations(connection, "hemibrain")
    # top_nt = the class with the largest mean probability; bodies without a prediction get ''.
    largest = ", ".join(f"t.{name}" for name in HEMIBRAIN_TRANSMITTER_CLASSES)
    classes = " ".join(f"WHEN t.{name} THEN '{name}'" for name in HEMIBRAIN_TRANSMITTER_CLASSES)
    result = connection.execute(
        f"""
        WITH traced AS (
            SELECT CAST(bodyId AS BIGINT) AS id,
                   coalesce(min(type), '') AS type,
                   coalesce(min(instance), '') AS instance
            FROM {HEMIBRAIN_LABEL_TABLE}
            WHERE bodyId IS NOT NULL
            GROUP BY bodyId
        ),
        hemibrain_votes AS (
            SELECT trim(part) AS type, super_class, cell_class
            FROM (
                SELECT coalesce(super_class, '') AS super_class,
                       coalesce(cell_class, '') AS cell_class,
                       unnest(string_split(hemibrain_type, ',')) AS part
                FROM {FLYWIRE_ANNOTATION_TABLE}
                WHERE hemibrain_type IS NOT NULL
            )
            WHERE trim(part) <> ''
        ),
        super_classes AS ({hemibrain_class_sql("super_class")}),
        cell_classes AS ({hemibrain_class_sql("cell_class")})
        SELECT traced.id, traced.type, traced.instance,
               coalesce(CASE greatest({largest}) {classes} END, '') AS top_nt,
               coalesce(s.super_class, '') AS super_class,
               coalesce(c.cell_class, '') AS cell_class
        FROM traced
        LEFT JOIN {HEMIBRAIN_TRANSMITTER_TABLE} AS t ON CAST(t.body AS BIGINT) = traced.id
        LEFT JOIN super_classes AS s ON s.type = traced.type
        LEFT JOIN cell_classes AS c ON c.type = traced.type
        ORDER BY traced.id
        """
    ).fetchnumpy()
    count = len(result["id"])
    empty = np.full(count, "", dtype=object)
    sides = {"R": "right", "L": "left"}
    fields = {name: empty.copy() for name in LABEL_FIELDS}
    fields["type"] = np.asarray(result["type"], dtype=object)
    fields["side"] = np.asarray(
        [sides.get(infer_side(instance), "") for instance in result["instance"]],
        dtype=object,
    )
    for name in ("top_nt", "super_class", "cell_class"):
        fields[name] = np.asarray(result[name], dtype=object)
    fields["transmitter"] = apply_transmitter_overrides("hemibrain", fields)
    return NeuronLabels(
        dataset="hemibrain",
        ids=np.asarray(result["id"], dtype=np.int64),
        fields=fields,
        sign=transmitter_signs(fields["transmitter"]),
    )


def require_hemibrain_transmitters(connection) -> None:
    hint = "run `fruitloops setup --hemibrain` to import the pinned hemibrain transmitter predictions"
    if not table_exists(connection, HEMIBRAIN_TRANSMITTER_TABLE):
        raise SystemExit(f"missing {HEMIBRAIN_TRANSMITTER_TABLE}; {hint}")
    columns = {row[0] for row in connection.execute(f"DESCRIBE {HEMIBRAIN_TRANSMITTER_TABLE}").fetchall()}
    missing = [column for column in ("body", *HEMIBRAIN_TRANSMITTER_CLASSES) if column not in columns]
    if missing:
        raise SystemExit(f"{HEMIBRAIN_TRANSMITTER_TABLE} lacks columns {', '.join(missing)}; {hint}")


def apply_transmitter_overrides(dataset: str, fields: dict[str, np.ndarray]) -> np.ndarray:
    transmitter = fields["top_nt"].copy()
    for override in transmitter_overrides():
        if override.dataset != dataset:
            continue
        transmitter[fields[override.field] == override.value] = override.transmitter
    return transmitter


def transmitter_signs(transmitter: np.ndarray) -> np.ndarray:
    sign = np.zeros(len(transmitter), dtype=np.int8)
    for name, value in TRANSMITTER_SIGNS.items():
        sign[transmitter == name] = value
    return sign
