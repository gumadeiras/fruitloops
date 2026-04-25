from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Iterable

from .env import env, require_env
from .filters import Filter, split_csv


HEMIBRAIN_SERVER = "neuprint.janelia.org"
HEMIBRAIN_DATASET = "hemibrain:v1.2.1"
FLYWIRE_DATASTACK = "flywire_fafb_public"


@dataclass(frozen=True)
class HemibrainConfig:
    server: str
    dataset: str
    token: str


@dataclass(frozen=True)
class FlyWireConfig:
    datastack: str
    token: str
    materialization_version: int | None


def hemibrain_config() -> HemibrainConfig:
    return HemibrainConfig(
        server=env("NEUPRINT_SERVER", HEMIBRAIN_SERVER) or HEMIBRAIN_SERVER,
        dataset=env("NEUPRINT_DATASET", HEMIBRAIN_DATASET) or HEMIBRAIN_DATASET,
        token=require_env(
            "NEUPRINT_APPLICATION_CREDENTIALS",
            "NEUPRINT_AUTH_TOKEN",
            "NEUPRINT_TOKEN",
        ),
    )


def flywire_config() -> FlyWireConfig:
    version = env("CAVE_MATERIALIZATION_VERSION")
    return FlyWireConfig(
        datastack=env("FLYWIRE_DATASTACK", FLYWIRE_DATASTACK) or FLYWIRE_DATASTACK,
        token=require_env("CAVE_AUTH_TOKEN", "CAVE_TOKEN"),
        materialization_version=int(version) if version else None,
    )


def hemibrain_client():
    try:
        from neuprint import Client
    except ImportError as exc:
        raise SystemExit(
            "hemibrain live access requires neuprint-python. "
            "Install with `python -m pip install -e '.[live]'`."
        ) from exc
    config = hemibrain_config()
    return Client(config.server, dataset=config.dataset, token=config.token)


def flywire_client():
    try:
        from caveclient import CAVEclient
    except ImportError as exc:
        raise SystemExit(
            "FlyWire live access requires caveclient. "
            "Install with `python -m pip install -e '.[live]'`."
        ) from exc
    config = flywire_config()
    return CAVEclient(config.datastack, auth_token=config.token)


def hemibrain_fetch_neurons(
    body_ids: list[int],
    type_contains: str | None,
    instance_contains: str | None,
    limit: int,
) -> list[dict[str, str]]:
    client = hemibrain_client()
    clauses = ["n:Neuron"]
    where = []
    if body_ids:
        where.append(f"n.bodyId IN {json.dumps(body_ids)}")
    if type_contains:
        where.append(f"toLower(n.type) CONTAINS {cypher_string(type_contains.lower())}")
    if instance_contains:
        where.append(
            f"toLower(n.instance) CONTAINS {cypher_string(instance_contains.lower())}"
        )
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    query = f"""
    MATCH ({clauses[0]})
    {where_clause}
    RETURN n.bodyId AS bodyId, n.type AS type, n.instance AS instance,
           n.status AS status, n.cropped AS cropped, n.size AS size
    ORDER BY n.type, n.instance, n.bodyId
    LIMIT {int(limit)}
    """
    return dataframe_records(fetch_hemibrain_custom(client, query))


def hemibrain_fetch_connections(
    upstream_body_ids: list[int],
    downstream_body_ids: list[int],
    min_weight: int,
    limit: int,
) -> list[dict[str, str]]:
    client = hemibrain_client()
    where = [f"w.weight >= {int(min_weight)}"]
    if upstream_body_ids:
        where.append(f"up.bodyId IN {json.dumps(upstream_body_ids)}")
    if downstream_body_ids:
        where.append(f"down.bodyId IN {json.dumps(downstream_body_ids)}")
    query = f"""
    MATCH (up:Neuron)-[w:ConnectsTo]->(down:Neuron)
    WHERE {' AND '.join(where)}
    RETURN up.bodyId AS upstream_bodyId, up.type AS upstream_type,
           down.bodyId AS downstream_bodyId, down.type AS downstream_type,
           w.weight AS weight
    ORDER BY w.weight DESC
    LIMIT {int(limit)}
    """
    return dataframe_records(fetch_hemibrain_custom(client, query))


def hemibrain_custom(query: str, limit: int | None = None) -> list[dict[str, str]]:
    client = hemibrain_client()
    if limit is not None and " limit " not in f" {query.lower()} ":
        query = f"{query.rstrip()} LIMIT {int(limit)}"
    return dataframe_records(fetch_hemibrain_custom(client, query))


def fetch_hemibrain_custom(client, query: str):
    from neuprint import fetch_custom

    return fetch_custom(query, client=client)


def flywire_tables() -> list[dict[str, str]]:
    client = flywire_client()
    tables = client.materialize.get_tables()
    return [{"table": str(table)} for table in tables]


def flywire_table(
    table: str,
    exact_filters: list[Filter],
    in_filters: list[Filter],
    select_columns: list[str],
    limit: int,
    materialization_version: int | None = None,
) -> list[dict[str, str]]:
    client = flywire_client()
    config = flywire_config()
    kwargs = {
        "filter_equal_dict": {
            key: parse_scalar(value) for key, value in exact_filters
        }
        or None,
        "filter_in_dict": parse_in_filters(in_filters) or None,
        "limit": int(limit),
        "metadata": False,
        "log_warning": False,
    }
    if select_columns:
        kwargs["select_columns"] = select_columns
    version = materialization_version or config.materialization_version
    if version is not None:
        kwargs["materialization_version"] = int(version)
    frame = client.materialize.query_table(
        table,
        **{key: value for key, value in kwargs.items() if value is not None},
    )
    return dataframe_records(frame)


def flywire_synapses(
    pre_root_ids: list[int],
    post_root_ids: list[int],
    limit: int,
    materialization_version: int | None = None,
) -> list[dict[str, str]]:
    filters = []
    if pre_root_ids:
        filters.append(
            ("pre_pt_root_id", ",".join(str(value) for value in pre_root_ids))
        )
    if post_root_ids:
        filters.append(
            ("post_pt_root_id", ",".join(str(value) for value in post_root_ids))
        )
    return flywire_table(
        "synapses_nt_v1",
        exact_filters=[],
        in_filters=filters,
        select_columns=["id", "pre_pt_root_id", "post_pt_root_id"],
        limit=limit,
        materialization_version=materialization_version,
    )


def dataframe_records(frame) -> list[dict[str, str]]:
    if hasattr(frame, "to_dict"):
        records = frame.to_dict(orient="records")
    else:
        records = list(frame)
    return [
        {key: "" if value is None else str(value) for key, value in row.items()}
        for row in records
    ]


def parse_ints(values: Iterable[str]) -> list[int]:
    out = []
    for value in values:
        for item in split_csv(value):
            if not re.fullmatch(r"\d+", item):
                raise ValueError(f"expected integer id, got: {item}")
            out.append(int(item))
    return out


def parse_in_filters(filters: list[Filter]) -> dict[str, list[str | int]]:
    parsed: dict[str, list[str | int]] = {}
    for column, value in filters:
        parsed[column] = [parse_scalar(item) for item in split_csv(value)]
    return parsed


def parse_scalar(value: str) -> str | int:
    return int(value) if re.fullmatch(r"\d+", value) else value


def cypher_string(value: str) -> str:
    return json.dumps(value)
