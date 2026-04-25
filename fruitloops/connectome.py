from __future__ import annotations

from .aggregate import aggregate_rows
from .data import FruitloopsData, TableInfo


def comparison_rows(data: FruitloopsData, name: str) -> list[dict[str, str]]:
    table = data.resolve("comparison:matched_ln_class_similarity")
    needle = name.lower()
    return [
        row
        for row in data.open_table(table)
        if needle in row.get("LN_class", "").lower()
        or needle in row.get("flywire_raw_LN_types", "").lower()
        or needle in row.get("flywire_primary_flywire_names", "").lower()
        or needle in row.get("flywire_hemibrain_types", "").lower()
    ]


def partner_rows(data: FruitloopsData, dataset: str, name: str, kind: str) -> tuple[list[dict[str, str]], list[str]]:
    if dataset == "flywire":
        return flywire_partner_rows(data, name, kind)
    return hemibrain_partner_rows(data, name, kind)


def flywire_partner_rows(data: FruitloopsData, name: str, kind: str) -> tuple[list[dict[str, str]], list[str]]:
    table_name = (
        "flywire:source_audit/orn_partner_counts_by_hemisphere"
        if kind == "orn"
        else "flywire:source_audit/pn_partner_counts_by_hemisphere"
    )
    table = data.resolve(table_name)
    rows = [
        row
        for row in data.open_table(table)
        if name.lower() in row.get("LN_type", "").lower()
        or name.lower() in row.get("LN_instance", "").lower()
        or name.lower() in row.get("source_root_id", "").lower()
    ]
    if kind == "orn":
        by = ["source_root_id", "LN_type", "analysis_hemisphere", "glomerulus", "input_relation"]
    else:
        by = ["source_root_id", "LN_type", "analysis_hemisphere", "glomerulus", "cell_sub_class"]
    out = aggregate_rows(rows, by, ["n_synapses"], [])
    return out, by + ["count", "sum_n_synapses"]


def hemibrain_partner_rows(data: FruitloopsData, name: str, kind: str) -> tuple[list[dict[str, str]], list[str]]:
    tables = hemibrain_partner_tables(data, name, kind)
    rows = []
    for table in tables:
        for row in data.open_table(table):
            row = dict(row)
            row["table"] = table.file_id
            row["relative_path"] = table.relative_path
            rows.append(row)
    if kind == "orn":
        by = ["LN_instance", "LN_bodyid", "glomerulus"]
        out = aggregate_rows(rows, by, ["n_inputs", "n_ipsi_inputs", "n_contra_inputs"], [])
        columns = by + ["count", "sum_n_inputs", "sum_n_ipsi_inputs", "sum_n_contra_inputs"]
    else:
        by = ["table", "glomerulus"]
        out = aggregate_rows(rows, by, ["weight", "LN_total_connections"], ["LN_to_PN_perct"])
        columns = by + ["count", "sum_weight", "sum_LN_total_connections", "mean_LN_to_PN_perct"]
    return out, columns


def hemibrain_partner_tables(data: FruitloopsData, name: str, kind: str) -> list[TableInfo]:
    collection = "ln_orn_by_glomerulus" if kind == "orn" else "ln_to_pn_connections"
    needle = name.lower()
    tables = [
        table
        for table in data.tables("hemibrain")
        if collection in table.collection and needle in table.relative_path.lower()
    ]
    return sorted(tables, key=lambda table: table.relative_path)
