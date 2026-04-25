from __future__ import annotations


Filter = tuple[str, str]


def parse_filters(items: list[str]) -> list[Filter]:
    filters = []
    for item in items:
        if "=" not in item:
            raise ValueError(f"filter must be column=value: {item}")
        column, value = item.split("=", 1)
        filters.append((column, value))
    return filters


def matches(row: dict[str, str], exact: list[Filter], contains: list[Filter]) -> bool:
    for column, value in exact:
        if row.get(column, "") != value:
            return False
    for column, value in contains:
        if value.lower() not in row.get(column, "").lower():
            return False
    return True


def project(row: dict[str, str], columns: list[str]) -> dict[str, str]:
    return {column: row.get(column, "") for column in columns}


def split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]
