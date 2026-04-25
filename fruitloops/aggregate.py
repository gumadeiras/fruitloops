from __future__ import annotations

from dataclasses import dataclass, field

from .filters import Filter, matches


@dataclass
class GroupState:
    count: int = 0
    sums: dict[str, float] = field(default_factory=dict)
    means: dict[str, float] = field(default_factory=dict)
    mean_counts: dict[str, int] = field(default_factory=dict)


def aggregate_rows(
    rows: list[dict[str, str]],
    by: list[str],
    sum_columns: list[str],
    mean_columns: list[str],
    exact: list[Filter] | None = None,
    contains: list[Filter] | None = None,
) -> list[dict[str, str]]:
    exact = exact or []
    contains = contains or []
    groups: dict[tuple[str, ...], GroupState] = {}
    for row in rows:
        if not matches(row, exact, contains):
            continue
        key = tuple(row.get(column, "") for column in by)
        state = groups.setdefault(key, GroupState())
        state.count += 1
        for column in sum_columns:
            state.sums[column] = state.sums.get(column, 0.0) + numeric(row.get(column, ""))
        for column in mean_columns:
            value = row.get(column, "")
            if value == "":
                continue
            state.means[column] = state.means.get(column, 0.0) + numeric(value)
            state.mean_counts[column] = state.mean_counts.get(column, 0) + 1

    out = []
    for key, state in groups.items():
        row = {column: value for column, value in zip(by, key)}
        row["count"] = str(state.count)
        for column in sum_columns:
            row[f"sum_{column}"] = format_number(state.sums.get(column, 0.0))
        for column in mean_columns:
            count = state.mean_counts.get(column, 0)
            row[f"mean_{column}"] = format_number(state.means.get(column, 0.0) / count) if count else ""
        out.append(row)
    return sorted(out, key=lambda row: tuple(row.get(column, "") for column in by))


def numeric(value: str) -> float:
    if value == "":
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


def format_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return f"{value:.12g}"
