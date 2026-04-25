from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from .aggregate import numeric


DEFAULT_FORMATS = ("png",)
DEFAULT_PALETTE = (
    "#4C78A8",
    "#F58518",
    "#54A24B",
    "#E45756",
    "#72B7B2",
    "#B279A2",
    "#FF9DA6",
    "#9D755D",
    "#BAB0AC",
    "#79706E",
)


@dataclass(frozen=True)
class PlotSpec:
    kind: str
    x: str | None = None
    y: str | None = None
    color: str | None = None
    size: str | None = None
    label: str | None = None
    value: str | None = None
    title: str | None = None
    xlabel: str | None = None
    ylabel: str | None = None
    output: Path = Path("figure")
    formats: tuple[str, ...] = DEFAULT_FORMATS
    limit: int | None = None
    top_labels: int = 0
    log_x: bool = False
    log_y: bool = False
    width: float = 7.0
    height: float = 5.0
    dpi: int = 300


def render_plot(rows: list[dict[str, str]], spec: PlotSpec) -> list[Path]:
    plt = import_matplotlib()
    rows = rows[: spec.limit] if spec.limit else rows
    fig, ax = plt.subplots(figsize=(spec.width, spec.height), dpi=spec.dpi)
    apply_style(ax)

    if spec.kind == "scatter":
        plot_scatter(ax, rows, spec)
    elif spec.kind == "line":
        plot_line(ax, rows, spec)
    elif spec.kind == "bar":
        plot_bar(ax, rows, spec)
    elif spec.kind == "hist":
        plot_hist(ax, rows, spec)
    elif spec.kind == "violin":
        plot_violin(ax, rows, spec)
    elif spec.kind == "bubble":
        plot_bubble(ax, rows, spec)
    elif spec.kind == "heatmap":
        plot_heatmap(ax, rows, spec)
    else:
        raise ValueError(f"unsupported plot kind: {spec.kind}")

    if spec.title:
        ax.set_title(spec.title)
    if spec.xlabel:
        ax.set_xlabel(spec.xlabel)
    elif spec.x:
        ax.set_xlabel(spec.x)
    if spec.ylabel:
        ax.set_ylabel(spec.ylabel)
    elif spec.y:
        ax.set_ylabel(spec.y)

    if spec.log_x:
        ax.set_xscale("log")
    if spec.log_y:
        ax.set_yscale("log")

    fig.tight_layout()
    paths = save_figure(fig, spec.output, spec.formats)
    plt.close(fig)
    return paths


def import_matplotlib():
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise SystemExit(
            "plotting requires matplotlib. Install with `python -m pip install -e '.[plot]'`."
        ) from exc
    return plt


def apply_style(ax) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, color="0.9", linewidth=0.6)


def plot_scatter(ax, rows: list[dict[str, str]], spec: PlotSpec) -> None:
    require_columns(spec, "x", "y")
    groups = grouped_rows(rows, spec.color)
    for index, (group, group_rows) in enumerate(groups.items()):
        ax.scatter(
            values(group_rows, spec.x),
            values(group_rows, spec.y),
            s=42,
            alpha=0.82,
            color=DEFAULT_PALETTE[index % len(DEFAULT_PALETTE)],
            edgecolors="white",
            linewidths=0.4,
            label=group,
        )
    add_legend(ax, groups, spec.color)
    annotate_points(ax, rows, spec)


def plot_line(ax, rows: list[dict[str, str]], spec: PlotSpec) -> None:
    require_columns(spec, "x", "y")
    groups = grouped_rows(rows, spec.color)
    for index, (group, group_rows) in enumerate(groups.items()):
        ordered = sorted(group_rows, key=lambda row: numeric(row.get(spec.x or "", "")))
        ax.plot(
            values(ordered, spec.x),
            values(ordered, spec.y),
            marker="o",
            linewidth=1.4,
            markersize=3.5,
            color=DEFAULT_PALETTE[index % len(DEFAULT_PALETTE)],
            label=group,
        )
    add_legend(ax, groups, spec.color)


def plot_bar(ax, rows: list[dict[str, str]], spec: PlotSpec) -> None:
    require_columns(spec, "x", "y")
    labels = [row.get(spec.x or "", "") for row in rows]
    ax.bar(range(len(rows)), values(rows, spec.y), color=DEFAULT_PALETTE[0], alpha=0.9)
    ax.set_xticks(range(len(rows)))
    ax.set_xticklabels(labels, rotation=90, ha="center", va="top")


def plot_hist(ax, rows: list[dict[str, str]], spec: PlotSpec) -> None:
    column = spec.value or spec.x
    if not column:
        raise ValueError("hist requires --value or --x")
    bins = min(40, max(8, int(math.sqrt(max(len(rows), 1)))))
    ax.hist(values(rows, column), bins=bins, color=DEFAULT_PALETTE[0])
    ax.set_xlabel(spec.xlabel or column)
    ax.set_ylabel(spec.ylabel or "count")


def plot_violin(ax, rows: list[dict[str, str]], spec: PlotSpec) -> None:
    value_column = spec.value or spec.y
    group_column = spec.x or spec.color
    if not value_column:
        raise ValueError("violin requires --value or --y")
    if not group_column:
        ax.violinplot([values(rows, value_column)], showmeans=False, showmedians=True)
        return
    groups = grouped_rows(rows, group_column)
    data = [values(group_rows, value_column) for group_rows in groups.values()]
    ax.violinplot(data, showmeans=False, showmedians=True)
    ax.set_xticks(range(1, len(groups) + 1))
    ax.set_xticklabels(list(groups.keys()), rotation=90, ha="center", va="top")


def plot_bubble(ax, rows: list[dict[str, str]], spec: PlotSpec) -> None:
    require_columns(spec, "x", "y", "size")
    size_values = values(rows, spec.size)
    sizes = scale_sizes(size_values, 20, 240)
    kwargs = (
        {"c": values(rows, spec.color), "cmap": "coolwarm"}
        if spec.color
        else {"color": DEFAULT_PALETTE[0]}
    )
    scatter = ax.scatter(
        values(rows, spec.x),
        values(rows, spec.y),
        s=sizes,
        alpha=0.82,
        **kwargs,
    )
    scatter.set_edgecolor("0.25")
    scatter.set_linewidth(0.2)
    if spec.color:
        cbar = ax.figure.colorbar(scatter, ax=ax, pad=0.01)
        cbar.set_label(spec.color)
    annotate_points(ax, rows, spec)


def plot_heatmap(ax, rows: list[dict[str, str]], spec: PlotSpec) -> None:
    require_columns(spec, "x", "y")
    value_column = spec.value or spec.color
    if not value_column:
        raise ValueError("heatmap requires --value or --color")
    x_order = sorted({row.get(spec.x or "", "") for row in rows})
    y_order = sorted({row.get(spec.y or "", "") for row in rows})
    x_lookup = {value: index for index, value in enumerate(x_order)}
    y_lookup = {value: index for index, value in enumerate(y_order)}
    matrix = [[math.nan for _ in x_order] for _ in y_order]
    for row in rows:
        matrix[y_lookup[row.get(spec.y or "", "")]][x_lookup[row.get(spec.x or "", "")]] = numeric(
            row.get(value_column, "")
        )
    image = ax.imshow(matrix, aspect="auto", cmap="viridis")
    ax.set_xticks(range(len(x_order)))
    ax.set_xticklabels(x_order, rotation=90, ha="center", va="top")
    ax.set_yticks(range(len(y_order)))
    ax.set_yticklabels(y_order)
    cbar = ax.figure.colorbar(image, ax=ax, pad=0.01)
    cbar.set_label(value_column)


def require_columns(spec: PlotSpec, *fields: str) -> None:
    missing = [field for field in fields if getattr(spec, field) is None]
    if missing:
        args = ", ".join(f"--{field}" for field in missing)
        raise ValueError(f"{spec.kind} requires {args}")


def grouped_rows(
    rows: list[dict[str, str]],
    column: str | None,
) -> dict[str, list[dict[str, str]]]:
    if not column:
        return {"data": rows}
    groups: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        groups.setdefault(row.get(column, ""), []).append(row)
    return groups


def values(rows: Sequence[dict[str, str]], column: str | None) -> list[float]:
    if column is None:
        return []
    return [numeric(row.get(column, "")) for row in rows]


def scale_sizes(values_: list[float], min_size: float, max_size: float) -> list[float]:
    if not values_:
        return []
    lo = min(values_)
    hi = max(values_)
    if hi == lo:
        return [(min_size + max_size) / 2] * len(values_)
    return [
        min_size + (value - lo) / (hi - lo) * (max_size - min_size)
        for value in values_
    ]


def annotate_points(ax, rows: list[dict[str, str]], spec: PlotSpec) -> None:
    if not spec.label or not spec.x or not spec.y or spec.top_labels <= 0:
        return
    labeled = rows[: spec.top_labels]
    for row in labeled:
        ax.annotate(
            row.get(spec.label, ""),
            (numeric(row.get(spec.x, "")), numeric(row.get(spec.y, ""))),
            xytext=(3, 3),
            textcoords="offset points",
            fontsize=7,
        )


def add_legend(
    ax,
    groups: dict[str, list[dict[str, str]]],
    color_column: str | None,
) -> None:
    if color_column and len(groups) > 1:
        ax.legend(title=color_column, frameon=False, fontsize=8, title_fontsize=8)


def save_figure(fig, output: Path, formats: Sequence[str]) -> list[Path]:
    output.parent.mkdir(parents=True, exist_ok=True)
    paths = []
    for fmt in formats:
        path = output.with_suffix(f".{fmt}")
        fig.savefig(path, bbox_inches="tight")
        paths.append(path)
    return paths
