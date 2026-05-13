from __future__ import annotations

import argparse


FORMATS = ("table", "csv", "json", "jsonl")


def add_format_arg(parser: argparse.ArgumentParser, default: str = "table") -> None:
    parser.add_argument("--format", choices=FORMATS, default=default)
    parser.add_argument("--csv", dest="format", action="store_const", const="csv", help="Alias for --format csv.")
    parser.add_argument("--json", dest="format", action="store_const", const="json", help="Alias for --format json.")
    parser.add_argument("--jsonl", dest="format", action="store_const", const="jsonl", help="Alias for --format jsonl.")


def add_dataset_arg(
    parser: argparse.ArgumentParser,
    choices: tuple[str, ...],
    *,
    required: bool = False,
) -> None:
    parser.add_argument("--dataset", choices=choices)
    for dataset in choices:
        parser.add_argument(
            f"--{dataset}",
            dest="dataset",
            action="store_const",
            const=dataset,
            help=f"Alias for --dataset {dataset}.",
        )
    if required:
        parser.set_defaults(_required_dataset=True)


def add_dataset_filter_arg(parser: argparse.ArgumentParser, choices: tuple[str, ...]) -> None:
    parser.add_argument("--dataset", choices=choices, action="append")
    for dataset in choices:
        parser.add_argument(
            f"--{dataset}",
            dest="dataset",
            action="append_const",
            const=dataset,
            help=f"Alias for --dataset {dataset}.",
        )


def add_partner_kind_arg(parser: argparse.ArgumentParser, *, required: bool = False) -> None:
    parser.add_argument("--kind", choices=("orn", "pn"))
    parser.add_argument("--orn", dest="kind", action="store_const", const="orn", help="Alias for --kind orn.")
    parser.add_argument("--pn", dest="kind", action="store_const", const="pn", help="Alias for --kind pn.")
    if required:
        parser.set_defaults(_required_kind=True)


def require_dataset(args: argparse.Namespace) -> str:
    dataset = getattr(args, "dataset", None)
    if not dataset:
        raise SystemExit("requires --dataset, --hemibrain, or --flywire")
    return dataset


def require_partner_kind(args: argparse.Namespace) -> str:
    kind = getattr(args, "kind", None)
    if not kind:
        raise SystemExit("requires --kind, --orn, or --pn")
    return kind


def unique_values(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    return list(dict.fromkeys(values))
