#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path


FORMULA_URL = "https://github.com/gumadeiras/fruitloops/releases/download/v{version}/fruitloops-{version}.tar.gz"
DATA_INSTALL = '    (libexec/"share/fruitloops/data").install Dir["data/*"]'


def main() -> int:
    parser = argparse.ArgumentParser(description="Update the fruitloops Homebrew formula.")
    parser.add_argument("--formula", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--sha256", required=True)
    args = parser.parse_args()

    text = args.formula.read_text()
    updated = update_formula(text, args.version, args.sha256)
    if updated != text:
        args.formula.write_text(updated)
    return 0


def update_formula(text: str, version: str, sha256: str) -> str:
    text = replace_once(
        text,
        r'  url "https://github\.com/gumadeiras/fruitloops/releases/download/v[^"]+"',
        f'  url "{FORMULA_URL.format(version=version)}"',
        "url",
    )
    text = replace_once(text, r'  sha256 "[0-9a-f]+"', f'  sha256 "{sha256}"', "sha256")
    text = re.sub(r"\n  revision \d+\n", "\n", text, count=1)
    text = replace_once(
        text,
        r'assert_match "fruitloops [^"]+"',
        f'assert_match "fruitloops {version}"',
        "version test",
    )
    text = ensure_data_install(text)
    text = ensure_datasets_test(text)
    return text


def replace_once(text: str, pattern: str, replacement: str, label: str) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1)
    if count != 1:
        raise ValueError(f"could not update formula {label}")
    return updated


def ensure_data_install(text: str) -> str:
    if DATA_INSTALL in text:
        return text
    marker = "    virtualenv_install_with_resources\n"
    if marker not in text:
        raise ValueError("could not find virtualenv install line")
    return text.replace(marker, f"{marker}\n{DATA_INSTALL}\n", 1)


def ensure_datasets_test(text: str) -> str:
    test_line = '    assert_match "flywire", shell_output("#{bin}/fruitloops datasets")'
    if test_line in text:
        return text
    marker = '    assert_match "Usage:", shell_output("#{bin}/fruitloops-install-extras --help")\n'
    if marker not in text:
        raise ValueError("could not find formula extras test line")
    return text.replace(marker, f"{marker}{test_line}\n", 1)


if __name__ == "__main__":
    raise SystemExit(main())
