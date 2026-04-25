from __future__ import annotations

import os
from pathlib import Path


def load_env_file(path: Path | None = None) -> Path | None:
    env_path = path or Path(os.environ.get("FRUITLOOPS_ENV_FILE", ".env"))
    env_path = env_path.expanduser()
    if not env_path.exists():
        return None
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value
    return env_path


def env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value


def require_env(*names: str) -> str:
    for name in names:
        value = env(name)
        if value:
            return value
    joined = " or ".join(names)
    raise RuntimeError(f"missing required environment variable: {joined}")
