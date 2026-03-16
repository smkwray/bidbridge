from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from .paths import CONFIGS_DIR


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    return data or {}


@lru_cache(maxsize=None)
def load_study_config() -> dict[str, Any]:
    return load_yaml(CONFIGS_DIR / "study.yml")


@lru_cache(maxsize=None)
def load_sources_config() -> dict[str, Any]:
    return load_yaml(CONFIGS_DIR / "sources.yml")


def reset_config_cache() -> None:
    load_study_config.cache_clear()
    load_sources_config.cache_clear()
