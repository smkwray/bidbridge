from __future__ import annotations

from pathlib import Path


def find_repo_root(start: Path | None = None) -> Path:
    candidate = (start or Path(__file__).resolve()).resolve()
    for path in [candidate, *candidate.parents]:
        if (path / "pyproject.toml").exists() and (path / "configs").exists():
            return path
    return Path(__file__).resolve().parents[1]


ROOT = find_repo_root()
CONFIGS_DIR = ROOT / "configs"
DOCS_DIR = ROOT / "docs"
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"
EXTERNAL_DIR = DATA_DIR / "external"
OUTPUTS_DIR = ROOT / "outputs"
FIGURES_DIR = OUTPUTS_DIR / "figures"
TABLES_DIR = OUTPUTS_DIR / "tables"


def ensure_project_directories() -> list[Path]:
    directories = [
        DATA_DIR,
        RAW_DIR,
        INTERIM_DIR,
        PROCESSED_DIR,
        EXTERNAL_DIR,
        OUTPUTS_DIR,
        FIGURES_DIR,
        TABLES_DIR,
    ]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    return directories
