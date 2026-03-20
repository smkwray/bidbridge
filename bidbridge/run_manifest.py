from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bidbridge.config import load_study_config


def _git_sha(cwd: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            cwd=cwd,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def _stringify_paths(mapping: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in mapping.items():
        result[key] = _json_safe(value)
    return result


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            return str(value)
    return value


def write_run_manifest(
    output_path: str | Path,
    repo_root: Path,
    raw_inputs: dict[str, Path],
    processed_outputs: dict[str, Path],
    analysis_outputs: dict[str, Path],
    audit_outputs: dict[str, Path],
    extension_outputs: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Write the end-to-end run manifest for reproducible executions."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_sha": _git_sha(repo_root),
        "study_config": _json_safe(load_study_config()),
        "raw_inputs": _stringify_paths(raw_inputs),
        "processed_outputs": _stringify_paths(processed_outputs),
        "analysis_outputs": _stringify_paths(analysis_outputs),
        "audit_outputs": _stringify_paths(audit_outputs),
        "extension_outputs": _stringify_paths(extension_outputs),
        "metadata": _stringify_paths(metadata or {}),
    }

    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return output_path
