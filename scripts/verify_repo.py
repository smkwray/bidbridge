from __future__ import annotations

from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bidbridge.paths import ROOT


REQUIRED = [
    ROOT / "README.md",
    ROOT / "AGENTS.md",
    ROOT / "docs" / "plan.md",
    ROOT / "docs" / "data_sources.md",
    ROOT / "docs" / "panel_spec.md",
    ROOT / "codex" / "tasks.json",
    ROOT / "scripts" / "codex_task_runner.py",
    ROOT / "configs" / "sources.yml",
    ROOT / "configs" / "study.yml",
]


def main() -> int:
    missing = [str(path.relative_to(ROOT)) for path in REQUIRED if not path.exists()]
    task_path = ROOT / "codex" / "tasks.json"
    tasks = json.loads(task_path.read_text(encoding="utf-8")) if task_path.exists() else []

    print(f"repo_root={ROOT}")
    print(f"task_count={len(tasks)}")
    if missing:
        print("missing_files:")
        for item in missing:
            print(f"  - {item}")
        return 1

    print("verification=ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
