from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bidbridge.paths import ROOT


REQUIRED = [
    ROOT / "README.md",
    ROOT / "AGENTS.md",
    ROOT / "docs" / "plan.md",
    ROOT / "docs" / "data_sources.md",
    ROOT / "docs" / "panel_spec.md",
    ROOT / "configs" / "sources.yml",
    ROOT / "configs" / "study.yml",
]


def main() -> int:
    missing = [str(path.relative_to(ROOT)) for path in REQUIRED if not path.exists()]

    print(f"repo_root={ROOT}")
    if missing:
        print("missing_files:")
        for item in missing:
            print(f"  - {item}")
        return 1

    print("verification=ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
