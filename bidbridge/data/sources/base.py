from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class DownloadManifest:
    source_id: str
    source_url: str
    retrieved_at_utc: str
    local_filename: str
    parser_version: str = "seed"
    content_type: str | None = None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "source_url": self.source_url,
            "retrieved_at_utc": self.retrieved_at_utc,
            "local_filename": self.local_filename,
            "parser_version": self.parser_version,
            "content_type": self.content_type,
            "notes": self.notes,
        }


def write_manifest(path: Path, manifest: DownloadManifest) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(manifest.to_dict(), handle, indent=2, sort_keys=True)
