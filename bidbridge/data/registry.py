from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from bidbridge.config import load_sources_config


@dataclass(frozen=True)
class SourceRecord:
    source_id: str
    label: str
    priority: int | None
    frequency: str
    grain: str
    page_url: str
    retrieval_status: str
    notes: str

    @property
    def is_priority(self) -> bool:
        return self.priority is not None and self.priority <= 2


def get_source_registry() -> list[SourceRecord]:
    raw_sources = load_sources_config().get("sources", {})
    records: list[SourceRecord] = []
    for source_id, payload in raw_sources.items():
        records.append(
            SourceRecord(
                source_id=source_id,
                label=str(payload.get("label", source_id)),
                priority=payload.get("priority"),
                frequency=str(payload.get("frequency", "unknown")),
                grain=str(payload.get("grain", "unknown")),
                page_url=str(payload.get("page_url", "")),
                retrieval_status=str(payload.get("retrieval_status", "unknown")),
                notes=str(payload.get("notes", "")).strip(),
            )
        )
    return sorted(records, key=lambda item: (item.priority or 999, item.source_id))


def iter_priority_sources() -> Iterable[SourceRecord]:
    for record in get_source_registry():
        if record.is_priority:
            yield record
