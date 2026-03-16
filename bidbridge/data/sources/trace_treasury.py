"""FINRA TRACE Treasury aggregates source stub.

Official page:
https://www.finra.org/filing-reporting/trace/data/trace-treasury-aggregates/about

Production target:
- optionally ingest daily or monthly Treasury aggregate volume,
- align it to auction-week windows,
- keep it modular because it is not required for the first bridge result.
"""

from __future__ import annotations

from pathlib import Path


def fetch_trace_treasury(output_dir: Path) -> Path:
    raise NotImplementedError(
        "Codex milestone M1 may optionally implement TRACE Treasury aggregate ingestion."
    )
