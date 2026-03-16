"""Federal Reserve H.8 — bank Treasury/agency securities holdings.

Source: Federal Reserve Statistical Release H.8, "Assets and Liabilities
of Commercial Banks in the United States" (weekly).

We pull data via the FRED CSV endpoint which mirrors the official H.8
series published by the Board of Governors.  Key series:

    TASACBW027SBOG  — Treasury & agency securities, all commercial banks (SA)
    TASACBW027NBOG  — same, not seasonally adjusted
    TMBACBW027SBOG  — Treasury & agency: MBS component (SA)
    TNMACBW027SBOG  — Treasury & agency: non-MBS component (SA)
    TMBACBW027NBOG  — MBS component (NSA)
    TNMACBW027NBOG  — non-MBS component (NSA)

All values are in billions of USD.  We convert to millions to match the
rest of the BidBridge panel convention.

Official H.8 pages:
    https://www.federalreserve.gov/releases/h8/current/default.htm
    https://www.federalreserve.gov/datadownload/Build.aspx?rel=H8
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from ..sources.base import DownloadManifest, write_manifest

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FRED CSV download endpoint — public, no API key required
# ---------------------------------------------------------------------------
_FRED_CSV_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# Series we fetch (all weekly, ending Wednesday)
_SERIES = {
    "TASACBW027SBOG": "bank_treasury_securities_sa",
    "TASACBW027NBOG": "bank_treasury_securities_nsa",
    "TMBACBW027SBOG": "bank_treasury_mbs_sa",
    "TNMACBW027SBOG": "bank_treasury_non_mbs_sa",
    "TMBACBW027NBOG": "bank_treasury_mbs_nsa",
    "TNMACBW027NBOG": "bank_treasury_non_mbs_nsa",
}

# The primary series used as the canonical "bank_treasury_securities" column
_PRIMARY_SA = "TASACBW027SBOG"
_PRIMARY_NSA = "TASACBW027NBOG"

_BILLIONS_TO_MILLIONS = 1_000.0


def _build_fred_url(series_ids: list[str], start_date: str, end_date: str) -> str:
    """Construct a FRED multi-series CSV download URL.

    The FRED CSV endpoint accepts comma-separated series IDs with
    per-series ``cosd`` (start) and ``coed`` (end) date parameters.
    """
    ids = ",".join(series_ids)
    cosd = ",".join([start_date] * len(series_ids))
    coed = ",".join([end_date] * len(series_ids))
    return f"{_FRED_CSV_BASE}?id={ids}&cosd={cosd}&coed={coed}"


def _fetch_fred_csv(
    series_ids: list[str],
    start_date: str,
    end_date: str,
    timeout: int = 90,
) -> pd.DataFrame:
    """Download one or more FRED series as a single CSV and return a DataFrame."""
    url = _build_fred_url(series_ids, start_date, end_date)
    logger.info("Fetching H.8 data from FRED: %s", url)
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()

    df = pd.read_csv(
        io.StringIO(resp.text),
        parse_dates=["observation_date"],
        na_values=[".", ""],
    )
    return df


def fetch_h8(
    output_dir: Path,
    start_date: str = "2010-01-01",
) -> Path:
    """Fetch weekly H.8 bank Treasury/agency securities holdings.

    Parameters
    ----------
    output_dir : Path
        Directory where ``h8_bank_securities.csv`` and its manifest
        will be written.
    start_date : str
        Earliest observation date to include (YYYY-MM-DD).

    Returns
    -------
    Path
        Path to the written CSV file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use today as the end date so we always get the latest release.
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    series_ids = list(_SERIES.keys())
    raw = _fetch_fred_csv(series_ids, start_date, end_date)

    if raw.empty:
        logger.warning("FRED returned an empty DataFrame for H.8 series")

    # Rename columns: FRED series codes -> friendly names
    rename_map = {"observation_date": "as_of_date"}
    rename_map.update(_SERIES)
    raw = raw.rename(columns=rename_map)

    # Convert from billions to millions (BidBridge panel convention)
    value_cols = list(_SERIES.values())
    for col in value_cols:
        if col in raw.columns:
            raw[col] = raw[col] * _BILLIONS_TO_MILLIONS

    # The primary column expected by downstream consumers
    # (not-seasonally-adjusted, matching the raw H.8 report)
    primary_col = _SERIES[_PRIMARY_NSA]
    if primary_col in raw.columns:
        raw["bank_treasury_securities"] = raw[primary_col]
    else:
        raw["bank_treasury_securities"] = None

    # Compute week boundaries using the project's Monday-start convention.
    # H.8 observations are dated to Wednesday.  week_start = Monday of the
    # same week (as_of_date minus weekday offset); week_end = week_start + 6.
    raw["as_of_date"] = pd.to_datetime(raw["as_of_date"], errors="coerce")
    raw = raw.dropna(subset=["as_of_date"])

    raw["week_start"] = raw["as_of_date"] - raw["as_of_date"].dt.weekday * pd.Timedelta(days=1)
    raw["week_end"] = raw["week_start"] + pd.Timedelta(days=6)

    # Sort and reorder columns for clarity
    raw = raw.sort_values("as_of_date").reset_index(drop=True)

    leading_cols = [
        "as_of_date",
        "week_start",
        "week_end",
        "bank_treasury_securities",
    ]
    remaining = [c for c in raw.columns if c not in leading_cols]
    raw = raw[leading_cols + sorted(remaining)]

    # ---- Persist --------------------------------------------------------
    csv_path = output_dir / "h8_bank_securities.csv"
    raw.to_csv(csv_path, index=False)
    logger.info("Wrote %d rows to %s", len(raw), csv_path)

    source_url = _build_fred_url(series_ids, start_date, end_date)
    write_manifest(
        output_dir / "h8_bank_securities_manifest.json",
        DownloadManifest(
            source_id="fed_h8",
            source_url=source_url,
            retrieved_at_utc=datetime.now(timezone.utc).isoformat(),
            local_filename="h8_bank_securities.csv",
            parser_version="v1",
            content_type="text/csv",
            notes=(
                f"{len(raw)} weekly observations from {start_date}; "
                f"values in millions USD; "
                f"series: {', '.join(series_ids)}"
            ),
        ),
    )

    return csv_path
