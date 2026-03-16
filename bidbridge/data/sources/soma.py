"""NY Fed SOMA (System Open Market Account) holdings source.

Endpoint: https://markets.newyorkfed.org/api/soma/summary.json

Provides weekly aggregate SOMA holdings by security type (Bills, Notes/Bonds,
TIPS, FRN, MBS, Agencies). Used as a balance-sheet backdrop to separate Fed
effects from dealer behavior.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from ..sources.base import DownloadManifest, write_manifest

logger = logging.getLogger(__name__)

SUMMARY_URL = "https://markets.newyorkfed.org/api/soma/summary.json"


def fetch_soma_holdings(
    output_dir: Path,
    start_date: str = "2010-01-01",
) -> Path:
    """Fetch SOMA aggregate holdings from NY Fed API.

    Parameters
    ----------
    output_dir : Path
        Directory where the CSV and manifest will be written.
    start_date : str
        Earliest as-of date to include (YYYY-MM-DD).

    Returns
    -------
    Path
        Path to the written CSV file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching SOMA summary holdings...")
    resp = requests.get(SUMMARY_URL, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    records = data.get("soma", {}).get("summary", [])
    logger.info("Fetched %d SOMA summary records", len(records))

    start_dt = pd.Timestamp(start_date)

    def _safe_float(val):
        if val is None or val == "" or val == "*":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    rows = []
    for rec in records:
        as_of = rec.get("asOfDate", "")
        if not as_of or pd.Timestamp(as_of) < start_dt:
            continue

        bills = _safe_float(rec.get("bills"))
        notesbonds = _safe_float(rec.get("notesbonds"))
        tips = _safe_float(rec.get("tips"))
        frn = _safe_float(rec.get("frn"))
        tips_infl = _safe_float(rec.get("tipsInflationCompensation"))
        mbs = _safe_float(rec.get("mbs"))
        agencies = _safe_float(rec.get("agencies"))
        total = _safe_float(rec.get("total"))

        # Compute Treasury-only total
        tsy_total = sum(v for v in [bills, notesbonds, tips, frn] if v is not None) or None

        as_of_dt = pd.Timestamp(as_of)
        week_start = (as_of_dt - pd.Timedelta(days=as_of_dt.weekday())).normalize()
        week_end = week_start + pd.Timedelta(days=6)

        rows.append({
            "as_of_date": as_of,
            "week_start": week_start,
            "week_end": week_end,
            "soma_bills": bills,
            "soma_notes_bonds": notesbonds,
            "soma_tips": tips,
            "soma_frn": frn,
            "soma_tips_inflation_comp": tips_infl,
            "soma_treasury_total": tsy_total,
            "soma_mbs": mbs,
            "soma_agencies": agencies,
            "soma_total": total,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
        df["week_start"] = pd.to_datetime(df["week_start"])
        df["week_end"] = pd.to_datetime(df["week_end"])
        df = df.sort_values("as_of_date").reset_index(drop=True)

    csv_path = output_dir / "soma_holdings.csv"
    df.to_csv(csv_path, index=False)

    write_manifest(
        output_dir / "soma_holdings_manifest.json",
        DownloadManifest(
            source_id="nyfed_soma",
            source_url=SUMMARY_URL,
            retrieved_at_utc=datetime.now(timezone.utc).isoformat(),
            local_filename="soma_holdings.csv",
            parser_version="v1",
            content_type="text/csv",
            notes=f"{len(df)} weekly observations from {start_date}",
        ),
    )

    logger.info("Wrote %d rows to %s", len(df), csv_path)
    return csv_path
