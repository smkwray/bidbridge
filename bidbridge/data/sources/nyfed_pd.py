"""NY Fed primary dealer statistics source.

API docs: https://markets.newyorkfed.org/static/docs/pd.html
Endpoint: https://markets.newyorkfed.org/api/pd/

Data is reported weekly (Wednesday as-of date, released Thursday ~4:15 PM ET).
Values are in millions of USD.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from ..sources.base import DownloadManifest, write_manifest

logger = logging.getLogger(__name__)

API_BASE = "https://markets.newyorkfed.org/api/pd"

# Key timeseries for Treasury positions and financing
POSITION_KEYS = [
    "PDPOSGST-TOT",    # Total U.S. Treasury (excl TIPS) net positions
    "PDPOSGS-B",        # Treasury Bills positions
    "PDPOSGSC-L2",      # Coupons <=2yr
    "PDPOSGSC-G2L3",    # Coupons 2-3yr
    "PDPOSGSC-G3L6",    # Coupons 3-6yr
    "PDPOSGSC-G6L7",    # Coupons 6-7yr
    "PDPOSGSC-G7L11",   # Coupons 7-11yr
    "PDPOSGSC-G11L21",  # Coupons 11-21yr
    "PDPOSGSC-G21",     # Coupons >21yr
    "PDPOSGS-BFRN",     # FRN positions
]

FINANCING_KEYS = [
    "PDSORA-UTSETTOT",   # Repo — U.S. Treasury
    "PDSIRRA-UTSETTOT",  # Reverse repo — U.S. Treasury
]

TIPS_KEYS = [
    "PDPOSTIPS-L2",      # TIPS <=2yr
    "PDPOSTIPS-G2",      # TIPS 2-6yr
    "PDPOSTIPS-G6L11",   # TIPS 6-11yr
    "PDPOSTIPS-G11",     # TIPS >11yr
]

# Current and recent series breaks
SERIES_BREAKS = [
    "SBN2024",  # 2024-07-03 to present
    "SBN2022",  # 2022-01-05 to 2024-07-02
    "SBN2015",  # 2015-01-01 to 2022-01-04
    "SBN2013",  # 2013-04-01 to 2014-12-31
    "SBP2013",  # 2001-07-01 to 2013-03-31
]


def _fetch_timeseries(series_break: str, keys: list[str]) -> list[dict]:
    """Fetch timeseries data for a given series break."""
    key_str = "_".join(keys)
    url = f"{API_BASE}/get/{series_break}/timeseries/{key_str}.json"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("pd", {}).get("timeseries", [])


def _fetch_all_breaks(keys: list[str], start_date: str) -> list[dict]:
    """Fetch across all relevant series breaks."""
    start_dt = pd.Timestamp(start_date)
    all_records = []
    for sb in SERIES_BREAKS:
        try:
            records = _fetch_timeseries(sb, keys)
            all_records.extend(records)
            logger.info("Fetched %d records from series break %s", len(records), sb)
        except requests.HTTPError as exc:
            logger.warning("Failed to fetch series break %s: %s", sb, exc)
            continue

    # Filter by start date and deduplicate
    filtered = []
    seen = set()
    for rec in all_records:
        date_str = rec.get("asofdate", "")
        key = rec.get("keyid", "")
        if not date_str or pd.Timestamp(date_str) < start_dt:
            continue
        dedup_key = (date_str, key)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        filtered.append(rec)

    return filtered


def fetch_primary_dealer_statistics(
    output_dir: Path,
    start_date: str = "2010-01-01",
) -> Path:
    """Fetch primary dealer position and financing data from NY Fed API.

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

    all_keys = POSITION_KEYS + FINANCING_KEYS + TIPS_KEYS
    records = _fetch_all_breaks(all_keys, start_date)
    logger.info("Total filtered records: %d", len(records))

    # Pivot: rows = asofdate, columns = keyid, values = value
    rows_by_date: dict[str, dict[str, float | None]] = {}
    for rec in records:
        date = rec["asofdate"]
        key = rec["keyid"]
        val_str = rec.get("value", "*")
        if val_str == "*" or val_str is None:
            val = None
        else:
            try:
                val = float(val_str)
            except (ValueError, TypeError):
                val = None

        if date not in rows_by_date:
            rows_by_date[date] = {}
        rows_by_date[date][key] = val

    output_rows = []
    for date_str in sorted(rows_by_date.keys()):
        vals = rows_by_date[date_str]
        as_of = pd.Timestamp(date_str)
        # Week start = Monday of the reporting week (as-of is Wednesday)
        week_start = (as_of - pd.Timedelta(days=as_of.weekday())).normalize()
        week_end = week_start + pd.Timedelta(days=6)

        total_treasury = vals.get("PDPOSGST-TOT")
        bills = vals.get("PDPOSGS-B")

        # Sum coupon buckets for total coupon positions
        coupon_keys = [k for k in POSITION_KEYS if k.startswith("PDPOSGSC-")]
        coupon_vals = [vals.get(k) for k in coupon_keys]
        coupon_total = sum(v for v in coupon_vals if v is not None) if any(
            v is not None for v in coupon_vals
        ) else None

        # Sum TIPS
        tips_vals = [vals.get(k) for k in TIPS_KEYS]
        tips_total = sum(v for v in tips_vals if v is not None) if any(
            v is not None for v in tips_vals
        ) else None

        frn = vals.get("PDPOSGS-BFRN")

        repo = vals.get("PDSORA-UTSETTOT")
        reverse_repo = vals.get("PDSIRRA-UTSETTOT")
        net_financing = None
        if repo is not None and reverse_repo is not None:
            net_financing = repo - reverse_repo

        output_rows.append({
            "as_of_date": date_str,
            "week_start": week_start,
            "week_end": week_end,
            "pd_treasury_inventory": total_treasury,
            "pd_bills_position": bills,
            "pd_coupon_position": coupon_total,
            "pd_tips_position": tips_total,
            "pd_frn_position": frn,
            "pd_repo_treasury": repo,
            "pd_reverse_repo_treasury": reverse_repo,
            "pd_financing_usage": net_financing,
        })

    _EXPECTED_COLUMNS = [
        "as_of_date", "week_start", "week_end",
        "pd_treasury_inventory", "pd_bills_position", "pd_coupon_position",
        "pd_tips_position", "pd_frn_position",
        "pd_repo_treasury", "pd_reverse_repo_treasury", "pd_financing_usage",
    ]

    df = pd.DataFrame(output_rows)

    if df.empty:
        df = pd.DataFrame(columns=_EXPECTED_COLUMNS)
    else:
        for col in ["as_of_date", "week_start", "week_end"]:
            df[col] = pd.to_datetime(df[col], errors="coerce")

        df = df.sort_values("as_of_date").reset_index(drop=True)

    # Forward-fill suppressed repo/reverse-repo values.
    # The NY Fed suppresses these with "*" for confidentiality when few dealers report.
    # Suppression is increasingly common from 2022 onward (50-87% of weeks by 2025).
    #
    # ASSUMPTION: LOCF (last observation carried forward) is used because the
    # underlying repo/reverse-repo volumes change slowly week-to-week. This is an
    # unverifiable assumption — the forward-filled values should be treated as
    # estimates, not observed data. The raw (unfilled) columns are preserved as
    # pd_repo_treasury_raw and pd_reverse_repo_treasury_raw for transparency.
    for col in ["pd_repo_treasury", "pd_reverse_repo_treasury"]:
        if col in df.columns:
            df[f"{col}_raw"] = df[col].copy()
            df[col] = df[col].ffill()

    # Recompute net financing after forward-fill
    if "pd_repo_treasury" in df.columns and "pd_reverse_repo_treasury" in df.columns:
        mask = df["pd_repo_treasury"].notna() & df["pd_reverse_repo_treasury"].notna()
        df.loc[mask, "pd_financing_usage"] = (
            df.loc[mask, "pd_repo_treasury"] - df.loc[mask, "pd_reverse_repo_treasury"]
        )

    csv_path = output_dir / "primary_dealer_stats.csv"
    df.to_csv(csv_path, index=False)

    write_manifest(
        output_dir / "primary_dealer_stats_manifest.json",
        DownloadManifest(
            source_id="nyfed_primary_dealer",
            source_url=API_BASE,
            retrieved_at_utc=datetime.now(timezone.utc).isoformat(),
            local_filename="primary_dealer_stats.csv",
            parser_version="v1",
            content_type="text/csv",
            notes=f"{len(df)} weekly observations from {start_date}",
        ),
    )

    logger.info("Wrote %d rows to %s", len(df), csv_path)
    return csv_path
