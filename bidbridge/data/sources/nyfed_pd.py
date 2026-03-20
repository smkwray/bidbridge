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
from ...features.auction_week import normalize_week_definition, week_end, week_start

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
    "PDPOSGSC-G11",     # Coupons >11yr (combined, pre-2022 series breaks)
    "PDPOSGSC-G11L21",  # Coupons 11-21yr (split, SBN2022+ only)
    "PDPOSGSC-G21",     # Coupons >21yr (split, SBN2022+ only)
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


def assign_reporting_weeks(
    as_of_dates: pd.Series,
    week_definition: str = "monday",
) -> pd.DataFrame:
    """Assign reporting weeks from NY Fed as-of dates under a given anchor."""
    starts = week_start(as_of_dates, week_definition)
    return pd.DataFrame({
        "week_start": starts,
        "week_end": week_end(starts),
    })


def finalize_primary_dealer_dataframe(
    df: pd.DataFrame,
    week_definition: str = "monday",
) -> pd.DataFrame:
    """Normalize dates and apply the documented financing forward-fill."""
    frame = df.copy()
    if frame.empty:
        return frame

    for col in ["as_of_date", "week_start", "week_end"]:
        if col in frame.columns:
            frame[col] = pd.to_datetime(frame[col], errors="coerce")

    if "as_of_date" in frame.columns:
        weeks = assign_reporting_weeks(frame["as_of_date"], week_definition=week_definition)
        frame["week_start"] = weeks["week_start"]
        frame["week_end"] = weeks["week_end"]

    frame = frame.sort_values("as_of_date").reset_index(drop=True)

    for col in ["pd_repo_treasury", "pd_reverse_repo_treasury"]:
        if col in frame.columns:
            frame[f"{col}_raw"] = frame[col].copy()
            frame[col] = frame[col].ffill()

    if "pd_repo_treasury" in frame.columns and "pd_reverse_repo_treasury" in frame.columns:
        mask = frame["pd_repo_treasury"].notna() & frame["pd_reverse_repo_treasury"].notna()
        frame.loc[mask, "pd_financing_usage"] = (
            frame.loc[mask, "pd_repo_treasury"] - frame.loc[mask, "pd_reverse_repo_treasury"]
        )

    frame.attrs["week_definition"] = normalize_week_definition(week_definition)
    return frame


def fetch_primary_dealer_statistics(
    output_dir: Path,
    start_date: str = "2010-01-01",
    week_definition: str = "monday",
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
        week_info = assign_reporting_weeks(pd.Series([as_of]), week_definition=week_definition)
        week_start_value = week_info.loc[0, "week_start"]
        week_end_value = week_info.loc[0, "week_end"]

        total_treasury = vals.get("PDPOSGST-TOT")
        bills = vals.get("PDPOSGS-B")

        # Sum coupon buckets for total coupon positions.
        # This aggregate tolerates missing bands (pre-2022 series breaks
        # lack G11L21 and G21 but have G11 combined).  The strict
        # all-or-nothing rule is applied per-bucket in panel_fe.py where
        # partial data would create bias.
        coupon_keys = [k for k in POSITION_KEYS if k.startswith("PDPOSGSC-")]
        coupon_vals = [vals.get(k) for k in coupon_keys]
        coupon_total = (
            sum(v for v in coupon_vals if v is not None)
            if any(v is not None for v in coupon_vals)
            else None
        )

        # Sum TIPS (same tolerant rule for the aggregate)
        tips_vals = [vals.get(k) for k in TIPS_KEYS]
        tips_total = (
            sum(v for v in tips_vals if v is not None)
            if any(v is not None for v in tips_vals)
            else None
        )

        frn = vals.get("PDPOSGS-BFRN")

        repo = vals.get("PDSORA-UTSETTOT")
        reverse_repo = vals.get("PDSIRRA-UTSETTOT")
        net_financing = None
        if repo is not None and reverse_repo is not None:
            net_financing = repo - reverse_repo

        # Individual coupon maturity bands (by remaining maturity)
        coupon_le2y = vals.get("PDPOSGSC-L2")
        coupon_2_3y = vals.get("PDPOSGSC-G2L3")
        coupon_3_6y = vals.get("PDPOSGSC-G3L6")
        coupon_6_7y = vals.get("PDPOSGSC-G6L7")
        coupon_7_11y = vals.get("PDPOSGSC-G7L11")
        coupon_gt11y = vals.get("PDPOSGSC-G11")       # combined >11yr (pre-2022)
        coupon_11_21y = vals.get("PDPOSGSC-G11L21")    # split 11-21yr (2022+)
        coupon_gt21y = vals.get("PDPOSGSC-G21")        # split >21yr (2022+)

        output_rows.append({
            "as_of_date": date_str,
            "week_start": week_start_value,
            "week_end": week_end_value,
            "pd_treasury_inventory": total_treasury,
            "pd_bills_position": bills,
            "pd_coupon_position": coupon_total,
            "pd_coupon_le2y": coupon_le2y,
            "pd_coupon_2_3y": coupon_2_3y,
            "pd_coupon_3_6y": coupon_3_6y,
            "pd_coupon_6_7y": coupon_6_7y,
            "pd_coupon_7_11y": coupon_7_11y,
            "pd_coupon_gt11y": coupon_gt11y,
            "pd_coupon_11_21y": coupon_11_21y,
            "pd_coupon_gt21y": coupon_gt21y,
            "pd_tips_position": tips_total,
            "pd_frn_position": frn,
            "pd_repo_treasury": repo,
            "pd_reverse_repo_treasury": reverse_repo,
            "pd_financing_usage": net_financing,
        })

    _EXPECTED_COLUMNS = [
        "as_of_date", "week_start", "week_end",
        "pd_treasury_inventory", "pd_bills_position", "pd_coupon_position",
        "pd_coupon_le2y", "pd_coupon_2_3y", "pd_coupon_3_6y",
        "pd_coupon_6_7y", "pd_coupon_7_11y", "pd_coupon_gt11y",
        "pd_coupon_11_21y", "pd_coupon_gt21y",
        "pd_tips_position", "pd_frn_position",
        "pd_repo_treasury", "pd_reverse_repo_treasury", "pd_financing_usage",
    ]

    df = pd.DataFrame(output_rows)

    if df.empty:
        df = pd.DataFrame(columns=_EXPECTED_COLUMNS)
    else:
        df = finalize_primary_dealer_dataframe(df, week_definition=week_definition)

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
