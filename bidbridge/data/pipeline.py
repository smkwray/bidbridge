"""Data pipeline: fetch sources, harmonize, build panel.

This module wires the individual fetchers together and produces the
auction-week panel with bridge metrics.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..features.auction_week import build_weekly_panel
from ..features.maturity_panel import build_maturity_panel, pivot_maturity_panel_wide
from ..paths import PROCESSED_DIR, RAW_DIR
from .sources.h8 import fetch_h8
from .sources.nyfed_pd import fetch_primary_dealer_statistics
from .sources.soma import fetch_soma_holdings
from .sources.treasury_auctions import fetch_treasury_auctions
from .sources.treasury_investor_class import fetch_investor_class_allotments

logger = logging.getLogger(__name__)


def _tag_refunding_weeks(auctions: pd.DataFrame) -> pd.DataFrame:
    """Mark refunding weeks based on the quarterly refunding package.

    Treasury quarterly refundings occur in February, May, August, November.
    The refunding package is a specific week containing both a 10-year Note
    and a 30-year Bond auction. This is tighter than "any week with >=2 coupon
    auctions in a refunding month," which overflags ordinary multi-auction weeks.
    """
    df = auctions.copy()
    df["auction_date"] = pd.to_datetime(df["auction_date"])
    refunding_months = {2, 5, 8, 11}

    df["_month"] = df["auction_date"].dt.month
    df["_week"] = df["auction_date"].dt.isocalendar().week.astype(int)
    df["_year"] = df["auction_date"].dt.year

    # Extract approximate term years for identifying 10Y and 30Y
    term_str = df.get("security_term", pd.Series(dtype=str))
    df["_term_years"] = term_str.str.extract(r"(\d+)-Year", expand=False).astype(float)

    # A refunding week must have both a ~10Y Note and a ~30Y Bond in a refunding month
    in_refunding_month = df["_month"].isin(refunding_months)
    is_10y = in_refunding_month & (df["_term_years"] >= 9) & (df["_term_years"] <= 11) & (df["security_type"] == "Note")
    is_30y = in_refunding_month & (df["_term_years"] >= 29) & (df["security_type"] == "Bond")

    weeks_with_10y = df.loc[is_10y, ["_year", "_week"]].drop_duplicates()
    weeks_with_30y = df.loc[is_30y, ["_year", "_week"]].drop_duplicates()

    refunding_weeks = weeks_with_10y.merge(weeks_with_30y, on=["_year", "_week"])

    df = df.merge(
        refunding_weeks.assign(refunding_week=True),
        on=["_year", "_week"],
        how="left",
    )
    df["refunding_week"] = df["refunding_week"].fillna(False)
    df = df.drop(columns=["_month", "_week", "_year", "_term_years"])
    return df


def _harmonize_auctions(raw_path: Path) -> pd.DataFrame:
    """Load raw auction CSV and harmonize to panel input schema."""
    df = pd.read_csv(raw_path, parse_dates=["auction_date", "issue_date", "maturity_date"])

    # Map instrument_group to the security_type expected by build_weekly_panel
    # The panel expects: Bill, Note, Bond (matching schema)
    # Keep original security_type from API

    # Compute maturity_bucket from instrument_group
    bucket_map = {
        "bills": "bills",
        "cmb": "bills",
        "nominal_coupons": "nominal_coupons",
        "bonds": "bonds",
        "tips": "tips",
        "frns": "frns",
    }
    df["maturity_bucket"] = df["instrument_group"].map(bucket_map).fillna("other")

    df = _tag_refunding_weeks(df)

    # Keep columns matching RAW_AUCTIONS_COLUMNS schema
    keep = [
        "auction_date", "issue_date", "security_type", "security_term",
        "maturity_bucket", "announced_amount", "awarded_amount", "bid_to_cover",
        "tail_bp", "refunding_week", "cusip", "instrument_group",
    ]
    return df[[c for c in keep if c in df.columns]].copy()


def _harmonize_investor_class(raw_path: Path) -> pd.DataFrame:
    """Load raw investor class CSV and harmonize."""
    df = pd.read_csv(raw_path, parse_dates=["issue_date"])

    keep = [
        "issue_date", "security_type", "cusip",
        "dealer_share", "investment_funds_share", "foreign_share",
        "depository_share", "other_share",
    ]
    return df[[c for c in keep if c in df.columns]].copy()


def _harmonize_dealer_stats(raw_path: Path) -> pd.DataFrame:
    """Load raw dealer stats CSV and harmonize to panel input schema."""
    df = pd.read_csv(raw_path, parse_dates=["week_start", "week_end"])

    keep = [
        "week_start", "week_end",
        "pd_treasury_inventory", "pd_financing_usage",
    ]
    return df[[c for c in keep if c in df.columns]].copy()


def _manifest_age_days(manifest_path: Path) -> float | None:
    """Return the age (in days) of a manifest file, or None if it doesn't exist."""
    if not manifest_path.exists():
        return None
    import json
    from datetime import datetime, timezone

    try:
        with manifest_path.open() as f:
            data = json.load(f)
        retrieved = data.get("retrieved_at_utc", "")
        dt = datetime.fromisoformat(retrieved.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 86400
    except (json.JSONDecodeError, ValueError, KeyError):
        return None


def fetch_all(
    raw_dir: Path | None = None,
    start_date: str = "2010-01-01",
    max_age_days: float | None = None,
) -> dict[str, Path]:
    """Fetch all data sources.

    Parameters
    ----------
    max_age_days : float, optional
        If set, skip sources whose manifest is younger than this many days.
        Use for incremental updates (e.g., max_age_days=1 to refresh daily).

    Returns dict mapping source name to CSV path.
    """
    raw_dir = raw_dir or RAW_DIR
    results = {}

    # Each entry: (name, out_dir, manifest_name, expected_csv_name, fetcher)
    sources = [
        ("auctions", raw_dir / "treasury", "treasury_auctions_manifest.json",
         "treasury_auctions.csv",
         lambda d: fetch_treasury_auctions(d, start_date=start_date)),
        ("dealer_stats", raw_dir / "nyfed", "primary_dealer_stats_manifest.json",
         "primary_dealer_stats.csv",
         lambda d: fetch_primary_dealer_statistics(d, start_date=start_date)),
        ("investor_class", raw_dir / "treasury", "investor_class_allotments_manifest.json",
         "investor_class_allotments.csv",
         lambda d: fetch_investor_class_allotments(d, start_date=start_date)),
        ("soma", raw_dir / "nyfed", "soma_holdings_manifest.json",
         "soma_holdings.csv",
         lambda d: fetch_soma_holdings(d, start_date=start_date)),
        ("h8", raw_dir / "fed", "h8_bank_securities_manifest.json",
         "h8_bank_securities.csv",
         lambda d: fetch_h8(d, start_date=start_date)),
    ]

    for name, out_dir, manifest_name, csv_name, fetcher in sources:
        manifest_path = out_dir / manifest_name
        expected_csv = out_dir / csv_name
        age = _manifest_age_days(manifest_path)
        if max_age_days is not None and age is not None and age < max_age_days:
            if expected_csv.exists():
                logger.info("Skipping %s (%.1f days old, max_age=%.1f)", name, age, max_age_days)
                results[name] = expected_csv
                continue
        logger.info("Fetching %s...", name)
        results[name] = fetcher(out_dir)

    return results


def build_panel(
    raw_dir: Path | None = None,
    output_path: Path | None = None,
    start_date: str = "2010-01-01",
) -> Path:
    """Build the auction-week panel from raw fetched data.

    If raw data doesn't exist, fetches it first.
    """
    raw_dir = raw_dir or RAW_DIR
    output_path = output_path or (PROCESSED_DIR / "auction_week_panel.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    auctions_path = raw_dir / "treasury" / "treasury_auctions.csv"
    dealer_path = raw_dir / "nyfed" / "primary_dealer_stats.csv"
    investor_path = raw_dir / "treasury" / "investor_class_allotments.csv"
    soma_path = raw_dir / "nyfed" / "soma_holdings.csv"

    # Fetch if missing
    if not auctions_path.exists() or not dealer_path.exists() or not investor_path.exists():
        logger.info("Raw data missing, fetching...")
        fetch_all(raw_dir=raw_dir, start_date=start_date)

    auctions = _harmonize_auctions(auctions_path)
    investor_class = _harmonize_investor_class(investor_path)
    dealer_stats = _harmonize_dealer_stats(dealer_path)

    logger.info(
        "Building panel: %d auctions, %d investor-class rows, %d dealer-stat weeks",
        len(auctions), len(investor_class), len(dealer_stats),
    )

    panel = build_weekly_panel(auctions, investor_class, dealer_stats)

    # Merge SOMA holdings if available.
    # SOMA as_of_date is Wednesday. To avoid look-ahead bias, lag by one week:
    # the SOMA observation from Wednesday of week W is merged onto panel week W+1.
    if soma_path.exists():
        soma = pd.read_csv(soma_path, parse_dates=["as_of_date"])
        soma_value_cols = ["soma_treasury_total", "soma_bills", "soma_notes_bonds", "soma_tips"]
        available_vals = [c for c in soma_value_cols if c in soma.columns]

        # Lag: map as_of Wednesday to the NEXT Monday-start week
        soma["_merge_week"] = (
            soma["as_of_date"]
            + pd.Timedelta(days=7)
            - pd.to_timedelta(
                (soma["as_of_date"] + pd.Timedelta(days=7)).dt.weekday, unit="D"
            )
        ).dt.normalize()

        soma_merge = soma[["_merge_week"] + available_vals].copy()
        soma_merge = soma_merge.rename(columns={"_merge_week": "week_start"})
        soma_merge = soma_merge.drop_duplicates(subset=["week_start"], keep="last")

        panel["week_start"] = pd.to_datetime(panel["week_start"])
        panel = panel.merge(soma_merge, on="week_start", how="left")
        logger.info(
            "Merged SOMA data (%d non-null weeks, lagged 1 week)",
            panel["soma_treasury_total"].notna().sum(),
        )

    # Merge H.8 bank securities if available.
    # H.8 observations are dated to Wednesday (end of reporting week). The data is
    # released the following Friday, so an H.8 observation for Wednesday 2025-01-08
    # reflects the bank balance sheet as of that date. To avoid forward-looking bias,
    # we lag the H.8 series by one week: the H.8 observation for week W is merged
    # onto panel week W+1 (the auction week it could actually explain).
    h8_path = raw_dir / "fed" / "h8_bank_securities.csv"
    if h8_path.exists():
        h8 = pd.read_csv(h8_path, parse_dates=["as_of_date"])
        # Map H.8 as_of_date (Wednesday) to the NEXT Monday-start week
        h8["_merge_week"] = (
            h8["as_of_date"]
            + pd.Timedelta(days=7)  # lag by one week
            - pd.to_timedelta(
                (h8["as_of_date"] + pd.Timedelta(days=7)).dt.weekday, unit="D"
            )
        ).dt.normalize()
        h8_merge = h8[["_merge_week", "bank_treasury_securities"]].copy()
        h8_merge = h8_merge.rename(columns={"_merge_week": "week_start"})
        h8_merge = h8_merge.drop_duplicates(subset=["week_start"], keep="last")
        panel = panel.merge(h8_merge, on="week_start", how="left")
        logger.info(
            "Merged H.8 data (%d non-null weeks, lagged 1 week)",
            panel["bank_treasury_securities"].notna().sum(),
        )

    panel.to_csv(output_path, index=False)
    logger.info("Wrote panel (%d weeks) to %s", len(panel), output_path)

    # Build maturity-bucket panel alongside the aggregate panel.
    # Use the harmonized auctions (which have instrument_group and refunding_week)
    # and the raw investor class data (which has cusip for merging).
    maturity_output = output_path.parent / "maturity_bucket_panel.csv"
    mat_panel = build_maturity_panel(auctions, investor_class)
    mat_panel.to_csv(maturity_output, index=False)

    # Wide-format maturity panel (one row per week, bucket-specific columns)
    wide_output = output_path.parent / "maturity_wide_panel.csv"
    wide_panel = pivot_maturity_panel_wide(mat_panel)
    wide_panel.to_csv(wide_output, index=False)
    logger.info("Wrote wide maturity panel (%d rows, %d cols) to %s",
                len(wide_panel), len(wide_panel.columns), wide_output)
    logger.info(
        "Wrote maturity panel (%d rows, %d buckets) to %s",
        len(mat_panel), mat_panel["maturity_bucket"].nunique(), maturity_output,
    )

    return output_path
