"""Maturity-bucket auction-week panel.

Produces a panel with one row per (week_start, maturity_bucket) pair,
enabling analysis of dealer absorption patterns across the yield curve.

Maturity buckets follow configs/study.yml:
  - bills: 4W-52W
  - short_coupon: 2Y, 3Y
  - belly_coupon: 5Y, 7Y
  - long_coupon: 10Y, 20Y, 30Y
  - tips: all TIPS
  - frns: all FRN
"""

from __future__ import annotations

import re

import pandas as pd

from .auction_week import monday_start, weighted_average


# Map security_term strings from FiscalData to maturity buckets
_BILL_TERMS = {"4-Week", "6-Week", "8-Week", "13-Week", "17-Week", "26-Week", "52-Week"}
_SHORT_TERMS = {"2-Year", "3-Year"}
_BELLY_TERMS = {"5-Year", "7-Year"}
_LONG_TERMS = {"10-Year", "20-Year", "30-Year"}


def _classify_maturity_bucket(row: pd.Series) -> str:
    """Classify an auction row into a maturity bucket."""
    ig = row.get("instrument_group", "")
    term = str(row.get("security_term", ""))

    if ig == "tips":
        return "tips"
    if ig == "frns":
        return "frns"
    if ig in ("bills", "cmb"):
        return "bills"

    # Normalize term: "9-Year 10-Month" -> extract leading number
    term_clean = term.strip()

    # Check exact matches first
    for t in _SHORT_TERMS:
        if term_clean.startswith(t.split("-")[0]) and "Year" in term_clean:
            yr = _extract_years(term_clean)
            if yr is not None and yr <= 3:
                return "short_coupon"

    for t in _BELLY_TERMS:
        if term_clean.startswith(t.split("-")[0]) and "Year" in term_clean:
            yr = _extract_years(term_clean)
            if yr is not None and 4 <= yr <= 7:
                return "belly_coupon"

    for t in _LONG_TERMS:
        yr = _extract_years(term_clean)
        if yr is not None and yr >= 10:
            return "long_coupon"

    # Fallback by instrument_group
    if ig == "bonds":
        return "long_coupon"
    if ig == "nominal_coupons":
        yr = _extract_years(term_clean)
        if yr is not None:
            if yr <= 3:
                return "short_coupon"
            if yr <= 7:
                return "belly_coupon"
            return "long_coupon"
        return "belly_coupon"  # default for unclassified coupons

    return "other"


def _extract_years(term: str) -> float | None:
    """Extract approximate years from a security_term string like '9-Year 10-Month'."""
    years = 0.0
    yr_match = re.search(r"(\d+)-Year", term)
    if yr_match:
        years += int(yr_match.group(1))
    mo_match = re.search(r"(\d+)-Month", term)
    if mo_match:
        years += int(mo_match.group(1)) / 12.0
    return years if years > 0 else None


def build_maturity_panel(
    auctions: pd.DataFrame,
    investor_class: pd.DataFrame,
) -> pd.DataFrame:
    """Build a (week_start, maturity_bucket) panel from auction-level data.

    Parameters
    ----------
    auctions : DataFrame
        Harmonized auction data with columns: auction_date, issue_date,
        security_type, instrument_group, security_term, awarded_amount,
        announced_amount, bid_to_cover, tail_bp, refunding_week, cusip.
    investor_class : DataFrame
        Harmonized investor class data with: issue_date, security_type,
        cusip, dealer_share, investment_funds_share, foreign_share, etc.

    Returns
    -------
    DataFrame
        Panel with one row per (week_start, maturity_bucket).
    """
    auctions = auctions.copy()
    investor_class = investor_class.copy()

    auctions["week_start"] = monday_start(auctions["auction_date"])
    auctions["week_end"] = auctions["week_start"] + pd.Timedelta(days=6)

    # Classify maturity buckets
    auctions["maturity_bucket"] = auctions.apply(_classify_maturity_bucket, axis=1)

    # Merge with investor class
    auctions["issue_date"] = pd.to_datetime(auctions["issue_date"])
    investor_class["issue_date"] = pd.to_datetime(investor_class["issue_date"])

    has_cusip = "cusip" in auctions.columns and "cusip" in investor_class.columns
    has_issue_date = "issue_date" in auctions.columns and "issue_date" in investor_class.columns
    if has_cusip and has_issue_date and auctions["cusip"].notna().any():
        merge_keys = ["cusip", "issue_date"]
    elif has_cusip and auctions["cusip"].notna().any():
        merge_keys = ["cusip"]
    else:
        merge_keys = ["issue_date", "security_type"]

    ic_deduped = investor_class.drop_duplicates(subset=merge_keys, keep="last")
    merged = auctions.merge(ic_deduped, on=merge_keys, how="left", suffixes=("", "_ic"))

    # Group by (week, bucket)
    rows: list[dict] = []
    for (ws, we, bucket), g in merged.groupby(
        ["week_start", "week_end", "maturity_bucket"], sort=True
    ):
        rows.append({
            "week_start": ws,
            "week_end": we,
            "maturity_bucket": bucket,
            "auction_count": int(len(g)),
            "announced_amount": float(g["announced_amount"].sum()),
            "awarded_amount": float(g["awarded_amount"].sum()),
            "weighted_bid_to_cover": weighted_average(g["bid_to_cover"], g["awarded_amount"]),
            "weighted_tail_bp": weighted_average(g["tail_bp"], g["awarded_amount"]),
            "dealer_share": weighted_average(
                g["dealer_share"], g["awarded_amount"]
            ) if "dealer_share" in g.columns else None,
            "investment_funds_share": weighted_average(
                g["investment_funds_share"], g["awarded_amount"]
            ) if "investment_funds_share" in g.columns else None,
            "foreign_share": weighted_average(
                g["foreign_share"], g["awarded_amount"]
            ) if "foreign_share" in g.columns else None,
            "refunding_week": bool(g["refunding_week"].any()) if "refunding_week" in g.columns else False,
        })

    panel = pd.DataFrame(rows)

    # Add bucket-level supply share (% of total weekly awarded)
    weekly_total = panel.groupby("week_start")["awarded_amount"].transform("sum")
    panel["bucket_share_of_weekly"] = (
        panel["awarded_amount"] / weekly_total.replace({0: pd.NA})
    ).fillna(0.0)

    return panel.sort_values(["week_start", "maturity_bucket"]).reset_index(drop=True)


def pivot_maturity_panel_wide(maturity_panel: pd.DataFrame) -> pd.DataFrame:
    """Pivot the long-format maturity panel to one row per week.

    Creates columns like bills_awarded, bills_dealer_share, short_coupon_awarded, etc.
    Useful for regressions that need maturity-specific regressors in the same row.
    """
    mp = maturity_panel.copy()
    buckets = sorted(mp["maturity_bucket"].unique())

    # Columns to pivot
    value_cols = ["awarded_amount", "dealer_share", "weighted_bid_to_cover",
                  "weighted_tail_bp", "bucket_share_of_weekly"]

    pivoted = mp.pivot_table(
        index="week_start",
        columns="maturity_bucket",
        values=[c for c in value_cols if c in mp.columns],
        aggfunc="first",
    )

    # Flatten column names: (awarded_amount, bills) -> bills_awarded
    pivoted.columns = [f"{bucket}_{col}" for col, bucket in pivoted.columns]
    pivoted = pivoted.reset_index()

    # Add total auction_count per week
    weekly_count = mp.groupby("week_start")["auction_count"].sum().reset_index()
    weekly_count.columns = ["week_start", "total_auction_count"]
    pivoted = pivoted.merge(weekly_count, on="week_start", how="left")

    # Add refunding flag
    refunding = mp.groupby("week_start")["refunding_week"].any().reset_index()
    pivoted = pivoted.merge(refunding, on="week_start", how="left")

    return pivoted.sort_values("week_start").reset_index(drop=True)
