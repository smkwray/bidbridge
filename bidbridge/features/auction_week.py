from __future__ import annotations

import pandas as pd

from .bridge_metrics import add_bridge_metrics


_WEEKDAY_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


def normalize_week_definition(week_definition: str | None) -> str:
    """Normalize supported week-definition aliases."""
    if not week_definition:
        return "monday"

    normalized = str(week_definition).strip().lower()
    alias_map = {
        "monday_start": "monday",
        "mon": "monday",
        "thursday_start": "thursday",
        "thu": "thursday",
    }
    normalized = alias_map.get(normalized, normalized)
    if normalized not in _WEEKDAY_INDEX:
        raise ValueError(f"Unsupported week definition: {week_definition}")
    return normalized


def week_start(date_series: pd.Series, week_definition: str = "monday") -> pd.Series:
    """Map dates to the start of the configured 7-day week."""
    dates = pd.to_datetime(date_series)
    weekday = _WEEKDAY_INDEX[normalize_week_definition(week_definition)]
    delta = (dates.dt.weekday - weekday) % 7
    return (dates - pd.to_timedelta(delta, unit="D")).dt.normalize()


def week_end(week_start_series: pd.Series) -> pd.Series:
    """Return the inclusive week end for a normalized week-start series."""
    return pd.to_datetime(week_start_series) + pd.Timedelta(days=6)


def monday_start(date_series: pd.Series) -> pd.Series:
    return week_start(date_series, "monday")


def choose_investor_merge_keys(
    auctions: pd.DataFrame,
    investor_class: pd.DataFrame,
) -> list[str]:
    """Choose stable merge keys for auctions x investor-class joins."""
    has_cusip = "cusip" in auctions.columns and "cusip" in investor_class.columns
    has_issue_date = "issue_date" in auctions.columns and "issue_date" in investor_class.columns
    if has_cusip and has_issue_date and auctions["cusip"].notna().any():
        return ["cusip", "issue_date"]
    if has_cusip and auctions["cusip"].notna().any():
        return ["cusip"]
    return ["issue_date", "security_type"]


def weighted_average(values: pd.Series, weights: pd.Series) -> float:
    """Weighted average, dropping rows where either value or weight is missing."""
    mask = values.notna() & weights.notna()
    v = values[mask]
    w = weights[mask]
    if w.sum() == 0.0 or len(w) == 0:
        return float("nan")
    return float((v * w).sum() / w.sum())


def build_weekly_panel(
    auctions: pd.DataFrame,
    investor_class: pd.DataFrame,
    dealer_stats: pd.DataFrame,
    week_definition: str = "monday",
) -> pd.DataFrame:
    auctions = auctions.copy()
    investor_class = investor_class.copy()
    dealer_stats = dealer_stats.copy()

    auctions["week_start"] = week_start(auctions["auction_date"], week_definition)
    auctions["week_end"] = week_end(auctions["week_start"])

    investor_class["issue_date"] = pd.to_datetime(investor_class["issue_date"])
    auctions["issue_date"] = pd.to_datetime(auctions["issue_date"])

    # Merge auctions with investor class allotments.
    # Use (cusip, issue_date) when both sides have both — this handles reopenings
    # where the same CUSIP appears across multiple issue dates.
    # Fall back to (issue_date, security_type) for demo data without cusip.
    merge_keys = choose_investor_merge_keys(auctions, investor_class)

    # Drop duplicate merge keys on investor_class side to avoid many-to-many
    ic_deduped = investor_class.drop_duplicates(subset=merge_keys, keep="last")

    merged = auctions.merge(
        ic_deduped,
        on=merge_keys,
        how="left",
        suffixes=("", "_ic"),
    )

    rows: list[dict] = []
    for (ws, we), g in merged.groupby(["week_start", "week_end"], sort=True):
        rows.append(
            {
                "week_start": ws,
                "week_end": we,
                "auction_count": int(g.shape[0]),
                "announced_amount_total": float(g["announced_amount"].sum()),
                "awarded_amount_total": float(g["awarded_amount"].sum()),
                "bill_amount": float(g.loc[g["security_type"].eq("Bill"), "awarded_amount"].sum()),
                "coupon_amount": float(g.loc[g["security_type"].ne("Bill"), "awarded_amount"].sum()),
                "refunding_week": bool(g["refunding_week"].any()),
                "weighted_bid_to_cover": weighted_average(g["bid_to_cover"], g["awarded_amount"]),
                "weighted_tail_bp": weighted_average(g["tail_bp"], g["awarded_amount"]),
                "dealer_share_allotment": weighted_average(g["dealer_share"], g["awarded_amount"]),
                "investment_funds_share_allotment": weighted_average(
                    g["investment_funds_share"], g["awarded_amount"]
                ),
                "foreign_share_allotment": weighted_average(g["foreign_share"], g["awarded_amount"]),
                "depository_share_allotment": weighted_average(
                    g["depository_share"], g["awarded_amount"]
                ),
                "other_share_allotment": weighted_average(g["other_share"], g["awarded_amount"]),
            }
        )

    weekly = pd.DataFrame(rows)

    weekly["bill_share"] = (
        weekly["bill_amount"] / weekly["awarded_amount_total"].replace({0: pd.NA})
    ).fillna(0.0)
    weekly["coupon_share"] = (
        weekly["coupon_amount"] / weekly["awarded_amount_total"].replace({0: pd.NA})
    ).fillna(0.0)

    # Preserve NaN when dealer_share is missing (no investor-class match).
    # Do NOT fill missing dealer_share with 0 — that would make nondealer = 1.0.
    weekly["nondealer_share"] = 1.0 - weekly["dealer_share_allotment"]

    dealer_stats["week_start"] = pd.to_datetime(dealer_stats["week_start"])
    dealer_stats["week_end"] = pd.to_datetime(dealer_stats["week_end"])

    panel = weekly.merge(
        dealer_stats,
        on=["week_start", "week_end"],
        how="left",
        validate="1:1",
    )

    panel = add_bridge_metrics(panel)
    panel.attrs["week_definition"] = normalize_week_definition(week_definition)
    return panel.sort_values("week_start").reset_index(drop=True)
