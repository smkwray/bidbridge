from __future__ import annotations

import pandas as pd

from .bridge_metrics import add_bridge_metrics


def monday_start(date_series: pd.Series) -> pd.Series:
    dates = pd.to_datetime(date_series)
    return (dates - pd.to_timedelta(dates.dt.weekday, unit="D")).dt.normalize()


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
) -> pd.DataFrame:
    auctions = auctions.copy()
    investor_class = investor_class.copy()
    dealer_stats = dealer_stats.copy()

    auctions["week_start"] = monday_start(auctions["auction_date"])
    auctions["week_end"] = auctions["week_start"] + pd.Timedelta(days=6)

    investor_class["issue_date"] = pd.to_datetime(investor_class["issue_date"])
    auctions["issue_date"] = pd.to_datetime(auctions["issue_date"])

    # Merge auctions with investor class allotments.
    # Use (cusip, issue_date) when both sides have both — this handles reopenings
    # where the same CUSIP appears across multiple issue dates.
    # Fall back to (issue_date, security_type) for demo data without cusip.
    has_cusip = "cusip" in auctions.columns and "cusip" in investor_class.columns
    has_issue_date = "issue_date" in auctions.columns and "issue_date" in investor_class.columns
    if has_cusip and has_issue_date and auctions["cusip"].notna().any():
        merge_keys = ["cusip", "issue_date"]
    elif has_cusip and auctions["cusip"].notna().any():
        merge_keys = ["cusip"]
    else:
        merge_keys = ["issue_date", "security_type"]

    # Drop duplicate merge keys on investor_class side to avoid many-to-many
    ic_deduped = investor_class.drop_duplicates(subset=merge_keys, keep="last")

    merged = auctions.merge(
        ic_deduped,
        on=merge_keys,
        how="left",
        suffixes=("", "_ic"),
    )

    rows: list[dict] = []
    for (week_start, week_end), g in merged.groupby(["week_start", "week_end"], sort=True):
        rows.append(
            {
                "week_start": week_start,
                "week_end": week_end,
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
    return panel.sort_values("week_start").reset_index(drop=True)
