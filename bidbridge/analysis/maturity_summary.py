"""Deterministic maturity-bucket summary used by the OPEN-07 promotion surface."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bidbridge.data.pipeline import _harmonize_auctions, _harmonize_investor_class
from bidbridge.features.maturity_panel import build_maturity_panel

OPEN07_MATURITY_SUMMARY_AS_OF = "2026-03-16"


def build_maturity_bucket_summary(maturity_panel: pd.DataFrame) -> pd.DataFrame:
    """Summarize one maturity-panel row per bucket with frozen rounding."""
    return (
        maturity_panel.groupby("maturity_bucket")
        .agg(
            auction_count_sum=("auction_count", "sum"),
            awarded_amount_sum=("awarded_amount", "sum"),
            awarded_amount_mean=("awarded_amount", "mean"),
            dealer_share_mean=("dealer_share", "mean"),
            dealer_share_std=("dealer_share", "std"),
            weighted_bid_to_cover_mean=("weighted_bid_to_cover", "mean"),
            weighted_tail_bp_mean=("weighted_tail_bp", "mean"),
            bucket_share_of_weekly_mean=("bucket_share_of_weekly", "mean"),
        )
        .reset_index()
        .round(4)
    )


def write_open07_maturity_bucket_summary(
    *,
    auctions_path: Path,
    investor_class_path: Path,
    output_path: Path,
    as_of: str = OPEN07_MATURITY_SUMMARY_AS_OF,
) -> tuple[Path, pd.DataFrame]:
    """Rebuild the dated maturity summary without using a stale retained table."""
    auctions = _harmonize_auctions(auctions_path)
    auctions = auctions.loc[auctions["auction_date"] <= pd.Timestamp(as_of)].copy()
    investor = _harmonize_investor_class(investor_class_path)
    maturity_panel = build_maturity_panel(
        auctions,
        investor,
        week_definition="monday",
    )
    summary = build_maturity_bucket_summary(maturity_panel)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output_path, index=False, lineterminator="\n")
    return output_path, summary
