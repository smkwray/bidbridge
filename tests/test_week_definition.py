from __future__ import annotations

import pandas as pd
import pytest

from bidbridge.analysis.panel_fe import build_bucket_outcomes
from bidbridge.features.auction_week import week_start
from bidbridge.features.maturity_panel import build_maturity_panel


def test_week_start_supports_thursday_anchor():
    dates = pd.Series(pd.to_datetime(["2025-01-08", "2025-01-09", "2025-01-10"]))
    starts = week_start(dates, "thursday")
    assert starts.dt.strftime("%Y-%m-%d").tolist() == [
        "2025-01-02", "2025-01-09", "2025-01-09",
    ]


def test_build_maturity_panel_uses_configurable_week_definition():
    auctions = pd.DataFrame({
        "auction_date": pd.to_datetime(["2025-01-08", "2025-01-10"]),
        "issue_date": pd.to_datetime(["2025-01-09", "2025-01-13"]),
        "security_type": ["Note", "Bill"],
        "instrument_group": ["nominal_coupons", "bills"],
        "security_term": ["2-Year", "26-Week"],
        "announced_amount": [50_000.0, 40_000.0],
        "awarded_amount": [48_000.0, 40_000.0],
        "bid_to_cover": [2.5, 3.2],
        "tail_bp": [0.2, 0.0],
        "refunding_week": [False, False],
        "cusip": ["A", "B"],
    })
    investor = pd.DataFrame({
        "issue_date": pd.to_datetime(["2025-01-09", "2025-01-13"]),
        "security_type": ["Note", "Bill"],
        "cusip": ["A", "B"],
        "dealer_share": [0.3, 0.5],
        "investment_funds_share": [0.3, 0.2],
        "foreign_share": [0.2, 0.2],
        "depository_share": [0.1, 0.05],
        "other_share": [0.1, 0.05],
    })

    panel = build_maturity_panel(auctions, investor, week_definition="thursday")
    assert set(panel["week_start"].dt.strftime("%Y-%m-%d")) == {"2025-01-02", "2025-01-09"}


def test_build_bucket_outcomes_fails_loud_without_granular_bands():
    maturity_panel = pd.DataFrame({
        "week_start": pd.to_datetime(["2025-01-06"]),
        "maturity_bucket": ["short_coupon"],
        "announced_amount": [50_000.0],
        "awarded_amount": [48_000.0],
        "dealer_share": [0.35],
    })
    dealer_stats = pd.DataFrame({
        "week_start": pd.to_datetime(["2025-01-06"]),
        "pd_bills_position": [80_000.0],
        "pd_coupon_position": [120_000.0],
        "pd_tips_position": [10_000.0],
    })

    with pytest.raises(ValueError, match="Granular coupon band columns"):
        build_bucket_outcomes(
            maturity_panel,
            dealer_stats,
            headline_strict=True,
            week_definition="monday",
        )
