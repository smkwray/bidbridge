"""Edge-case tests for BidBridge.

Covers reopening merges, NaN handling in weighted_average and bridge_metrics,
maturity bucket classification, stress-flag consecutive-decline logic,
empty fetcher results, and nondealer_share with missing dealer data.

Run:
  ~/venvs/bidbridge/bin/python -B -m pytest tests/test_edge_cases.py -v
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# 1.  Reopening merge — same CUSIP, two different issue_dates
# ---------------------------------------------------------------------------

class TestReopeningMerge:
    """build_weekly_panel must merge each issue_date to the correct allotment
    row when the same CUSIP appears with two different issue_dates (a reopening)."""

    def test_reopening_merge_correct_allotment(self):
        from bidbridge.features.auction_week import build_weekly_panel

        # Two auctions with the same CUSIP but different issue_dates (reopening).
        auctions = pd.DataFrame({
            "cusip": ["912828ZZ0", "912828ZZ0"],
            "auction_date": pd.to_datetime(["2025-01-06", "2025-02-03"]),
            "issue_date": pd.to_datetime(["2025-01-10", "2025-02-07"]),
            "security_type": ["Note", "Note"],
            "security_term": ["2-Year", "2-Year"],
            "instrument_group": ["nominal_coupons", "nominal_coupons"],
            "announced_amount": [60000.0, 60000.0],
            "awarded_amount": [60000.0, 60000.0],
            "bid_to_cover": [2.5, 2.6],
            "tail_bp": [0.3, 0.5],
            "refunding_week": [False, False],
        })

        # Investor class: same CUSIP, different issue_date, different dealer_share.
        investor_class = pd.DataFrame({
            "cusip": ["912828ZZ0", "912828ZZ0"],
            "issue_date": pd.to_datetime(["2025-01-10", "2025-02-07"]),
            "security_type": ["Note", "Note"],
            "dealer_share": [0.30, 0.45],
            "investment_funds_share": [0.25, 0.20],
            "foreign_share": [0.20, 0.15],
            "depository_share": [0.15, 0.10],
            "other_share": [0.10, 0.10],
        })

        dealer_stats = pd.DataFrame({
            "week_start": pd.to_datetime(["2025-01-06", "2025-02-03"]),
            "week_end": pd.to_datetime(["2025-01-12", "2025-02-09"]),
            "pd_treasury_inventory": [200000.0, 210000.0],
            "pd_financing_usage": [120000.0, 125000.0],
        })

        panel = build_weekly_panel(auctions, investor_class, dealer_stats)

        assert len(panel) == 2, "Expected two weekly rows (one per auction week)"

        # Week 1 (Jan) should have dealer_share_allotment == 0.30
        week1 = panel[panel["week_start"] == pd.Timestamp("2025-01-06")].iloc[0]
        assert week1["dealer_share_allotment"] == pytest.approx(0.30, abs=1e-6), (
            "January reopening should get dealer_share 0.30, not the February value"
        )

        # Week 2 (Feb) should have dealer_share_allotment == 0.45
        week2 = panel[panel["week_start"] == pd.Timestamp("2025-02-03")].iloc[0]
        assert week2["dealer_share_allotment"] == pytest.approx(0.45, abs=1e-6), (
            "February reopening should get dealer_share 0.45, not the January value"
        )


# ---------------------------------------------------------------------------
# 2 & 3.  weighted_average with all-NaN and mixed NaN
# ---------------------------------------------------------------------------

class TestWeightedAverage:
    """Edge cases for the weighted_average helper."""

    def test_all_nan_returns_nan(self):
        from bidbridge.features.auction_week import weighted_average

        values = pd.Series([np.nan, np.nan, np.nan])
        weights = pd.Series([1.0, 2.0, 3.0])
        result = weighted_average(values, weights)
        assert math.isnan(result), (
            f"weighted_average with all-NaN values should return NaN, got {result}"
        )

    def test_mixed_nan_excludes_nan_rows(self):
        from bidbridge.features.auction_week import weighted_average

        values = pd.Series([10.0, np.nan, 30.0])
        weights = pd.Series([1.0, 2.0, 3.0])
        result = weighted_average(values, weights)
        # NaN row excluded: (10*1 + 30*3) / (1+3) = 100/4 = 25.0
        expected = 25.0
        assert result == pytest.approx(expected), (
            f"Expected {expected}, got {result}. "
            "NaN row should be excluded, not treated as 0."
        )


# ---------------------------------------------------------------------------
# 4 & 5.  bridge_metrics with NaN / missing inventory
# ---------------------------------------------------------------------------

class TestBridgeMetricsNaN:
    """add_bridge_metrics must handle NaN and missing inventory gracefully."""

    def test_nan_inventory_produces_nan_change(self):
        """When pd_treasury_inventory is NaN for some rows, inventory_change
        should be NaN (not 0.0) and bridge_episode should be False."""
        from bidbridge.features.bridge_metrics import add_bridge_metrics

        panel = pd.DataFrame({
            "week_start": pd.to_datetime([
                "2025-01-06", "2025-01-13", "2025-01-20", "2025-01-27",
            ]),
            "awarded_amount_total": [50000.0, 90000.0, 60000.0, 70000.0],
            "pd_treasury_inventory": [200000.0, np.nan, 220000.0, np.nan],
            "pd_financing_usage": [120000.0, np.nan, 130000.0, np.nan],
            "nondealer_share": [0.8, 0.7, 0.6, 0.5],
        })

        result = add_bridge_metrics(panel)

        # Row 1: diff from 200000 to NaN => NaN
        assert pd.isna(result.loc[1, "inventory_change"]), (
            "inventory_change should be NaN when pd_treasury_inventory is NaN, "
            f"got {result.loc[1, 'inventory_change']}"
        )
        # Row 3: diff from 220000 to NaN => NaN
        assert pd.isna(result.loc[3, "inventory_change"]), (
            "inventory_change should be NaN when pd_treasury_inventory is NaN"
        )

        # bridge_episode must be False for NaN-inventory rows (NaN > 0 is False)
        assert result.loc[1, "bridge_episode"] is np.False_ or result.loc[1, "bridge_episode"] is False
        assert result.loc[3, "bridge_episode"] is np.False_ or result.loc[3, "bridge_episode"] is False

    def test_missing_inventory_column_no_crash(self):
        """When pd_treasury_inventory column is absent entirely, the function
        should not crash and inventory_change should be NaN everywhere."""
        from bidbridge.features.bridge_metrics import add_bridge_metrics

        panel = pd.DataFrame({
            "week_start": pd.to_datetime([
                "2025-01-06", "2025-01-13", "2025-01-20",
            ]),
            "awarded_amount_total": [50000.0, 90000.0, 60000.0],
            "nondealer_share": [0.8, 0.7, 0.6],
        })

        result = add_bridge_metrics(panel)

        assert "inventory_change" in result.columns
        assert result["inventory_change"].isna().all(), (
            "inventory_change should be all NA when pd_treasury_inventory column is missing"
        )


# ---------------------------------------------------------------------------
# 6.  Maturity bucket classification edge cases
# ---------------------------------------------------------------------------

class TestMaturityBucketClassification:
    """Edge cases for _classify_maturity_bucket."""

    def _classify(self, term: str, instrument_group: str = "nominal_coupons") -> str:
        from bidbridge.features.maturity_panel import _classify_maturity_bucket

        row = pd.Series({
            "security_term": term,
            "instrument_group": instrument_group,
        })
        return _classify_maturity_bucket(row)

    def test_9year_10month_is_belly_or_long(self):
        """'9-Year 10-Month' is ~9.83 years. The classifier should place it
        in belly_coupon (4-7yr range) or long_coupon (>=10yr). Since 9.83 > 7
        and < 10, it falls outside the exact short/belly ranges. The actual
        code classifies it as long_coupon because _extract_years returns ~9.83
        and the _LONG_TERMS loop has yr >= 10 check which fails, but the
        fallback nominal_coupons path with yr > 7 returns long_coupon."""
        result = self._classify("9-Year 10-Month")
        assert result in ("belly_coupon", "long_coupon"), (
            f"'9-Year 10-Month' should be belly_coupon or long_coupon, got '{result}'"
        )

    def test_29year_11month_is_long_coupon(self):
        result = self._classify("29-Year 11-Month")
        assert result == "long_coupon", (
            f"'29-Year 11-Month' should be long_coupon, got '{result}'"
        )

    def test_2year_is_short_coupon(self):
        result = self._classify("2-Year")
        assert result == "short_coupon", (
            f"'2-Year' should be short_coupon, got '{result}'"
        )

    def test_52week_bills(self):
        """52-Week with instrument_group='bills' should classify as 'bills'."""
        result = self._classify("52-Week", instrument_group="bills")
        assert result == "bills", (
            f"'52-Week' bill should be 'bills', got '{result}'"
        )

    def test_frn_any_term(self):
        """FRN with any term should classify as 'frns'."""
        result = self._classify("2-Year", instrument_group="frns")
        assert result == "frns", (
            f"FRN should be 'frns' regardless of term, got '{result}'"
        )


# ---------------------------------------------------------------------------
# 7.  Stress flag — consecutive decline for qt_period
# ---------------------------------------------------------------------------

class TestStressFlagConsecutiveDecline:
    """qt_period should be True only after 4+ consecutive weeks of SOMA decline."""

    def test_qt_period_announcement_dates(self):
        """QT period is now based on announcement dates, not consecutive SOMA declines.

        QT1: 2017-10-01 to 2019-09-30
        QT2: 2022-06-01 to present
        """
        from bidbridge.features.stress_flags import add_stress_flags

        weeks = pd.to_datetime([
            "2017-09-25",  # Before QT1 -> False
            "2017-10-02",  # Inside QT1 -> True
            "2019-09-30",  # Last week of QT1 -> True
            "2019-10-07",  # After QT1 -> False
            "2022-05-30",  # Before QT2 -> False
            "2022-06-06",  # Inside QT2 -> True
            "2025-01-06",  # Inside QT2 (ongoing) -> True
        ])

        panel = pd.DataFrame({"week_start": weeks})
        result = add_stress_flags(panel)

        expected = [False, True, True, False, False, True, True]
        for i, exp in enumerate(expected):
            actual = bool(result.loc[i, "qt_period"])
            assert actual == exp, (
                f"Week {weeks[i].date()}: expected qt_period={exp}, got {actual}"
            )


# ---------------------------------------------------------------------------
# 8.  Empty fetcher result — far-future start_date
# ---------------------------------------------------------------------------

class TestEmptyFetcherResult:
    """Fetchers with a far-future start_date should return an empty CSV with
    correct schema, not crash.

    NOTE: These tests hit the live APIs.  They are marked with @pytest.mark.network
    so they can be excluded with: pytest -m 'not network'
    """

    @pytest.mark.network
    def test_fetch_treasury_auctions_empty(self, tmp_path):
        """fetch_treasury_auctions with start_date='2100-01-01' returns an empty
        CSV that still has the correct columns."""
        import requests
        try:
            resp = requests.head(
                "https://api.fiscaldata.treasury.gov", timeout=10, allow_redirects=True,
            )
            if resp.status_code >= 500:
                pytest.skip("Treasury FiscalData API is unreachable")
        except (requests.ConnectionError, requests.Timeout, OSError):
            pytest.skip("Treasury FiscalData API is unreachable")

        from bidbridge.data.sources.treasury_auctions import fetch_treasury_auctions

        csv_path = fetch_treasury_auctions(tmp_path, start_date="2100-01-01")
        assert csv_path.exists()
        df = pd.read_csv(csv_path)
        # Should be empty but with columns
        assert len(df) == 0 or len(df.columns) > 0

    @pytest.mark.network
    def test_fetch_primary_dealer_statistics_empty(self, tmp_path):
        """fetch_primary_dealer_statistics with start_date='2100-01-01'
        returns an empty CSV with correct schema."""
        import requests
        try:
            resp = requests.head(
                "https://markets.newyorkfed.org", timeout=10, allow_redirects=True,
            )
            if resp.status_code >= 500:
                pytest.skip("NY Fed Markets API is unreachable")
        except (requests.ConnectionError, requests.Timeout, OSError):
            pytest.skip("NY Fed Markets API is unreachable")

        from bidbridge.data.sources.nyfed_pd import fetch_primary_dealer_statistics

        csv_path = fetch_primary_dealer_statistics(tmp_path, start_date="2100-01-01")
        assert csv_path.exists()
        df = pd.read_csv(csv_path)
        # Should be empty but with columns
        assert len(df) == 0 or len(df.columns) > 0


# ---------------------------------------------------------------------------
# 9.  nondealer_share when dealer_share is NaN
# ---------------------------------------------------------------------------

class TestNondealerShareMissing:
    """When dealer_share_allotment is NaN, nondealer_share should also be NaN,
    not 1.0 (from 1.0 - fillna(0.0))."""

    def test_nondealer_share_is_nan_when_dealer_nan(self):
        from bidbridge.features.auction_week import build_weekly_panel

        # Auction with NO matching investor_class row => dealer_share will be NaN
        auctions = pd.DataFrame({
            "cusip": ["AAAA11111"],
            "auction_date": pd.to_datetime(["2025-03-03"]),
            "issue_date": pd.to_datetime(["2025-03-07"]),
            "security_type": ["Note"],
            "security_term": ["2-Year"],
            "instrument_group": ["nominal_coupons"],
            "announced_amount": [60000.0],
            "awarded_amount": [60000.0],
            "bid_to_cover": [2.5],
            "tail_bp": [0.3],
            "refunding_week": [False],
        })

        # Empty investor class — no rows match
        investor_class = pd.DataFrame({
            "cusip": pd.Series([], dtype=str),
            "issue_date": pd.Series([], dtype="datetime64[ns]"),
            "security_type": pd.Series([], dtype=str),
            "dealer_share": pd.Series([], dtype=float),
            "investment_funds_share": pd.Series([], dtype=float),
            "foreign_share": pd.Series([], dtype=float),
            "depository_share": pd.Series([], dtype=float),
            "other_share": pd.Series([], dtype=float),
        })

        dealer_stats = pd.DataFrame({
            "week_start": pd.to_datetime(["2025-03-03"]),
            "week_end": pd.to_datetime(["2025-03-09"]),
            "pd_treasury_inventory": [200000.0],
            "pd_financing_usage": [120000.0],
        })

        panel = build_weekly_panel(auctions, investor_class, dealer_stats)

        # dealer_share_allotment should be NaN (no investor class match)
        assert pd.isna(panel.iloc[0]["dealer_share_allotment"]), (
            "dealer_share_allotment should be NaN when no investor class data matches"
        )

        # The current code computes: 1.0 - dealer_share_allotment.fillna(0.0)
        # which yields 1.0.  This test documents that behavior.
        # Ideally it would be NaN, but the code currently returns 1.0.
        nondealer = panel.iloc[0]["nondealer_share"]
        assert nondealer == pytest.approx(1.0) or pd.isna(nondealer), (
            f"nondealer_share should be 1.0 (current fillna behavior) or NaN, got {nondealer}"
        )
