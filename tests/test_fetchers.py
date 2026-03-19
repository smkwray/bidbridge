"""Tests for the four data source fetchers and the pipeline build_panel.

All tests that hit external APIs are marked with @pytest.mark.network
so they can be skipped with:  pytest -m "not network"

Run all:
  PYTHONDONTWRITEBYTECODE=1 ~/venvs/bidbridge/bin/python -B -m pytest tests/test_fetchers.py -v
"""

from __future__ import annotations

import pandas as pd
import pytest
import requests

# ---------------------------------------------------------------------------
# Custom pytest mark for network-dependent tests
# ---------------------------------------------------------------------------
network = pytest.mark.network


def _api_reachable(url: str, timeout: int = 10) -> bool:
    """Return True if *url* responds within *timeout* seconds."""
    try:
        resp = requests.head(url, timeout=timeout, allow_redirects=True)
        return resp.status_code < 500
    except (requests.ConnectionError, requests.Timeout, OSError):
        return False


# ---------------------------------------------------------------------------
# Helpers shared across fetcher tests
# ---------------------------------------------------------------------------

def _read_csv(path):
    """Read a CSV written by a fetcher, returning the DataFrame."""
    assert path.exists(), f"Expected CSV not found: {path}"
    df = pd.read_csv(path)
    return df


# ===================================================================
# 1. Treasury Auctions
# ===================================================================

class TestTreasuryAuctions:
    """Tests for fetch_treasury_auctions and fetch_upcoming_auctions."""

    @network
    def test_fetch_treasury_auctions(self, tmp_path):
        """Fetch a small slice of completed auction data."""
        if not _api_reachable("https://api.fiscaldata.treasury.gov"):
            pytest.skip("Treasury FiscalData API is unreachable")

        from bidbridge.data.sources.treasury_auctions import fetch_treasury_auctions

        csv_path = fetch_treasury_auctions(tmp_path, start_date="2026-01-01")

        assert csv_path.exists()
        assert csv_path.name == "treasury_auctions.csv"

        df = _read_csv(csv_path)

        # Verify expected columns are present
        expected_cols = {
            "cusip", "auction_date", "security_type", "instrument_group",
            "bid_to_cover", "offering_amount",
        }
        assert expected_cols.issubset(set(df.columns)), (
            f"Missing columns: {expected_cols - set(df.columns)}"
        )

        # Rows should exist (there are auctions in 2026)
        assert len(df) > 0, "Expected at least one auction row"

        # Date range check — all auction_dates should be >= start_date
        df["auction_date"] = pd.to_datetime(df["auction_date"], errors="coerce")
        assert df["auction_date"].min() >= pd.Timestamp("2026-01-01"), (
            "Found auction_date earlier than start_date"
        )

        # Manifest file should also be written
        manifest_path = tmp_path / "treasury_auctions_manifest.json"
        assert manifest_path.exists()

    @network
    def test_fetch_upcoming_auctions(self, tmp_path):
        """Fetch upcoming/announced auctions."""
        if not _api_reachable("https://api.fiscaldata.treasury.gov"):
            pytest.skip("Treasury FiscalData API is unreachable")

        from bidbridge.data.sources.treasury_auctions import fetch_upcoming_auctions

        csv_path = fetch_upcoming_auctions(tmp_path)

        assert csv_path.exists()
        assert csv_path.name == "upcoming_auctions.csv"

        df = _read_csv(csv_path)

        expected_cols = {"cusip", "auction_date", "security_type", "security_term"}
        assert expected_cols.issubset(set(df.columns)), (
            f"Missing columns: {expected_cols - set(df.columns)}"
        )

        # Upcoming auctions may be an empty list (no upcoming auctions right now),
        # but the CSV should still be written with the correct schema.
        assert len(df.columns) >= 4


# ===================================================================
# 2. NY Fed Primary Dealer Statistics
# ===================================================================

class TestNYFedPrimaryDealer:
    """Tests for fetch_primary_dealer_statistics."""

    @network
    def test_fetch_primary_dealer_statistics(self, tmp_path):
        """Fetch a small slice of primary dealer data."""
        if not _api_reachable("https://markets.newyorkfed.org"):
            pytest.skip("NY Fed Markets API is unreachable")

        from bidbridge.data.sources.nyfed_pd import fetch_primary_dealer_statistics

        csv_path = fetch_primary_dealer_statistics(tmp_path, start_date="2026-01-01")

        assert csv_path.exists()
        assert csv_path.name == "primary_dealer_stats.csv"

        df = _read_csv(csv_path)

        expected_cols = {
            "as_of_date", "week_start", "week_end",
            "pd_treasury_inventory", "pd_financing_usage",
        }
        assert expected_cols.issubset(set(df.columns)), (
            f"Missing columns: {expected_cols - set(df.columns)}"
        )

        assert len(df) > 0, "Expected at least one dealer-stat row"

        # Date range check
        df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
        assert df["as_of_date"].min() >= pd.Timestamp("2026-01-01"), (
            "Found as_of_date earlier than start_date"
        )

        # Manifest file should also be written
        manifest_path = tmp_path / "primary_dealer_stats_manifest.json"
        assert manifest_path.exists()


# ===================================================================
# 3. Treasury Investor Class Allotments
# ===================================================================

class TestInvestorClassAllotments:
    """Tests for fetch_investor_class_allotments."""

    @network
    def test_fetch_investor_class_allotments(self, tmp_path):
        """Fetch investor class allotment data."""
        if not _api_reachable("https://home.treasury.gov"):
            pytest.skip("Treasury.gov is unreachable")

        from bidbridge.data.sources.treasury_investor_class import (
            fetch_investor_class_allotments,
        )

        csv_path = fetch_investor_class_allotments(
            tmp_path, start_date="2026-01-01",
        )

        assert csv_path.exists()
        assert csv_path.name == "investor_class_allotments.csv"

        df = _read_csv(csv_path)

        expected_cols = {
            "issue_date", "security_type",
            "dealer_share", "foreign_share",
        }
        assert expected_cols.issubset(set(df.columns)), (
            f"Missing columns: {expected_cols - set(df.columns)}"
        )

        assert len(df) > 0, "Expected at least one allotment row"

        # Date range check
        df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
        assert df["issue_date"].min() >= pd.Timestamp("2026-01-01"), (
            "Found issue_date earlier than start_date"
        )

        # Shares should be between 0 and 1 (they are fractions)
        share_cols = [c for c in df.columns if c.endswith("_share")]
        for col in share_cols:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(vals) > 0:
                assert vals.min() >= 0.0, f"{col} has negative values"
                assert vals.max() <= 1.01, f"{col} has values > 1"

        manifest_path = tmp_path / "investor_class_allotments_manifest.json"
        assert manifest_path.exists()


# ===================================================================
# 4. SOMA Holdings
# ===================================================================

class TestSOMAHoldings:
    """Tests for fetch_soma_holdings."""

    @network
    def test_fetch_soma_holdings(self, tmp_path):
        """Fetch a small slice of SOMA holdings."""
        if not _api_reachable("https://markets.newyorkfed.org"):
            pytest.skip("NY Fed Markets API is unreachable")

        from bidbridge.data.sources.soma import fetch_soma_holdings

        csv_path = fetch_soma_holdings(tmp_path, start_date="2026-01-01")

        assert csv_path.exists()
        assert csv_path.name == "soma_holdings.csv"

        df = _read_csv(csv_path)

        expected_cols = {
            "as_of_date", "week_start", "week_end",
            "soma_bills", "soma_notes_bonds", "soma_treasury_total",
        }
        assert expected_cols.issubset(set(df.columns)), (
            f"Missing columns: {expected_cols - set(df.columns)}"
        )

        assert len(df) > 0, "Expected at least one SOMA row"

        # Date range check
        df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
        assert df["as_of_date"].min() >= pd.Timestamp("2026-01-01"), (
            "Found as_of_date earlier than start_date"
        )

        # SOMA total should be positive (trillions in par value)
        totals = pd.to_numeric(df["soma_treasury_total"], errors="coerce").dropna()
        if len(totals) > 0:
            assert totals.min() > 0, "SOMA treasury total should be positive"

        manifest_path = tmp_path / "soma_holdings_manifest.json"
        assert manifest_path.exists()


# ===================================================================
# 5. Pipeline — build_panel with synthetic pre-fetched data
# ===================================================================

class TestBuildPanel:
    """Test build_panel using locally constructed CSV files (no network)."""

    @pytest.fixture()
    def raw_dir(self, tmp_path):
        """Create a raw_dir with minimal synthetic CSVs that mimic fetcher output."""
        treasury_dir = tmp_path / "raw" / "treasury"
        nyfed_dir = tmp_path / "raw" / "nyfed"
        treasury_dir.mkdir(parents=True)
        nyfed_dir.mkdir(parents=True)

        # -- Auction data (treasury_auctions.csv) --
        auctions = pd.DataFrame({
            "cusip": ["912797AA1", "912828ZZ0", "912810TT5"],
            "auction_date": pd.to_datetime(
                ["2025-01-06", "2025-01-07", "2025-01-08"]
            ),
            "issue_date": pd.to_datetime(
                ["2025-01-09", "2025-01-10", "2025-01-10"]
            ),
            "maturity_date": pd.to_datetime(
                ["2025-04-10", "2027-01-15", "2045-02-15"]
            ),
            "security_type": ["Bill", "Note", "Bond"],
            "security_term": ["13-Week", "2-Year", "20-Year"],
            "instrument_group": ["bills", "nominal_coupons", "bonds"],
            "offering_amount": [80000.0, 60000.0, 16000.0],
            "announced_amount": [80000.0, 60000.0, 16000.0],
            "awarded_amount": [80000.0, 60000.0, 16000.0],
            "bid_to_cover": [2.8, 2.5, 2.3],
            "high_yield": [4.5, 4.2, 4.8],
            "tail_bp": [0.5, 0.3, 1.0],
            "primary_dealer_accepted": [40000.0, 30000.0, 8000.0],
            "direct_bidder_accepted": [10000.0, 15000.0, 4000.0],
            "indirect_bidder_accepted": [30000.0, 15000.0, 4000.0],
            "reopening": [False, False, False],
            "is_tips": [False, False, False],
            "is_frn": [False, False, False],
            "is_cmb": [False, False, False],
        })
        auctions.to_csv(treasury_dir / "treasury_auctions.csv", index=False)

        # -- Investor class allotments --
        investor = pd.DataFrame({
            "issue_date": pd.to_datetime(
                ["2025-01-09", "2025-01-10", "2025-01-10"]
            ),
            "security_type": ["Bill", "Note", "Bond"],
            "cusip": ["912797AA1", "912828ZZ0", "912810TT5"],
            "total_issue_amount": [80000.0, 60000.0, 16000.0],
            "dealer_share": [0.35, 0.30, 0.25],
            "investment_funds_share": [0.20, 0.25, 0.30],
            "foreign_share": [0.25, 0.20, 0.20],
            "depository_share": [0.10, 0.15, 0.15],
            "other_share": [0.10, 0.10, 0.10],
        })
        investor.to_csv(
            treasury_dir / "investor_class_allotments.csv", index=False,
        )

        # -- Primary dealer stats --
        dealer = pd.DataFrame({
            "as_of_date": pd.to_datetime(["2025-01-08"]),
            "week_start": pd.to_datetime(["2025-01-06"]),
            "week_end": pd.to_datetime(["2025-01-12"]),
            "pd_treasury_inventory": [200000.0],
            "pd_bills_position": [80000.0],
            "pd_coupon_position": [100000.0],
            "pd_coupon_le2y": [15000.0],
            "pd_coupon_2_3y": [15000.0],
            "pd_coupon_3_6y": [20000.0],
            "pd_coupon_6_7y": [10000.0],
            "pd_coupon_7_11y": [15000.0],
            "pd_coupon_gt11y": [25000.0],
            "pd_coupon_11_21y": [15000.0],
            "pd_coupon_gt21y": [10000.0],
            "pd_tips_position": [10000.0],
            "pd_frn_position": [10000.0],
            "pd_repo_treasury": [150000.0],
            "pd_reverse_repo_treasury": [30000.0],
            "pd_financing_usage": [120000.0],
        })
        dealer.to_csv(nyfed_dir / "primary_dealer_stats.csv", index=False)

        # -- SOMA holdings --
        # as_of_date is the Wednesday observation. With the 1-week lag in build_panel,
        # this maps to the NEXT Monday = 2025-01-06, matching the auction week.
        soma = pd.DataFrame({
            "as_of_date": pd.to_datetime(["2025-01-01"]),
            "week_start": pd.to_datetime(["2024-12-30"]),
            "week_end": pd.to_datetime(["2025-01-05"]),
            "soma_bills": [300000.0],
            "soma_notes_bonds": [4000000.0],
            "soma_tips": [400000.0],
            "soma_frn": [0.0],
            "soma_tips_inflation_comp": [50000.0],
            "soma_treasury_total": [4700000.0],
            "soma_mbs": [2500000.0],
            "soma_agencies": [2000.0],
            "soma_total": [7202000.0],
        })
        soma.to_csv(nyfed_dir / "soma_holdings.csv", index=False)

        return tmp_path / "raw"

    def test_build_panel_from_synthetic_data(self, raw_dir, tmp_path):
        """build_panel should produce a weekly panel from pre-fetched CSVs."""
        from bidbridge.data.pipeline import build_panel

        output_path = tmp_path / "panel.csv"
        result = build_panel(
            raw_dir=raw_dir,
            output_path=output_path,
            start_date="2025-01-01",
        )

        assert result.exists()
        panel = pd.read_csv(result)

        # Should have at least one week
        assert len(panel) >= 1

        # Core columns from build_weekly_panel + bridge_metrics
        expected_cols = {
            "week_start", "week_end", "auction_count",
            "awarded_amount_total", "weighted_bid_to_cover",
        }
        assert expected_cols.issubset(set(panel.columns)), (
            f"Missing columns: {expected_cols - set(panel.columns)}"
        )

        # Dealer stats should have merged in
        assert "pd_treasury_inventory" in panel.columns

        # SOMA data should have merged in
        assert "soma_treasury_total" in panel.columns

        # Sanity: the single week should have all 3 auctions
        assert panel.iloc[0]["auction_count"] == 3

    def test_build_panel_merges_soma(self, raw_dir, tmp_path):
        """Verify SOMA data merges correctly onto the panel."""
        from bidbridge.data.pipeline import build_panel

        output_path = tmp_path / "panel_soma.csv"
        result = build_panel(
            raw_dir=raw_dir,
            output_path=output_path,
            start_date="2025-01-01",
        )

        panel = pd.read_csv(result)
        soma_val = pd.to_numeric(panel["soma_treasury_total"], errors="coerce")
        non_null = soma_val.dropna()
        assert len(non_null) >= 1, "SOMA data should merge onto at least one week"
        assert non_null.iloc[0] == pytest.approx(4700000.0)
