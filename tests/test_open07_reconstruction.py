from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bidbridge.analysis.maturity_summary import build_maturity_bucket_summary
from bidbridge.analysis.panel_fe import (
    _driscoll_kraay_bandwidth,
    _strict_lag_matches_calendar_week,
)
from bidbridge.open07_reconstruction import FIELDS, reconstruct
from bidbridge.open07_reproduce import (
    _estimation_rows,
    _strict_lag_estimation_rows,
    _versioned_path,
)


def _source_row(**updates: str) -> dict[str, str]:
    row = {name: "" for name in FIELDS.split(",")}
    row.update(
        {
            "cusip": "912TEST01",
            "auction_date": "2025-01-06",
            "issue_date": "2025-01-09",
            "maturity_date": "2025-04-10",
            "announcemt_date": "2025-01-02",
            "security_type": "Bill",
            "security_term": "13-Week",
            "offering_amt": "70000000000",
            "total_accepted": "70000100000",
            "bid_to_cover_ratio": "2.75",
            "high_discnt_rate": "4.1",
            "avg_med_discnt_rate": "4.08",
            "high_investment_rate": "4.2",
            "primary_dealer_accepted": "21000000000",
            "direct_bidder_accepted": "7000000000",
            "indirect_bidder_accepted": "35000000000",
            "reopening": "No",
            "inflation_index_security": "No",
            "floating_rate": "No",
            "cash_management_bill_cmb": "No",
        }
    )
    row.update(updates)
    return row


def test_reconstruction_requires_corroborating_equivalence(tmp_path: Path) -> None:
    primary = tmp_path / "primary.csv"
    corroborating = tmp_path / "corroborating.csv"
    metadata = tmp_path / "meta.json"
    pd.DataFrame([_source_row()]).to_csv(primary, index=False, lineterminator="\n")
    pd.DataFrame([_source_row(offering_amt="71000000000")]).to_csv(
        corroborating, index=False, lineterminator="\n"
    )
    metadata.write_text(
        json.dumps({"endpoint": "v1/accounting/od/auctions_query"}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="differ over the frozen domain"):
        reconstruct(
            primary_path=primary,
            metadata_path=metadata,
            corroborating_path=corroborating,
            output_path=tmp_path / "normalized.csv",
            manifest_path=tmp_path / "manifest.json",
        )


def test_reconstruction_is_bounded_and_deterministic(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    corroborating = tmp_path / "corroborating.csv"
    metadata = tmp_path / "meta.json"
    rows = [
        _source_row(),
        _source_row(cusip="912INCOMP", bid_to_cover_ratio=""),
        _source_row(cusip="912LATER", auction_date="2026-03-23"),
    ]
    pd.DataFrame(rows).to_csv(source, index=False, lineterminator="\n")
    pd.DataFrame(rows).to_csv(corroborating, index=False, lineterminator="\n")
    metadata.write_text(
        json.dumps(
            {
                "endpoint": "v1/accounting/od/auctions_query",
                "params": {"sort": "-auction_date"},
                "pages_fetched": 1,
                "rows_fetched": 3,
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "normalized.csv"
    manifest = tmp_path / "manifest.json"
    _, _, payload = reconstruct(
        primary_path=source,
        metadata_path=metadata,
        corroborating_path=corroborating,
        output_path=output,
        manifest_path=manifest,
    )

    normalized = pd.read_csv(output)
    assert normalized["cusip"].tolist() == ["912TEST01"]
    assert normalized.loc[0, "instrument_group"] == "bills"
    assert normalized.loc[0, "tail_bp"] == pytest.approx(2.0)
    assert payload["producer_input_identity_changed"] is True
    assert payload["original_normalized_input_sha256"] is None
    assert payload["network_refetch_used"] is False


def test_maturity_summary_and_versioned_paths_are_stable() -> None:
    panel = pd.DataFrame(
        [
            {
                "maturity_bucket": "bills",
                "auction_count": 2,
                "awarded_amount": 10.0,
                "dealer_share": 0.6,
                "weighted_bid_to_cover": 2.5,
                "weighted_tail_bp": 1.0,
                "bucket_share_of_weekly": 0.8,
            },
            {
                "maturity_bucket": "bills",
                "auction_count": 1,
                "awarded_amount": 20.0,
                "dealer_share": 0.4,
                "weighted_bid_to_cover": 3.0,
                "weighted_tail_bp": 2.0,
                "bucket_share_of_weekly": 0.7,
            },
        ]
    )
    summary = build_maturity_bucket_summary(panel)
    assert summary.loc[0, "auction_count_sum"] == 3
    assert summary.loc[0, "dealer_share_mean"] == 0.5
    assert _versioned_path("outputs/tables/lp_results.csv") == (
        "outputs/open07_v5/tables/lp_results.csv"
    )


def test_primary_estimation_rows_are_supply_only() -> None:
    outcomes = pd.DataFrame(
        [
            {
                "maturity_bucket": "bills",
                "week_start": "2025-01-06",
                "delta_position": 5.0,
                "announced_amount": 2_000_000_000.0,
                "lagged_dealer_share": None,
                "dealer_share": 0.5,
                "lagged_soft_demand": None,
            }
        ]
    )
    rows = _estimation_rows(outcomes)
    assert rows.loc[0, "supply_B"] == 2.0
    assert list(rows.columns) == [
        "maturity_bucket",
        "week_start",
        "delta_position",
        "supply_B",
    ]


def test_strict_lag_rows_never_substitute_current_share() -> None:
    outcomes = pd.DataFrame(
        [
            {
                "maturity_bucket": "bills",
                "week_start": "2025-01-06",
                "delta_position": 5.0,
                "announced_amount": 2_000_000_000.0,
                "lagged_dealer_share": None,
                "dealer_share": 0.5,
            },
            {
                "maturity_bucket": "bills",
                "week_start": "2025-01-13",
                "delta_position": 6.0,
                "announced_amount": 3_000_000_000.0,
                "lagged_dealer_share": 0.5,
                "dealer_share": 0.6,
            },
        ]
    )
    rows = _strict_lag_estimation_rows(outcomes)
    assert len(rows) == 1
    assert rows.loc[0, "week_start"] == "2025-01-13"
    assert rows.loc[0, "lagged_dealer_share"] == 0.5


def test_strict_lag_validator_requires_exact_previous_calendar_week() -> None:
    panel = pd.DataFrame(
        [
            {
                "maturity_bucket": "bills",
                "week_start": pd.Timestamp("2025-01-06"),
                "dealer_share": 0.5,
                "lagged_dealer_share": None,
            },
            {
                "maturity_bucket": "bills",
                "week_start": pd.Timestamp("2025-01-13"),
                "dealer_share": 0.6,
                "lagged_dealer_share": 0.5,
            },
            {
                "maturity_bucket": "bills",
                "week_start": pd.Timestamp("2025-01-27"),
                "dealer_share": 0.7,
                "lagged_dealer_share": None,
            },
        ]
    )
    assert _strict_lag_matches_calendar_week(panel) is True
    panel.loc[2, "lagged_dealer_share"] = 0.6
    assert _strict_lag_matches_calendar_week(panel) is False
    assert _driscoll_kraay_bandwidth(675) == 6
