from __future__ import annotations

import json

import pandas as pd

from bidbridge.analysis.data_audit import build_data_audit, write_data_audit
from bidbridge.analysis.pressure_monitor import (
    build_upcoming_pressure_monitor,
    write_upcoming_pressure_monitor,
)


def test_data_audit_counts_unmatched_and_forward_filled(tmp_path):
    auctions = pd.DataFrame({
        "cusip": ["A", "B"],
        "issue_date": pd.to_datetime(["2025-01-09", "2025-01-16"]),
        "security_type": ["Note", "Bill"],
    })
    investor = pd.DataFrame({
        "cusip": ["A"],
        "issue_date": pd.to_datetime(["2025-01-09"]),
        "security_type": ["Note"],
        "dealer_share": [0.3],
    })
    dealer = pd.DataFrame({
        "week_start": pd.to_datetime(["2025-01-06", "2025-01-13"]),
        "pd_coupon_le2y": [1.0, 1.0],
        "pd_coupon_2_3y": [1.0, 1.0],
        "pd_coupon_3_6y": [1.0, 1.0],
        "pd_coupon_6_7y": [1.0, 1.0],
        "pd_coupon_7_11y": [1.0, 1.0],
        "pd_repo_treasury_raw": [10.0, None],
        "pd_reverse_repo_treasury_raw": [4.0, None],
    })

    audit = build_data_audit(auctions, investor, dealer)
    assert audit["unmatched_investor_rows"] == 1
    assert audit["financing_forward_fill_count"] == 1
    outputs = write_data_audit(auctions, investor, dealer, tmp_path)
    assert outputs["data_audit_csv"].exists()
    payload = json.loads(outputs["data_audit_json"].read_text())
    assert payload["headline_fe_eligible"] is True


def test_pressure_monitor_writes_expected_schema(tmp_path):
    panel = pd.DataFrame({
        "week_start": pd.date_range("2025-01-06", periods=20, freq="W-MON"),
        "announced_amount_total": [80_000.0 + i * 500 for i in range(20)],
        "bridge_episode": [0, 1] * 10,
        "weak_end_investor_absorption": [False, True] * 10,
    })
    upcoming = pd.DataFrame({
        "auction_date": pd.to_datetime(["2025-05-26", "2025-05-27", "2025-06-03"]),
        "issue_date": pd.to_datetime(["2025-05-29", "2025-05-29", "2025-06-05"]),
        "security_type": ["Bill", "Note", "Bill"],
        "security_term": ["26-Week", "10-Year", "13-Week"],
        "offering_amount": [55_000.0, 35_000.0, 25_000.0],
    })

    monitor = build_upcoming_pressure_monitor(panel, upcoming)
    assert {"composite_pressure_score", "pressure_category", "weeks_ahead"}.issubset(monitor.columns)
    outputs = write_upcoming_pressure_monitor(
        panel,
        upcoming,
        tmp_path / "monitor.csv",
        tmp_path / "monitor.json",
    )
    assert outputs["pressure_monitor_csv"].exists()
    payload = json.loads(outputs["pressure_monitor_json"].read_text())
    assert isinstance(payload, list)
    assert "composite_pressure_score" in payload[0]
