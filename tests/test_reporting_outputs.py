from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from bidbridge.analysis.panel_fe import generate_panel_fe_table
from bidbridge.analysis.promotion import (
    _placebo_overlap_summary,
    build_promotion_summary,
    write_promotion_artifacts,
)
from bidbridge.analysis.regressions import _ols_robust
from bidbridge.analysis.site_data import build_site_payload
from bidbridge.run_manifest import write_run_manifest


class _FakeResult:
    def __init__(self, beta: float):
        self.params = {"supply_B": beta, "lagged_dealer_share": 0.2, "supply_x_soft_demand": 0.1}
        self.std_errors = {"supply_B": 0.1, "lagged_dealer_share": 0.05, "supply_x_soft_demand": 0.04}
        self.nobs = 120
        self.rsquared = 0.25


def test_panel_fe_table_prefers_driscoll_kraay_labels(tmp_path):
    results = {
        "pooled_supply_only": _FakeResult(0.1),
        "bucket_fe_supply_only": _FakeResult(0.2),
        "twoway_fe_supply_only_driscoll_kraay": _FakeResult(0.3),
        "twoway_fe_strict_lag_driscoll_kraay": _FakeResult(0.4),
        "_metadata": {
            "headline_fe_eligible": True,
            "week_definition": "monday",
            "driscoll_kraay_kernel": "bartlett",
            "driscoll_kraay_bandwidth": 6,
            "strict_lag_driscoll_kraay_bandwidth": 6,
        },
    }
    out = generate_panel_fe_table(results, tmp_path)
    table = pd.read_csv(out)
    covariance = table[table["variable"] == "Covariance"]["coefficient"].tolist()
    assert "Driscoll-Kraay" in covariance
    assert "Clustered by bucket" in covariance


def test_write_run_manifest_records_artifacts(tmp_path):
    manifest = write_run_manifest(
        tmp_path / "run_manifest.json",
        repo_root=Path.cwd(),
        raw_inputs={"auctions": tmp_path / "raw.csv"},
        processed_outputs={"panel": tmp_path / "panel.csv"},
        analysis_outputs={"lp": tmp_path / "lp.csv"},
        audit_outputs={"audit": tmp_path / "audit.json"},
        extension_outputs={"pressure": tmp_path / "pressure.csv"},
        metadata={"panel_fe": {"headline_fe_eligible": True}},
    )
    payload = json.loads(manifest.read_text())
    assert "study_config" in payload
    assert payload["metadata"]["panel_fe"]["headline_fe_eligible"] is True


def test_promotion_artifacts_summarize_headline_evidence(tmp_path):
    tables = tmp_path / "tables"
    tables.mkdir()
    pd.DataFrame(
        [
            {
                "horizon": 0,
                "beta": 7200.0,
                "se": 1900.0,
                "t_stat": 3.8,
                "p_value": 0.001,
                "ci_lower": 3400.0,
                "ci_upper": 11000.0,
                "n_obs": 675,
                "r_squared": 0.12,
                "regime": "full_sample",
            },
            {
                "horizon": 0,
                "beta": 10800.0,
                "se": 2600.0,
                "t_stat": 4.0,
                "p_value": 0.001,
                "ci_lower": 5500.0,
                "ci_upper": 16000.0,
                "n_obs": 675,
                "r_squared": 0.12,
                "regime": "qt_period",
            },
            {
                "horizon": 0,
                "beta": 5200.0,
                "se": 2200.0,
                "t_stat": 2.4,
                "p_value": 0.02,
                "ci_lower": 900.0,
                "ci_upper": 9500.0,
                "n_obs": 675,
                "r_squared": 0.12,
                "regime": "non_qt_period",
            },
        ]
    ).to_csv(tables / "lp_results.csv", index=False)
    significant_placebos = {
        ("lead_placebo_h1", 0),
        ("lead_placebo_h1", 2),
        ("lead_placebo_h1", 6),
        ("lead_placebo_h1", 9),
        ("lead_placebo_h1", 11),
        ("shifted_placebo_h4", 3),
        ("shifted_placebo_h4", 4),
        ("shifted_placebo_h4", 5),
    }
    placebo_rows = [
        {
            "placebo_type": placebo_type,
            "horizon": horizon,
            "p_value": 0.001 if (placebo_type, horizon) in significant_placebos else 0.50,
        }
        for placebo_type in ("lead_placebo_h1", "shifted_placebo_h4")
        for horizon in range(13)
    ]
    pd.DataFrame(placebo_rows).to_csv(
        tables / "lp_placebo_results.csv", index=False,
    )
    fe_row = {
        "specification": "twoway_fe_supply_only_driscoll_kraay",
        "variable": "Announced supply ($B)",
        "coefficient": 10.6,
        "std_error": 4.1,
        "ci_lower": 2.5,
        "ci_upper": 18.7,
    }
    pd.DataFrame([fe_row]).to_csv(tables / "panel_fe_results.csv", index=False)
    pd.DataFrame([{**fe_row, "coefficient": 15.7, "ci_lower": 6.8, "ci_upper": 24.6}]).to_csv(
        tables / "panel_fe_results_thursday.csv", index=False,
    )
    pd.DataFrame(
        [
            {
                "variable": "Inventory change ($M)",
                "difference": 11400.0,
                "t_stat": 5.2,
                "p_value": 0.0,
            }
        ]
    ).to_csv(tables / "regression_refunding_test.csv", index=False)
    pd.DataFrame(
        [
            {"maturity_bucket": "bills", "dealer_share_mean": 0.62},
            {"maturity_bucket": "long_coupon", "dealer_share_mean": 0.27},
        ]
    ).to_csv(tables / "maturity_bucket_summary.csv", index=False)

    summary = build_promotion_summary(tables)
    assert set(summary["claim"]) >= {"lp_same_week_full_sample", "maturity_bucket_fe_thursday"}
    assert summary.loc[summary["claim"] == "lp_placebo_boundary", "status"].iloc[0] == "boundary"
    placebo_row = summary.loc[summary["claim"] == "lp_placebo_boundary"].iloc[0]
    assert placebo_row["estimate"] == "2"
    assert placebo_row["stat"] == "2 of 5 non-overlapping; 8 of 26 total; 21 overlapping"
    assert "does not provide clean timing validation" in placebo_row["interpretation"]
    lp_rows = summary[summary["claim"].str.startswith("lp_same_week_")]
    assert lp_rows["interpretation"].str.contains("high-supply indicator").all()
    assert not lp_rows["interpretation"].str.contains(
        "Offering amounts for auction weeks"
    ).any()

    outputs = write_promotion_artifacts(
        tables,
        tables / "bidbridge_promotion_summary.csv",
        tmp_path / "bidbridge_promotion_memo.md",
    )
    assert outputs["promotion_summary"].exists()
    memo = outputs["promotion_memo"].read_text(encoding="utf-8")
    assert "short-run aggregate primary-dealer warehousing association" in memo
    assert "binary high-supply indicator" in memo
    assert "not a continuous offering-amount slope" in memo
    assert "QT amplification" not in memo
    assert "timing-consistent" not in memo
    assert "2 of 5 non-overlapping lead/anticipation cells are significant" in memo
    assert "lead-1 h=0; lead-4 h=3" in memo
    assert "bridge between auction allotments and final holders" not in memo


def test_placebo_non_overlap_requires_outcome_horizon_before_treatment_lead():
    rows = pd.DataFrame(
        [
            {"placebo_type": "lead_placebo_h1", "horizon": 0, "p_value": 0.001},
            {"placebo_type": "lead_placebo_h1", "horizon": 1, "p_value": 0.001},
            {"placebo_type": "shifted_placebo_h4", "horizon": 3, "p_value": 0.001},
            {"placebo_type": "shifted_placebo_h4", "horizon": 4, "p_value": 0.001},
        ]
    )
    diagnostic = _placebo_overlap_summary(rows)
    assert diagnostic["non_overlapping_cells"] == 2
    assert diagnostic["overlapping_cells"] == 2
    assert diagnostic["significant_non_overlapping_labels"] == [
        "lead-1 h=0",
        "lead-4 h=3",
    ]


def test_build_site_payload_serializes_pressure_monitor(monkeypatch):
    monkeypatch.setattr(
        "bidbridge.analysis.site_data.run_extended_bridge_regression",
        lambda panel: pd.DataFrame(
            [
                {
                    "term": "supply_M",
                    "coefficient": 0.1,
                    "std_error": 0.02,
                    "t_stat": 5.0,
                    "p_value": 0.001,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "bidbridge.analysis.site_data.run_refunding_test",
        lambda panel: pd.DataFrame(columns=["variable", "refunding_mean", "ordinary_mean"]),
    )
    monkeypatch.setattr("bidbridge.analysis.site_data.get_source_registry", list)

    panel = pd.DataFrame(
        {
            "week_start": pd.to_datetime(["2025-01-06", "2025-01-13"]),
            "auction_count": [2, 1],
            "awarded_amount_total": [90_000_000_000.0, 95_000_000_000.0],
            "dealer_share_allotment": [0.31, 0.34],
            "pd_treasury_inventory": [42_000.0, 43_500.0],
            "inventory_change": [600.0, 900.0],
            "bridge_episode": [0, 1],
            "weighted_bid_to_cover": [2.5, 2.4],
            "refunding_week": [False, True],
        }
    )
    lp_results = {
        "full_sample": pd.DataFrame(
            {
                "horizon": [0],
                "beta": [7.3],
                "se": [1.0],
                "p_value": [0.001],
                "ci_lower": [5.0],
                "ci_upper": [9.6],
            }
        )
    }
    pressure_monitor = pd.DataFrame(
        {
            "week_start": pd.to_datetime(["2025-02-03"]),
            "weeks_ahead": [1],
            "total_offering_amount": [55_000_000_000.0],
            "supply_size_score": [0.82],
            "bill_share": [0.65],
            "recent_bridge_rate": [0.30],
            "recent_weak_demand_rate": [0.25],
            "composite_pressure_score": [0.58],
            "pressure_category": ["medium"],
        }
    )

    payload = build_site_payload(
        panel,
        lp_results,
        stress_summary=pd.DataFrame(),
        bridge_summary=pd.DataFrame(),
        pressure_monitor=pressure_monitor,
    )

    assert payload["pressure_monitor"][0]["pressure_category"] == "medium"
    assert payload["pressure_monitor"][0]["week_start"] == "2025-02-03"


def test_site_pressure_monitor_contract_present():
    repo_root = Path(__file__).resolve().parents[1]
    index_html = (repo_root / "site" / "index.html").read_text(encoding="utf-8")
    app_js = (repo_root / "site" / "app.js").read_text(encoding="utf-8")

    assert 'id="pressure-monitor"' in index_html
    assert 'id="pressure-monitor-empty"' in index_html
    assert "How the score and category work" in index_html
    assert "renderPressureMonitor" in app_js
    assert 'document.getElementById("pressure-monitor-body")' in app_js
    assert "metricConfig" in app_js


def test_ols_robust_handles_singular_design_matrix():
    y = np.array([1.0, 2.0, 3.0, 4.0])
    x = np.array(
        [
            [1.0, 1.0, 2.0],
            [1.0, 2.0, 4.0],
            [1.0, 3.0, 6.0],
            [1.0, 4.0, 8.0],
        ]
    )

    result = _ols_robust(y, x, ["intercept", "x", "x_dup"])

    assert list(result["term"]) == ["intercept", "x", "x_dup"]
    assert np.isfinite(result["coefficient"]).all()
