from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from bidbridge.analysis.panel_fe import generate_panel_fe_table
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
        "pooled": _FakeResult(0.1),
        "bucket_fe": _FakeResult(0.2),
        "twoway_fe_driscoll_kraay": _FakeResult(0.3),
        "interaction_driscoll_kraay": _FakeResult(0.4),
        "_metadata": {
            "headline_fe_eligible": True,
            "week_definition": "monday",
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
    monkeypatch.setattr("bidbridge.analysis.site_data.get_source_registry", lambda: [])

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
