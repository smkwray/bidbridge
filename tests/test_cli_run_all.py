from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bidbridge import cli
import bidbridge.paths as paths


class _FakeResult:
    def __init__(self, beta: float):
        self.params = {
            "supply_B": beta,
            "lagged_dealer_share": 0.2,
            "supply_x_soft_demand": 0.1,
        }
        self.std_errors = {
            "supply_B": 0.1,
            "lagged_dealer_share": 0.05,
            "supply_x_soft_demand": 0.04,
        }
        self.nobs = 48
        self.rsquared = 0.25


def _write_text(path: Path, content: str = "ok\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _write_csv(path: Path, rows: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _configure_temp_paths(monkeypatch, tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw"
    processed_dir = tmp_path / "data" / "processed"
    outputs_dir = tmp_path / "outputs"
    figures_dir = outputs_dir / "figures"
    tables_dir = outputs_dir / "tables"
    site_dir = tmp_path / "site"
    site_data_dir = site_dir / "data"

    monkeypatch.setattr(paths, "RAW_DIR", raw_dir)
    monkeypatch.setattr(paths, "PROCESSED_DIR", processed_dir)
    monkeypatch.setattr(paths, "OUTPUTS_DIR", outputs_dir)
    monkeypatch.setattr(paths, "FIGURES_DIR", figures_dir)
    monkeypatch.setattr(paths, "TABLES_DIR", tables_dir)
    monkeypatch.setattr(paths, "SITE_DIR", site_dir)
    monkeypatch.setattr(paths, "SITE_DATA_DIR", site_data_dir)
    monkeypatch.setattr(cli, "SITE_DATA_DIR", site_data_dir)

    for directory in (raw_dir, processed_dir, outputs_dir, figures_dir, tables_dir, site_dir, site_data_dir):
        directory.mkdir(parents=True, exist_ok=True)


def _install_run_all_stubs(monkeypatch, fe_eligible: bool) -> None:
    def fake_fetch_all(raw_dir: Path | None = None, start_date: str = "2010-01-01", max_age_days=None):
        base = raw_dir or paths.RAW_DIR
        treasury_dir = base / "treasury"
        nyfed_dir = base / "nyfed"
        treasury_dir.mkdir(parents=True, exist_ok=True)
        nyfed_dir.mkdir(parents=True, exist_ok=True)

        auctions_path = _write_csv(
            treasury_dir / "treasury_auctions.csv",
            [
                {
                    "auction_date": "2025-01-06",
                    "issue_date": "2025-01-09",
                    "maturity_date": "2027-01-09",
                    "security_type": "Note",
                    "security_term": "2-Year",
                    "instrument_group": "nominal_coupons",
                    "announced_amount": 120000.0,
                    "awarded_amount": 118000.0,
                    "bid_to_cover": 2.5,
                    "tail_bp": 0.3,
                    "cusip": "AAA111111",
                },
                {
                    "auction_date": "2025-01-13",
                    "issue_date": "2025-01-16",
                    "maturity_date": "2025-07-16",
                    "security_type": "Bill",
                    "security_term": "26-Week",
                    "instrument_group": "bills",
                    "announced_amount": 90000.0,
                    "awarded_amount": 90000.0,
                    "bid_to_cover": 3.1,
                    "tail_bp": 0.0,
                    "cusip": "BBB222222",
                },
            ],
        )
        investor_path = _write_csv(
            treasury_dir / "investor_class_allotments.csv",
            [
                {
                    "issue_date": "2025-01-09",
                    "security_type": "Note",
                    "cusip": "AAA111111",
                    "dealer_share": 0.35,
                    "investment_funds_share": 0.2,
                    "foreign_share": 0.2,
                    "depository_share": 0.1,
                    "other_share": 0.15,
                }
            ],
        )
        dealer_path = _write_csv(
            nyfed_dir / "primary_dealer_stats.csv",
            [
                {
                    "as_of_date": "2025-01-08",
                    "week_start": "2025-01-06",
                    "week_end": "2025-01-12",
                    "pd_treasury_inventory": 41000.0,
                    "pd_financing_usage": 22000.0,
                },
                {
                    "as_of_date": "2025-01-15",
                    "week_start": "2025-01-13",
                    "week_end": "2025-01-19",
                    "pd_treasury_inventory": 45500.0,
                    "pd_financing_usage": 23800.0,
                },
            ],
        )
        return {
            "auctions": auctions_path,
            "investor_class": investor_path,
            "dealer_stats": dealer_path,
        }

    def fake_fetch_upcoming_auctions(output_dir: Path):
        return _write_csv(
            output_dir / "upcoming_auctions.csv",
            [
                {
                    "auction_date": "2025-02-03",
                    "issue_date": "2025-02-06",
                    "security_type": "Bill",
                    "security_term": "13-Week",
                    "offering_amount": 55000.0,
                },
                {
                    "auction_date": "2025-02-10",
                    "issue_date": "2025-02-13",
                    "security_type": "Note",
                    "security_term": "10-Year",
                    "offering_amount": 42000.0,
                },
            ],
        )

    def fake_build_panel(raw_dir: Path | None = None, output_path: Path | None = None, start_date: str = "2010-01-01", week_definition: str | None = None):
        output = output_path or (paths.PROCESSED_DIR / "auction_week_panel.csv")
        panel_rows = []
        week_starts = pd.date_range("2025-01-06", periods=6, freq="W-MON")
        for idx, week_start in enumerate(week_starts):
            panel_rows.append(
                {
                    "week_start": week_start.strftime("%Y-%m-%d"),
                    "week_end": (week_start + pd.Timedelta(days=6)).strftime("%Y-%m-%d"),
                    "auction_count": 1 + (idx % 2),
                    "announced_amount_total": 95_000_000_000 + idx * 2_500_000_000,
                    "awarded_amount_total": 92_000_000_000 + idx * 2_000_000_000,
                    "dealer_share_allotment": 0.28 + idx * 0.015,
                    "pd_treasury_inventory": 40_000 + idx * 1_250,
                    "inventory_change": 600 + idx * 80,
                    "refunding_week": idx % 3 == 0,
                    "bridge_episode": idx % 2,
                    "weak_end_investor_absorption": idx % 2 == 1,
                    "weighted_bid_to_cover": 2.45 + idx * 0.06,
                    "weighted_tail_bp": 0.1 + idx * 0.02,
                    "financing_intensity": 0.42 + idx * 0.03,
                }
            )
        _write_csv(output, panel_rows)
        _write_csv(
            paths.PROCESSED_DIR / "maturity_bucket_panel.csv",
            [{"week_start": "2025-01-06", "maturity_bucket": "bills", "announced_amount": 10.0}],
        )
        _write_csv(
            paths.PROCESSED_DIR / "maturity_wide_panel.csv",
            [{"week_start": "2025-01-06", "bills_announced_amount": 10.0}],
        )
        return output

    def fake_finalize_primary_dealer_dataframe(dealer_raw: pd.DataFrame, week_definition: str = "monday"):
        rows = [
            {
                "week_start": pd.Timestamp("2025-01-06"),
                "week_end": pd.Timestamp("2025-01-12"),
                "pd_treasury_inventory": 41000.0,
                "pd_financing_usage": 22000.0,
                "pd_repo_treasury_raw": 15.0,
                "pd_reverse_repo_treasury_raw": 6.0,
                "pd_bills_position": 8.0,
                "pd_tips_position": 3.0,
                "pd_frn_position": 2.0,
            },
            {
                "week_start": pd.Timestamp("2025-01-13"),
                "week_end": pd.Timestamp("2025-01-19"),
                "pd_treasury_inventory": 45500.0,
                "pd_financing_usage": 23800.0,
                "pd_repo_treasury_raw": None,
                "pd_reverse_repo_treasury_raw": None,
                "pd_bills_position": 9.0,
                "pd_tips_position": 3.5,
                "pd_frn_position": 2.1,
            },
        ]
        if fe_eligible:
            for row in rows:
                row.update(
                    {
                        "pd_coupon_le2y": 1.0,
                        "pd_coupon_2_3y": 1.1,
                        "pd_coupon_3_6y": 1.2,
                        "pd_coupon_6_7y": 1.3,
                        "pd_coupon_7_11y": 1.4,
                        "pd_coupon_11_21y": 1.5,
                        "pd_coupon_gt21y": 1.6,
                    }
                )
        return pd.DataFrame(rows)

    def fake_run_all_analysis(panel_path: Path):
        return {"timeseries_figure": _write_text(paths.FIGURES_DIR / "timeseries.png")}

    def fake_run_local_projections_by_regime(panel: pd.DataFrame):
        base = pd.DataFrame(
            {
                "horizon": [0, 1, 2],
                "beta": [7.3, 4.0, 2.0],
                "se": [1.0, 1.0, 1.0],
                "p_value": [0.001, 0.01, 0.1],
                "ci_lower": [5.0, 2.0, -0.5],
                "ci_upper": [9.6, 6.0, 4.5],
            }
        )
        return {
            "full_sample": base,
            "qt_period": base.assign(beta=[12.0, 6.0, 3.0]),
            "non_qt_period": base.assign(beta=[5.0, 3.0, 1.0]),
        }

    def fake_generate_lp_figures(results: dict[str, pd.DataFrame], output_dir: Path):
        return {"lp_irf": _write_text(output_dir / "lp_irf.png")}

    def fake_generate_shock_distribution_figure(panel: pd.DataFrame, output_dir: Path):
        return _write_text(output_dir / "shock_distribution.png")

    def fake_generate_lp_table(results: dict[str, pd.DataFrame], output_dir: Path):
        return _write_csv(output_dir / "lp_results.csv", [{"specification": "full_sample", "beta": 7.3}])

    def fake_run_local_projection_placebos(panel: pd.DataFrame):
        return {"lead_shock": pd.DataFrame([{"horizon": 0, "beta": 0.0}])}

    def fake_generate_lp_placebo_table(placebo: dict[str, pd.DataFrame], output_dir: Path):
        return _write_csv(
            output_dir / "lp_placebo_results.csv",
            [{"placebo": "lead_shock", "beta": 0.0}],
        )

    def fake_build_maturity_panel(auctions: pd.DataFrame, investor: pd.DataFrame, week_definition: str = "monday"):
        return pd.DataFrame(
            {
                "week_start": pd.to_datetime(["2025-01-06", "2025-01-13"]),
                "maturity_bucket": ["bills", "short_coupon"],
                "announced_amount": [10.0, 11.0],
                "awarded_amount": [10.0, 10.5],
                "dealer_share": [0.35, 0.4],
                "weighted_bid_to_cover": [2.4, 2.5],
                "refunding_week": [False, False],
            }
        )

    def fake_build_bucket_outcomes(maturity_panel: pd.DataFrame, dealer_stats: pd.DataFrame, headline_strict: bool = False, week_definition: str = "monday"):
        if headline_strict and not fe_eligible:
            raise ValueError("Granular coupon band columns are required for headline FE outputs.")
        panel = pd.DataFrame(
            {
                "week_start": pd.to_datetime(["2025-01-06", "2025-01-13"]),
                "maturity_bucket": ["bills", "short_coupon"],
                "delta_position": [0.4, 0.5],
                "announced_amount": [10.0, 11.0],
                "lagged_dealer_share": [0.35, 0.4],
                "lagged_soft_demand": [0, 1],
            }
        )
        panel.attrs["headline_fe_eligible"] = fe_eligible
        panel.attrs["week_definition"] = week_definition
        return panel

    def fake_run_bucket_fe_regression(panel: pd.DataFrame):
        return {
            "pooled": _FakeResult(0.1),
            "bucket_fe": _FakeResult(0.2),
            "twoway_fe_driscoll_kraay": _FakeResult(0.3),
            "interaction_driscoll_kraay": _FakeResult(0.4),
            "twoway_fe": _FakeResult(0.25),
            "interaction": _FakeResult(0.35),
            "_metadata": {
                "headline_fe_eligible": bool(panel.attrs.get("headline_fe_eligible", False)),
                "week_definition": panel.attrs.get("week_definition", "monday"),
            },
        }

    def fake_generate_panel_fe_figures(results: dict[str, object], panel: pd.DataFrame, output_dir: Path, file_prefix: str = "panel_fe"):
        return {f"{file_prefix}_figure": _write_text(output_dir / f"{file_prefix}.png")}

    def fake_generate_panel_fe_table(results: dict[str, object], output_dir: Path, file_name: str = "panel_fe_results.csv"):
        return _write_csv(
            output_dir / file_name,
            [
                {
                    "specification": "twoway_fe_driscoll_kraay",
                    "variable": "Covariance",
                    "coefficient": "Driscoll-Kraay",
                }
            ],
        )

    def fake_generate_persistence_figures(panel: pd.DataFrame, output_dir: Path):
        return {"persistence_figure": _write_text(output_dir / "persistence.png")}

    def fake_generate_persistence_table(panel: pd.DataFrame, output_dir: Path):
        return _write_csv(output_dir / "persistence_summary.csv", [{"year": 2025, "episodes": 2}])

    def fake_add_stress_flags(panel: pd.DataFrame):
        return panel.assign(qt_period=False)

    def fake_generate_stress_figures(panel: pd.DataFrame, output_dir: Path):
        return {"stress_figure": _write_text(output_dir / "stress.png")}

    def fake_generate_stress_table(panel: pd.DataFrame, output_dir: Path):
        return _write_csv(
            output_dir / "stress_regime_summary.csv",
            [{"stress_flag": "qt_period", "bridge_rate_flagged": 0.2, "bridge_rate_unflagged": 0.1}],
        )

    def fake_write_site_data(panel: pd.DataFrame, lp_results, stress_summary: pd.DataFrame, bridge_summary: pd.DataFrame, output_path: str | Path, pressure_monitor: pd.DataFrame | None = None):
        payload = {
            "panel_stats": {"total_weeks": int(len(panel))},
            "pressure_monitor": [] if pressure_monitor is None else pressure_monitor.assign(week_start=pressure_monitor["week_start"].astype(str)).to_dict(orient="records"),
        }
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return output

    monkeypatch.setattr("bidbridge.data.pipeline.fetch_all", fake_fetch_all)
    monkeypatch.setattr("bidbridge.data.pipeline.build_panel", fake_build_panel)
    monkeypatch.setattr("bidbridge.data.sources.treasury_auctions.fetch_upcoming_auctions", fake_fetch_upcoming_auctions)
    monkeypatch.setattr("bidbridge.data.sources.nyfed_pd.finalize_primary_dealer_dataframe", fake_finalize_primary_dealer_dataframe)
    monkeypatch.setattr("bidbridge.analysis.outputs.run_all_analysis", fake_run_all_analysis)
    monkeypatch.setattr("bidbridge.analysis.local_projections.run_local_projections_by_regime", fake_run_local_projections_by_regime)
    monkeypatch.setattr("bidbridge.analysis.local_projections.generate_lp_figures", fake_generate_lp_figures)
    monkeypatch.setattr("bidbridge.analysis.local_projections.generate_shock_distribution_figure", fake_generate_shock_distribution_figure)
    monkeypatch.setattr("bidbridge.analysis.local_projections.generate_lp_table", fake_generate_lp_table)
    monkeypatch.setattr("bidbridge.analysis.local_projections.run_local_projection_placebos", fake_run_local_projection_placebos)
    monkeypatch.setattr("bidbridge.analysis.local_projections.generate_lp_placebo_table", fake_generate_lp_placebo_table)
    monkeypatch.setattr("bidbridge.features.maturity_panel.build_maturity_panel", fake_build_maturity_panel)
    monkeypatch.setattr("bidbridge.analysis.panel_fe.build_bucket_outcomes", fake_build_bucket_outcomes)
    monkeypatch.setattr("bidbridge.analysis.panel_fe.run_bucket_fe_regression", fake_run_bucket_fe_regression)
    monkeypatch.setattr("bidbridge.analysis.panel_fe.generate_panel_fe_figures", fake_generate_panel_fe_figures)
    monkeypatch.setattr("bidbridge.analysis.panel_fe.generate_panel_fe_table", fake_generate_panel_fe_table)
    monkeypatch.setattr("bidbridge.analysis.persistence.generate_persistence_figures", fake_generate_persistence_figures)
    monkeypatch.setattr("bidbridge.analysis.persistence.generate_persistence_table", fake_generate_persistence_table)
    monkeypatch.setattr("bidbridge.features.stress_flags.add_stress_flags", fake_add_stress_flags)
    monkeypatch.setattr("bidbridge.features.stress_flags.generate_stress_figures", fake_generate_stress_figures)
    monkeypatch.setattr("bidbridge.features.stress_flags.generate_stress_table", fake_generate_stress_table)
    monkeypatch.setattr("bidbridge.analysis.site_data.write_site_data", fake_write_site_data)


def test_run_all_writes_full_artifact_graph(monkeypatch, tmp_path):
    _configure_temp_paths(monkeypatch, tmp_path)
    _install_run_all_stubs(monkeypatch, fe_eligible=True)

    rc = cli.main(["run-all", "--start-date", "2025-01-01"])
    assert rc == 0

    manifest_path = paths.OUTPUTS_DIR / "run_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert (paths.PROCESSED_DIR / "auction_week_panel.csv").exists()
    assert (paths.TABLES_DIR / "data_audit.csv").exists()
    assert (paths.TABLES_DIR / "lp_placebo_results.csv").exists()
    assert (paths.TABLES_DIR / "panel_fe_results.csv").exists()
    assert (paths.TABLES_DIR / "panel_fe_results_thursday.csv").exists()
    assert (paths.TABLES_DIR / "upcoming_pressure_monitor.csv").exists()
    assert (paths.SITE_DATA_DIR / "upcoming_pressure_monitor.json").exists()
    assert (paths.SITE_DATA_DIR / "bidbridge.json").exists()

    assert "auction_week_panel" in payload["processed_outputs"]
    assert "lp_placebo_results" in payload["analysis_outputs"]
    assert "panel_fe_results" in payload["analysis_outputs"]
    assert "pressure_monitor_csv" in payload["extension_outputs"]
    assert payload["metadata"]["panel_fe"]["headline_fe_eligible"] is True


def test_run_all_records_fe_ineligibility_without_failing_pipeline(monkeypatch, tmp_path):
    _configure_temp_paths(monkeypatch, tmp_path)
    _install_run_all_stubs(monkeypatch, fe_eligible=False)

    rc = cli.main(["run-all", "--start-date", "2025-01-01"])
    assert rc == 0

    manifest_path = paths.OUTPUTS_DIR / "run_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert (paths.TABLES_DIR / "lp_placebo_results.csv").exists()
    assert (paths.TABLES_DIR / "upcoming_pressure_monitor.csv").exists()
    assert not (paths.TABLES_DIR / "panel_fe_results.csv").exists()
    assert "panel_fe_results" not in payload["analysis_outputs"]
    assert payload["metadata"]["panel_fe"]["headline_fe_eligible"] is False
    assert "reason" in payload["metadata"]["panel_fe"]
