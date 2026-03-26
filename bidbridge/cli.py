from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from .config import load_sources_config, load_study_config
from .data.registry import get_source_registry
from .demo import build_demo_outputs, write_demo_data
from .features.auction_week import normalize_week_definition
from .paths import ROOT, SITE_DATA_DIR, ensure_project_directories


def _study_week_definition() -> str:
    return normalize_week_definition(
        load_study_config().get("sample", {}).get("week_definition", "monday_start")
    )


def _load_harmonized_inputs(start_date: str) -> dict[str, object]:
    from .data.pipeline import (
        _harmonize_auctions,
        _harmonize_dealer_stats,
        _harmonize_investor_class,
        fetch_all,
    )
    from .paths import RAW_DIR

    auctions_path = RAW_DIR / "treasury" / "treasury_auctions.csv"
    dealer_path = RAW_DIR / "nyfed" / "primary_dealer_stats.csv"
    investor_path = RAW_DIR / "treasury" / "investor_class_allotments.csv"
    if not auctions_path.exists() or not dealer_path.exists() or not investor_path.exists():
        fetch_all(start_date=start_date)

    return {
        "auctions": _harmonize_auctions(auctions_path),
        "investor_class": _harmonize_investor_class(investor_path),
        "dealer_stats": _harmonize_dealer_stats(dealer_path),
        "raw_paths": {
            "auctions": auctions_path,
            "dealer_stats": dealer_path,
            "investor_class": investor_path,
        },
    }


def _write_audit(start_date: str) -> dict[str, Path]:
    from .analysis.data_audit import write_data_audit
    from .paths import TABLES_DIR

    inputs = _load_harmonized_inputs(start_date)
    return write_data_audit(
        inputs["auctions"],
        inputs["investor_class"],
        inputs["dealer_stats"],
        TABLES_DIR,
    )


def _doctor() -> int:
    ensure_project_directories()
    required = [
        ROOT / "pyproject.toml",
        ROOT / "README.md",
        ROOT / "AGENTS.md",
        ROOT / "configs" / "study.yml",
        ROOT / "configs" / "sources.yml",
        ROOT / "docs" / "plan.md",
    ]
    missing = [path for path in required if not path.exists()]
    print(f"repo_root={ROOT}")
    print(f"source_count={len(get_source_registry())}")
    if missing:
        print("status=error")
        for path in missing:
            print(f"missing={path}")
        return 1
    print("status=ok")
    return 0


def _list_sources() -> int:
    for record in get_source_registry():
        print(
            f"{record.source_id}\tpriority={record.priority}\tfrequency={record.frequency}\tstatus={record.retrieval_status}"
        )
    return 0


def _show_config(name: str) -> int:
    payload = load_study_config() if name == "study" else load_sources_config()
    print(json.dumps(payload, indent=2))
    return 0


def _fetch(start_date: str) -> int:
    from .data.pipeline import fetch_all

    results = fetch_all(start_date=start_date)
    for name, path in results.items():
        print(f"{name}={path}")
    return 0


def _build_panel(start_date: str) -> int:
    from .data.pipeline import build_panel

    path = build_panel(start_date=start_date, week_definition=_study_week_definition())
    print(f"panel={path}")
    return 0


def _analyze() -> int:
    from .analysis.outputs import run_all_analysis

    outputs = run_all_analysis()
    for name, path in outputs.items():
        print(f"{name}={path}")
    return 0


def _persistence() -> int:
    from .analysis.persistence import generate_persistence_figures, generate_persistence_table
    from .paths import FIGURES_DIR, PROCESSED_DIR, TABLES_DIR

    import pandas as pd

    panel = pd.read_csv(
        PROCESSED_DIR / "auction_week_panel.csv", parse_dates=["week_start", "week_end"],
    )
    figs = generate_persistence_figures(panel, FIGURES_DIR)
    for name, path in figs.items():
        print(f"{name}={path}")
    tbl = generate_persistence_table(panel, TABLES_DIR)
    print(f"persistence_summary={tbl}")
    return 0


def _stress() -> int:
    from .features.stress_flags import (
        add_stress_flags,
        generate_stress_figures,
        generate_stress_table,
    )
    from .paths import FIGURES_DIR, PROCESSED_DIR, TABLES_DIR

    import pandas as pd

    panel = pd.read_csv(
        PROCESSED_DIR / "auction_week_panel.csv", parse_dates=["week_start", "week_end"],
    )
    panel = add_stress_flags(panel)
    figs = generate_stress_figures(panel, FIGURES_DIR)
    for name, path in figs.items():
        print(f"{name}={path}")
    tbl = generate_stress_table(panel, TABLES_DIR)
    print(f"stress_summary={tbl}")
    return 0


def _local_projections() -> int:
    from .analysis.local_projections import (
        generate_lp_figures,
        generate_lp_placebo_table,
        generate_lp_table,
        generate_shock_distribution_figure,
        run_local_projection_placebos,
        run_local_projections_by_regime,
    )
    from .paths import FIGURES_DIR, PROCESSED_DIR, TABLES_DIR

    import pandas as pd

    panel = pd.read_csv(
        PROCESSED_DIR / "auction_week_panel.csv", parse_dates=["week_start", "week_end"],
    )
    results = run_local_projections_by_regime(panel)
    figs = generate_lp_figures(results, FIGURES_DIR)
    for name, path in figs.items():
        print(f"{name}={path}")
    # Shock distribution figure needs the panel
    shock_path = generate_shock_distribution_figure(panel, FIGURES_DIR)
    if shock_path:
        print(f"shock_distribution={shock_path}")
    tbl = generate_lp_table(results, TABLES_DIR)
    print(f"lp_results={tbl}")
    placebo = run_local_projection_placebos(panel)
    placebo_tbl = generate_lp_placebo_table(placebo, TABLES_DIR)
    print(f"lp_placebo_results={placebo_tbl}")
    return 0


def _panel_fe() -> int:
    from .analysis.panel_fe import (
        build_bucket_outcomes,
        generate_panel_fe_figures,
        generate_panel_fe_table,
        run_bucket_fe_regression,
    )
    from .data.pipeline import _harmonize_auctions, _harmonize_investor_class
    from .data.sources.nyfed_pd import finalize_primary_dealer_dataframe
    from .features.maturity_panel import build_maturity_panel
    from .paths import FIGURES_DIR, RAW_DIR, TABLES_DIR

    import pandas as pd

    auctions = _harmonize_auctions(RAW_DIR / "treasury" / "treasury_auctions.csv")
    investor = _harmonize_investor_class(RAW_DIR / "treasury" / "investor_class_allotments.csv")
    dealer_raw = pd.read_csv(
        RAW_DIR / "nyfed" / "primary_dealer_stats.csv",
        parse_dates=["as_of_date", "week_start", "week_end"],
    )

    try:
        week_definition = _study_week_definition()
        mp = build_maturity_panel(auctions, investor, week_definition=week_definition)
        ds = finalize_primary_dealer_dataframe(dealer_raw, week_definition=week_definition)
        bo = build_bucket_outcomes(mp, ds, headline_strict=True, week_definition=week_definition)
    except ValueError as exc:
        print(f"panel_fe_status=ineligible")
        print(f"panel_fe_reason={exc}")
        return 1

    results = run_bucket_fe_regression(bo)
    figs = generate_panel_fe_figures(results, bo, FIGURES_DIR)
    for name, path in figs.items():
        print(f"{name}={path}")
    tbl = generate_panel_fe_table(results, TABLES_DIR)
    print(f"panel_fe_results={tbl}")

    mp_thu = build_maturity_panel(auctions, investor, week_definition="thursday")
    ds_thu = finalize_primary_dealer_dataframe(dealer_raw, week_definition="thursday")
    bo_thu = build_bucket_outcomes(mp_thu, ds_thu, headline_strict=True, week_definition="thursday")
    results_thu = run_bucket_fe_regression(bo_thu)
    figs_thu = generate_panel_fe_figures(
        results_thu, bo_thu, FIGURES_DIR, file_prefix="panel_fe_thursday",
    )
    for name, path in figs_thu.items():
        print(f"{name}={path}")
    tbl_thu = generate_panel_fe_table(
        results_thu, TABLES_DIR, file_name="panel_fe_results_thursday.csv",
    )
    print(f"panel_fe_results_thursday={tbl_thu}")
    return 0


def _update(max_age_days: float) -> int:
    from .data.pipeline import build_panel, fetch_all

    results = fetch_all(max_age_days=max_age_days)
    for name, path in results.items():
        print(f"{name}={path}")
    path = build_panel(week_definition=_study_week_definition())
    print(f"panel={path}")
    return 0


def _run_all(start_date: str) -> int:
    from .analysis.local_projections import (
        generate_lp_figures,
        generate_lp_placebo_table,
        generate_lp_table,
        generate_shock_distribution_figure,
        run_local_projection_placebos,
        run_local_projections_by_regime,
    )
    from .analysis.panel_fe import (
        build_bucket_outcomes,
        generate_panel_fe_figures,
        generate_panel_fe_table,
        run_bucket_fe_regression,
    )
    from .analysis.pressure_monitor import write_upcoming_pressure_monitor
    from .analysis.site_data import write_site_data
    from .analysis.data_audit import write_data_audit
    from .analysis.persistence import generate_persistence_figures, generate_persistence_table
    from .analysis.outputs import run_all_analysis
    from .data.pipeline import (
        _harmonize_auctions,
        _harmonize_investor_class,
        build_panel,
        fetch_all,
    )
    from .data.sources.nyfed_pd import finalize_primary_dealer_dataframe
    from .data.sources.treasury_auctions import fetch_upcoming_auctions
    from .features.maturity_panel import build_maturity_panel
    from .features.stress_flags import add_stress_flags, generate_stress_figures, generate_stress_table
    from .paths import FIGURES_DIR, OUTPUTS_DIR, PROCESSED_DIR, RAW_DIR, TABLES_DIR
    from .run_manifest import write_run_manifest

    import pandas as pd

    raw_inputs = fetch_all(start_date=start_date)
    upcoming_path = fetch_upcoming_auctions(RAW_DIR / "treasury")
    raw_inputs["upcoming_auctions"] = upcoming_path

    panel_path = build_panel(start_date=start_date, week_definition=_study_week_definition())
    processed_outputs = {
        "auction_week_panel": panel_path,
        "maturity_bucket_panel": PROCESSED_DIR / "maturity_bucket_panel.csv",
        "maturity_wide_panel": PROCESSED_DIR / "maturity_wide_panel.csv",
    }

    auctions = _harmonize_auctions(RAW_DIR / "treasury" / "treasury_auctions.csv")
    investor = _harmonize_investor_class(RAW_DIR / "treasury" / "investor_class_allotments.csv")
    dealer_raw = pd.read_csv(
        RAW_DIR / "nyfed" / "primary_dealer_stats.csv",
        parse_dates=["as_of_date", "week_start", "week_end"],
    )
    dealer_stats = finalize_primary_dealer_dataframe(
        dealer_raw, week_definition=_study_week_definition(),
    )
    audit_outputs = write_data_audit(auctions, investor, dealer_stats, TABLES_DIR)

    analysis_outputs = run_all_analysis(panel_path)
    panel = pd.read_csv(panel_path, parse_dates=["week_start", "week_end"])

    lp_results = run_local_projections_by_regime(panel)
    analysis_outputs.update(generate_lp_figures(lp_results, FIGURES_DIR))
    shock_path = generate_shock_distribution_figure(panel, FIGURES_DIR)
    if shock_path is not None:
        analysis_outputs["lp_shock_distribution"] = shock_path
    analysis_outputs["lp_results"] = generate_lp_table(lp_results, TABLES_DIR)
    placebo_results = run_local_projection_placebos(panel)
    analysis_outputs["lp_placebo_results"] = generate_lp_placebo_table(placebo_results, TABLES_DIR)

    fe_metadata = {"headline_fe_eligible": False}
    try:
        mp = build_maturity_panel(auctions, investor, week_definition=_study_week_definition())
        bo = build_bucket_outcomes(
            mp, dealer_stats, headline_strict=True, week_definition=_study_week_definition(),
        )
        fe_results = run_bucket_fe_regression(bo)
        analysis_outputs.update(generate_panel_fe_figures(fe_results, bo, FIGURES_DIR))
        analysis_outputs["panel_fe_results"] = generate_panel_fe_table(fe_results, TABLES_DIR)

        dealer_thu = finalize_primary_dealer_dataframe(dealer_raw, week_definition="thursday")
        mp_thu = build_maturity_panel(auctions, investor, week_definition="thursday")
        bo_thu = build_bucket_outcomes(
            mp_thu, dealer_thu, headline_strict=True, week_definition="thursday",
        )
        fe_thu = run_bucket_fe_regression(bo_thu)
        analysis_outputs.update(
            generate_panel_fe_figures(fe_thu, bo_thu, FIGURES_DIR, file_prefix="panel_fe_thursday")
        )
        analysis_outputs["panel_fe_results_thursday"] = generate_panel_fe_table(
            fe_thu, TABLES_DIR, file_name="panel_fe_results_thursday.csv",
        )
        fe_metadata = fe_results.get("_metadata", fe_metadata)
    except ValueError as exc:
        fe_metadata = {"headline_fe_eligible": False, "reason": str(exc)}

    analysis_outputs.update(generate_persistence_figures(panel, FIGURES_DIR))
    analysis_outputs["persistence_summary"] = generate_persistence_table(panel, TABLES_DIR)

    stress_panel = add_stress_flags(panel)
    analysis_outputs.update(generate_stress_figures(stress_panel, FIGURES_DIR))
    analysis_outputs["stress_summary"] = generate_stress_table(stress_panel, TABLES_DIR)

    upcoming_df = pd.read_csv(upcoming_path, parse_dates=["auction_date", "issue_date"])
    extension_outputs = write_upcoming_pressure_monitor(
        panel,
        upcoming_df,
        TABLES_DIR / "upcoming_pressure_monitor.csv",
        SITE_DATA_DIR / "upcoming_pressure_monitor.json",
        week_definition=_study_week_definition(),
    )

    bridge_summary_path = TABLES_DIR / "bridge_episode_summary.csv"
    bridge_summary = (
        pd.read_csv(bridge_summary_path)
        if bridge_summary_path.exists()
        else pd.DataFrame(columns=["year", "episodes", "avg_inv_change_M"])
    )
    stress_summary_path = TABLES_DIR / "stress_regime_summary.csv"
    stress_summary = (
        pd.read_csv(stress_summary_path)
        if stress_summary_path.exists()
        else pd.DataFrame(columns=["stress_flag", "bridge_rate_flagged", "bridge_rate_unflagged"])
    )
    pressure_monitor_df = pd.read_csv(extension_outputs["pressure_monitor_csv"], parse_dates=["week_start"])
    mat_panel_path = processed_outputs.get("maturity_bucket_panel")
    mat_panel = (
        pd.read_csv(mat_panel_path, parse_dates=["week_start"])
        if mat_panel_path and Path(mat_panel_path).exists()
        else None
    )
    extension_outputs["site_data"] = write_site_data(
        panel,
        lp_results,
        stress_summary,
        bridge_summary,
        SITE_DATA_DIR / "bidbridge.json",
        pressure_monitor=pressure_monitor_df,
        maturity_panel=mat_panel,
    )

    manifest_path = write_run_manifest(
        OUTPUTS_DIR / "run_manifest.json",
        repo_root=ROOT,
        raw_inputs=raw_inputs,
        processed_outputs=processed_outputs,
        analysis_outputs=analysis_outputs,
        audit_outputs=audit_outputs,
        extension_outputs=extension_outputs,
        metadata={"panel_fe": fe_metadata},
    )

    for name, path in {**processed_outputs, **audit_outputs, **analysis_outputs, **extension_outputs}.items():
        print(f"{name}={path}")
    print(f"run_manifest={manifest_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bidbridge")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("doctor")
    subparsers.add_parser("list-sources")

    config_parser = subparsers.add_parser("show-config")
    config_parser.add_argument("name", choices=["study", "sources"])

    subparsers.add_parser("demo-data")
    subparsers.add_parser("demo-panel")

    fetch_parser = subparsers.add_parser("fetch", help="Fetch all priority-1 data sources")
    fetch_parser.add_argument(
        "--start-date", default="2010-01-01",
        help="Earliest date to fetch (YYYY-MM-DD, default: 2010-01-01)",
    )

    panel_parser = subparsers.add_parser(
        "build-panel", help="Build auction-week panel from fetched data",
    )
    panel_parser.add_argument(
        "--start-date", default="2010-01-01",
        help="Earliest date (YYYY-MM-DD, default: 2010-01-01)",
    )

    subparsers.add_parser("analyze", help="Generate all descriptive analysis outputs")
    subparsers.add_parser("persistence", help="Run persistence / half-life analysis")
    subparsers.add_parser("stress", help="Run stress-flag regime analysis")
    subparsers.add_parser("lp", help="Run local projection impulse responses")
    subparsers.add_parser("panel-fe", help="Run maturity-bucket panel fixed-effects regressions")

    update_parser = subparsers.add_parser(
        "update", help="Incremental refresh: skip sources fetched within max-age days",
    )
    update_parser.add_argument(
        "--max-age", type=float, default=1.0,
        help="Skip sources younger than this many days (default: 1.0)",
    )

    run_all_parser = subparsers.add_parser(
        "run-all", help="Full reproduction path: fetch, build, audit, analyze, FE, LP, persistence, stress, pressure monitor",
    )
    run_all_parser.add_argument(
        "--start-date", default="2010-01-01",
        help="Earliest date (YYYY-MM-DD, default: 2010-01-01)",
    )

    args = parser.parse_args(argv)

    if args.verbose:
        logging.basicConfig(level=logging.INFO, format="%(name)s: %(message)s")

    if args.command == "doctor":
        return _doctor()
    if args.command == "list-sources":
        return _list_sources()
    if args.command == "show-config":
        return _show_config(args.name)
    if args.command == "demo-data":
        outputs = write_demo_data()
        for key, value in outputs.items():
            print(f"{key}={value}")
        return 0
    if args.command == "demo-panel":
        outputs = build_demo_outputs()
        for key, value in outputs.items():
            print(f"{key}={value}")
        return 0
    if args.command == "fetch":
        return _fetch(args.start_date)
    if args.command == "build-panel":
        return _build_panel(args.start_date)
    if args.command == "analyze":
        return _analyze()
    if args.command == "persistence":
        return _persistence()
    if args.command == "stress":
        return _stress()
    if args.command == "lp":
        return _local_projections()
    if args.command == "panel-fe":
        return _panel_fe()
    if args.command == "update":
        return _update(args.max_age)
    if args.command == "run-all":
        return _run_all(args.start_date)

    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
