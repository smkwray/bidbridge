from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from .config import load_sources_config, load_study_config
from .data.registry import get_source_registry
from .demo import build_demo_outputs, write_demo_data
from .paths import CODEX_DIR, ROOT, ensure_project_directories


def _doctor() -> int:
    ensure_project_directories()
    required = [
        ROOT / "pyproject.toml",
        ROOT / "README.md",
        ROOT / "AGENTS.md",
        ROOT / "configs" / "study.yml",
        ROOT / "configs" / "sources.yml",
        ROOT / "docs" / "plan.md",
        ROOT / "codex" / "tasks.json",
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


def _show_tasks() -> int:
    tasks_path = CODEX_DIR / "tasks.json"
    payload = json.loads(tasks_path.read_text(encoding="utf-8"))
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

    path = build_panel(start_date=start_date)
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
        generate_lp_table,
        generate_shock_distribution_figure,
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
    return 0


def _panel_fe() -> int:
    from .analysis.panel_fe import (
        build_bucket_outcomes,
        generate_panel_fe_figures,
        generate_panel_fe_table,
        run_bucket_fe_regression,
    )
    from .paths import FIGURES_DIR, PROCESSED_DIR, RAW_DIR, TABLES_DIR

    import pandas as pd

    mp = pd.read_csv(
        PROCESSED_DIR / "maturity_bucket_panel.csv", parse_dates=["week_start"],
    )
    ds = pd.read_csv(
        RAW_DIR / "nyfed" / "primary_dealer_stats.csv",
        parse_dates=["week_start", "week_end"],
    )
    bo = build_bucket_outcomes(mp, ds)
    results = run_bucket_fe_regression(bo)
    figs = generate_panel_fe_figures(results, bo, FIGURES_DIR)
    for name, path in figs.items():
        print(f"{name}={path}")
    tbl = generate_panel_fe_table(results, TABLES_DIR)
    print(f"panel_fe_results={tbl}")
    return 0


def _update(max_age_days: float) -> int:
    from .data.pipeline import build_panel, fetch_all

    results = fetch_all(max_age_days=max_age_days)
    for name, path in results.items():
        print(f"{name}={path}")
    path = build_panel()
    print(f"panel={path}")
    return 0


def _run_all(start_date: str) -> int:
    print("=== FETCH ===")
    _fetch(start_date)
    print("\n=== BUILD PANEL ===")
    _build_panel(start_date)
    print("\n=== ANALYZE ===")
    _analyze()
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

    subparsers.add_parser("show-tasks")
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

    run_all_parser = subparsers.add_parser("run-all", help="Fetch, build panel, and analyze")
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
    if args.command == "show-tasks":
        return _show_tasks()
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
