from __future__ import annotations

from pathlib import Path
import random

import pandas as pd

from .analysis.event_studies import make_supply_inventory_plot
from .analysis.regressions import run_demo_bridge_regression
from .features.auction_week import build_weekly_panel
from .paths import FIGURES_DIR, RAW_DIR, TABLES_DIR, ensure_project_directories


def _week_rows(seed: int = 20260316) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rng = random.Random(seed)
    weeks = pd.date_range("2025-09-01", periods=20, freq="W-MON")

    auction_rows: list[dict] = []
    investor_rows: list[dict] = []
    dealer_rows: list[dict] = []

    treasury_inventory = 210.0
    previous_change = 0.0

    for index, week_start in enumerate(weeks):
        heavy = index in {4, 8, 12, 15}
        refunding = index % 4 == 0
        total_supply = 72 + rng.uniform(-6, 6) + (28 if heavy else 0)
        bill_share = 0.40 + rng.uniform(-0.08, 0.08)
        bill_amount = total_supply * bill_share
        coupon_amount = total_supply - bill_amount

        securities = [
            {
                "auction_date": week_start + pd.Timedelta(days=1),
                "issue_date": week_start + pd.Timedelta(days=3),
                "security_type": "Bill",
                "maturity_bucket": "bills",
                "announced_amount": round(bill_amount * 1.01, 3),
                "awarded_amount": round(bill_amount, 3),
                "bid_to_cover": round(3.1 - (0.18 if heavy else 0.0) + rng.uniform(-0.05, 0.08), 3),
                "tail_bp": round(0.05 + (0.25 if heavy else 0.02) + rng.uniform(0.0, 0.05), 3),
                "refunding_week": refunding,
            },
            {
                "auction_date": week_start + pd.Timedelta(days=3),
                "issue_date": week_start + pd.Timedelta(days=4),
                "security_type": "Note",
                "maturity_bucket": "nominal_coupons",
                "announced_amount": round(coupon_amount * 1.01, 3),
                "awarded_amount": round(coupon_amount, 3),
                "bid_to_cover": round(2.45 - (0.22 if heavy else 0.0) + rng.uniform(-0.06, 0.10), 3),
                "tail_bp": round(0.18 + (0.55 if heavy else 0.08) + rng.uniform(0.0, 0.07), 3),
                "refunding_week": refunding,
            },
        ]

        dealer_share_base = 0.26 + (0.09 if heavy else 0.0) + rng.uniform(-0.015, 0.02)
        investment_share_base = 0.43 - (0.08 if heavy else 0.0) + rng.uniform(-0.02, 0.02)
        foreign_share_base = 0.19 - (0.03 if heavy else 0.0) + rng.uniform(-0.015, 0.015)
        depository_base = 0.09 + rng.uniform(-0.015, 0.015)

        for sec in securities:
            sec_multiplier = 1.05 if sec["security_type"] == "Bill" else 0.95
            dealer_share = max(0.05, min(0.75, dealer_share_base * sec_multiplier))
            investment_share = max(0.05, min(0.75, investment_share_base / sec_multiplier))
            foreign_share = max(0.01, min(0.35, foreign_share_base))
            depository_share = max(0.01, min(0.20, depository_base))
            other_share = max(0.0, 1.0 - dealer_share - investment_share - foreign_share - depository_share)

            auction_rows.append(sec)
            investor_rows.append(
                {
                    "issue_date": sec["issue_date"],
                    "security_type": sec["security_type"],
                    "dealer_share": round(dealer_share, 6),
                    "investment_funds_share": round(investment_share, 6),
                    "foreign_share": round(foreign_share, 6),
                    "depository_share": round(depository_share, 6),
                    "other_share": round(other_share, 6),
                }
            )

        weekly_dealer_awards = sum(
            row["awarded_amount"] * investor_rows[-2 + idx]["dealer_share"]
            for idx, row in enumerate(securities)
        )
        mean_reversion = -0.22 * previous_change
        inventory_change = 0.33 * weekly_dealer_awards + mean_reversion + rng.uniform(-4.0, 4.0)
        treasury_inventory = max(160.0, treasury_inventory + inventory_change)
        financing_usage = max(100.0, 0.62 * treasury_inventory + 0.55 * max(inventory_change, 0) + rng.uniform(-6.0, 6.0))

        dealer_rows.append(
            {
                "week_start": week_start.normalize(),
                "week_end": (week_start + pd.Timedelta(days=6)).normalize(),
                "pd_treasury_inventory": round(treasury_inventory, 3),
                "pd_financing_usage": round(financing_usage, 3),
            }
        )
        previous_change = inventory_change

    return (
        pd.DataFrame(auction_rows),
        pd.DataFrame(investor_rows),
        pd.DataFrame(dealer_rows),
    )


def write_demo_data(raw_demo_dir: Path | None = None) -> dict[str, Path]:
    ensure_project_directories()
    raw_demo_dir = raw_demo_dir or (RAW_DIR / "demo")
    raw_demo_dir.mkdir(parents=True, exist_ok=True)

    auctions, investor_class, dealer_stats = _week_rows()

    outputs = {
        "auctions": raw_demo_dir / "treasury_auctions_demo.csv",
        "investor_class": raw_demo_dir / "investor_class_demo.csv",
        "dealer_stats": raw_demo_dir / "primary_dealer_demo.csv",
    }

    auctions.to_csv(outputs["auctions"], index=False)
    investor_class.to_csv(outputs["investor_class"], index=False)
    dealer_stats.to_csv(outputs["dealer_stats"], index=False)
    return outputs


def build_demo_outputs(raw_demo_dir: Path | None = None) -> dict[str, Path]:
    ensure_project_directories()
    raw_demo_dir = raw_demo_dir or (RAW_DIR / "demo")
    paths = write_demo_data(raw_demo_dir)

    auctions = pd.read_csv(paths["auctions"], parse_dates=["auction_date", "issue_date"])
    investor_class = pd.read_csv(paths["investor_class"], parse_dates=["issue_date"])
    dealer_stats = pd.read_csv(paths["dealer_stats"], parse_dates=["week_start", "week_end"])

    panel = build_weekly_panel(auctions, investor_class, dealer_stats)
    panel_path = raw_demo_dir.parent.parent / "processed" / "auction_week_panel_demo.csv"
    panel.to_csv(panel_path, index=False)

    regression = run_demo_bridge_regression(panel)
    regression_path = TABLES_DIR / "demo_bridge_regression.csv"
    regression_path.parent.mkdir(parents=True, exist_ok=True)
    regression.to_csv(regression_path, index=False)

    figure_path = FIGURES_DIR / "demo_supply_inventory_plot.png"
    make_supply_inventory_plot(panel, figure_path)

    summary = panel[
        [
            "week_start",
            "awarded_amount_total",
            "dealer_share_allotment",
            "nondealer_share",
            "pd_treasury_inventory",
            "inventory_change",
            "dealer_bridge_ratio",
            "bridge_episode",
        ]
    ]
    summary_path = TABLES_DIR / "demo_panel_summary.csv"
    summary.to_csv(summary_path, index=False)

    return {
        "panel": panel_path,
        "regression": regression_path,
        "figure": figure_path,
        "summary": summary_path,
    }
