"""Generate all standard descriptive analysis outputs from the panel.

Called by `bidbridge analyze`. Produces figures and tables in the outputs/ directory.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from ..paths import FIGURES_DIR, PROCESSED_DIR, TABLES_DIR

logger = logging.getLogger(__name__)


def _load_panel(panel_path: Path | None = None) -> pd.DataFrame:
    path = panel_path or (PROCESSED_DIR / "auction_week_panel.csv")
    if not path.exists():
        raise FileNotFoundError(
            f"Panel not found at {path}. Run `bidbridge build-panel` first."
        )
    return pd.read_csv(path, parse_dates=["week_start", "week_end"])


def generate_timeseries_figure(panel: pd.DataFrame, out: Path) -> Path:
    """Supply and dealer inventory change over time."""
    p = panel.dropna(subset=["pd_treasury_inventory"])
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    ax1.plot(p["week_start"], p["awarded_amount_total"] / 1e9, alpha=0.6, linewidth=0.8)
    refunding = p[p["refunding_week"]]
    ax1.scatter(
        refunding["week_start"], refunding["awarded_amount_total"] / 1e9,
        color="red", s=10, alpha=0.5, label="Refunding week", zorder=5,
    )
    ax1.set_ylabel("Weekly awarded ($B)")
    ax1.set_title("Treasury auction supply and dealer inventory dynamics")
    ax1.legend()

    ax2.plot(p["week_start"], p["inventory_change"], alpha=0.6, linewidth=0.8, color="C1")
    if "bridge_episode" in p.columns:
        eps = p[p["bridge_episode"]]
        ax2.scatter(
            eps["week_start"], eps["inventory_change"],
            color="red", s=15, alpha=0.7, label="Bridge episode", zorder=5,
        )
        ax2.legend()
    ax2.axhline(0, color="gray", linewidth=0.5, linestyle="--")
    ax2.set_ylabel("Dealer inventory change ($M)")
    ax2.set_xlabel("Week")

    plt.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def generate_dealer_scatter(panel: pd.DataFrame, out: Path) -> Path:
    """Dealer share vs supply volume scatter."""
    fig, ax = plt.subplots(figsize=(10, 6))
    scatter = ax.scatter(
        panel["awarded_amount_total"] / 1e9,
        panel["dealer_share_allotment"],
        c=panel["week_start"].astype(int) / 1e18,
        cmap="viridis", alpha=0.4, s=12,
    )
    ax.set_xlabel("Weekly awarded ($B)")
    ax.set_ylabel("Dealer share of allotment")
    ax.set_title("Dealer absorption vs supply volume")
    plt.colorbar(scatter, ax=ax, label="Time")
    plt.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def generate_event_study_refunding(panel: pd.DataFrame, out: Path) -> Path:
    """Event study: inventory around refunding weeks."""
    p = panel.dropna(subset=["pd_treasury_inventory"]).reset_index(drop=True)
    refunding_idx = p.index[p["refunding_week"]].tolist()
    window = 6

    event_data = []
    for idx in refunding_idx:
        for w in range(-window, window + 1):
            row_idx = idx + w
            if 0 <= row_idx < len(p):
                event_data.append({
                    "event_week": w,
                    "inventory_change": p.loc[row_idx, "inventory_change"],
                    "awarded_M": p.loc[row_idx, "awarded_amount_total"] / 1e6,
                    "dealer_share": p.loc[row_idx, "dealer_share_allotment"],
                })

    edf = pd.DataFrame(event_data)
    avg = edf.groupby("event_week").agg(
        inv_mean=("inventory_change", "mean"),
        inv_std=("inventory_change", "std"),
        inv_count=("inventory_change", "count"),
        supply_mean=("awarded_M", "mean"),
        dealer_mean=("dealer_share", "mean"),
    ).reset_index()
    avg["inv_se"] = avg["inv_std"] / np.sqrt(avg["inv_count"])

    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)

    axes[0].bar(avg["event_week"], avg["inv_mean"], color="steelblue", alpha=0.7)
    axes[0].errorbar(
        avg["event_week"], avg["inv_mean"], yerr=1.96 * avg["inv_se"],
        fmt="none", color="black", capsize=3, alpha=0.5,
    )
    axes[0].axhline(0, color="gray", linewidth=0.5, linestyle="--")
    axes[0].axvline(0, color="red", linewidth=1, linestyle="--", alpha=0.5, label="Refunding week")
    axes[0].set_ylabel("Inventory change ($M)")
    axes[0].set_title("Event study: Dealer behavior around quarterly refundings")
    axes[0].legend()

    axes[1].bar(avg["event_week"], avg["supply_mean"], color="darkorange", alpha=0.7)
    axes[1].axvline(0, color="red", linewidth=1, linestyle="--", alpha=0.5)
    axes[1].set_ylabel("Weekly awarded ($M)")

    axes[2].plot(avg["event_week"], avg["dealer_mean"], "o-", color="green", alpha=0.7)
    axes[2].axvline(0, color="red", linewidth=1, linestyle="--", alpha=0.5)
    axes[2].set_ylabel("Dealer share")
    axes[2].set_xlabel("Weeks relative to refunding")

    plt.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def generate_event_study_bridge(panel: pd.DataFrame, out: Path) -> Path:
    """Event study: inventory around bridge episodes."""
    p = panel.dropna(subset=["pd_treasury_inventory"]).reset_index(drop=True)
    bridge_idx = p.index[p["bridge_episode"]].tolist()
    if not bridge_idx:
        logger.warning("No bridge episodes found — skipping bridge event study")
        return None

    window = 6
    data = []
    for idx in bridge_idx:
        for w in range(-window, window + 1):
            row_idx = idx + w
            if 0 <= row_idx < len(p):
                data.append({
                    "event_week": w,
                    "inventory_level": p.loc[row_idx, "pd_treasury_inventory"],
                    "inventory_change": p.loc[row_idx, "inventory_change"],
                    "financing_intensity": p.loc[row_idx, "financing_intensity"],
                })

    bdf = pd.DataFrame(data)
    bavg = bdf.groupby("event_week").mean().reset_index()

    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)

    axes[0].plot(bavg["event_week"], bavg["inventory_level"], "o-", color="steelblue")
    axes[0].axvline(0, color="red", linewidth=1, linestyle="--", alpha=0.5, label="Bridge episode")
    axes[0].set_ylabel("Dealer inventory ($M)")
    axes[0].set_title(f"Event study: Dealer inventory around bridge episodes (n={len(bridge_idx)})")
    axes[0].legend()

    axes[1].bar(bavg["event_week"], bavg["inventory_change"], color="steelblue", alpha=0.7)
    axes[1].axhline(0, color="gray", linewidth=0.5, linestyle="--")
    axes[1].axvline(0, color="red", linewidth=1, linestyle="--", alpha=0.5)
    axes[1].set_ylabel("Inventory change ($M)")

    axes[2].plot(bavg["event_week"], bavg["financing_intensity"], "o-", color="purple")
    axes[2].axvline(0, color="red", linewidth=1, linestyle="--", alpha=0.5)
    axes[2].set_ylabel("Financing intensity")
    axes[2].set_xlabel("Weeks relative to bridge episode")

    plt.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def generate_soma_comparison(panel: pd.DataFrame, out: Path) -> Path:
    """SOMA vs dealer Treasury holdings over time."""
    p = panel.dropna(subset=["pd_treasury_inventory"])

    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax1.plot(p["week_start"], p["pd_treasury_inventory"] / 1e3, label="Dealer inventory ($B)", alpha=0.7)
    ax2 = ax1.twinx()
    if "soma_treasury_total" in p.columns:
        soma = p.dropna(subset=["soma_treasury_total"])
        ax2.plot(soma["week_start"], soma["soma_treasury_total"] / 1e12, color="red", alpha=0.5, label="SOMA Treasury ($T)")
    ax1.set_xlabel("Week")
    ax1.set_ylabel("Dealer inventory ($B)")
    ax2.set_ylabel("SOMA Treasury holdings ($T)")
    ax1.set_title("Primary dealer vs Fed Treasury holdings")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    plt.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def generate_summary_tables(panel: pd.DataFrame) -> dict[str, Path]:
    """Generate summary statistics and annual tables."""
    paths = {}

    # Panel summary stats
    stat_cols = [
        "auction_count", "awarded_amount_total", "dealer_share_allotment",
        "nondealer_share", "pd_treasury_inventory", "inventory_change",
        "weighted_bid_to_cover", "financing_intensity",
    ]
    available = [c for c in stat_cols if c in panel.columns]
    summary = panel[available].describe().T
    out = TABLES_DIR / "panel_summary_stats.csv"
    summary.to_csv(out)
    paths["summary_stats"] = out

    # Annual summary
    p = panel.copy()
    p["year"] = p["week_start"].dt.year
    annual = p.groupby("year").agg({
        "auction_count": "sum",
        "awarded_amount_total": "sum",
        "dealer_share_allotment": "mean",
        "pd_treasury_inventory": "mean",
        "inventory_change": "mean",
        "heavy_supply": "sum",
        "refunding_week": "sum",
        "bridge_episode": "sum",
    }).round(2)
    annual["awarded_total_B"] = (annual["awarded_amount_total"] / 1e9).round(1)
    out = TABLES_DIR / "annual_summary.csv"
    annual.to_csv(out)
    paths["annual_summary"] = out

    # Bridge episode summary by year
    eps = p[p["bridge_episode"]].copy()
    if len(eps) > 0:
        ep_summary = eps.groupby("year").agg({
            "bridge_episode": "sum",
            "inventory_change": "mean",
            "awarded_amount_total": lambda x: (x / 1e6).mean(),
            "dealer_share_allotment": "mean",
            "financing_intensity": "mean",
        }).round(2)
        ep_summary.columns = [
            "episodes", "avg_inv_change_M", "avg_awarded_M",
            "avg_dealer_share", "avg_financing_intensity",
        ]
        out = TABLES_DIR / "bridge_episode_summary.csv"
        ep_summary.to_csv(out)
        paths["bridge_episodes"] = out

    # Regressions
    from .regressions import run_all_regressions
    p_with_data = panel.dropna(subset=["pd_treasury_inventory"])
    if len(p_with_data) > 10:
        reg_results = run_all_regressions(p_with_data)

        for name, result in reg_results.items():
            if isinstance(result, dict):
                # Subsample results: dict of name -> DataFrame
                for sub_name, sub_df in result.items():
                    out = TABLES_DIR / f"regression_{name}_{sub_name}.csv"
                    sub_df.to_csv(out, index=False)
                    paths[f"regression_{name}_{sub_name}"] = out
                    logger.info("  %s/%s: n=%s", name, sub_name,
                                sub_df.attrs.get("n_obs", len(sub_df)))
            else:
                out = TABLES_DIR / f"regression_{name}.csv"
                result.to_csv(out, index=False)
                paths[f"regression_{name}"] = out
                logger.info(
                    "  %s: n=%s, R²=%s",
                    name,
                    result.attrs.get("n_obs", len(result)),
                    f"{result.attrs.get('r_squared', 'N/A'):.4f}" if isinstance(result.attrs.get("r_squared"), float) else "N/A",
                )

    return paths


def run_all_analysis(panel_path: Path | None = None) -> dict[str, Path]:
    """Generate all standard outputs. Returns dict of output paths."""
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    panel = _load_panel(panel_path)
    outputs: dict[str, Path] = {}

    logger.info("Generating timeseries figure...")
    outputs["timeseries"] = generate_timeseries_figure(
        panel, FIGURES_DIR / "supply_inventory_timeseries.png",
    )

    logger.info("Generating dealer scatter...")
    outputs["dealer_scatter"] = generate_dealer_scatter(
        panel, FIGURES_DIR / "dealer_share_vs_supply.png",
    )

    logger.info("Generating refunding event study...")
    outputs["event_refunding"] = generate_event_study_refunding(
        panel, FIGURES_DIR / "event_study_refunding.png",
    )

    logger.info("Generating bridge episode event study...")
    bridge_path = generate_event_study_bridge(
        panel, FIGURES_DIR / "event_study_bridge_episodes.png",
    )
    if bridge_path is not None:
        outputs["event_bridge"] = bridge_path

    logger.info("Generating SOMA comparison...")
    outputs["soma_comparison"] = generate_soma_comparison(
        panel, FIGURES_DIR / "dealer_vs_soma_timeseries.png",
    )

    logger.info("Generating summary tables...")
    table_paths = generate_summary_tables(panel)
    outputs.update(table_paths)

    logger.info("Analysis complete: %d outputs generated", len(outputs))
    return outputs
