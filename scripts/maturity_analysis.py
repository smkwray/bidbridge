"""Maturity-split analysis: how dealer absorption differs across instrument types.

Generates:
  - outputs/figures/maturity_split_timeseries.png
  - outputs/figures/dealer_share_by_instrument.png
  - outputs/figures/btc_tail_by_maturity.png
  - outputs/tables/maturity_split_stats.csv
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure the repo root is on sys.path so bidbridge is importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np

from bidbridge.paths import (
    FIGURES_DIR,
    TABLES_DIR,
    RAW_DIR,
    PROCESSED_DIR,
    ensure_project_directories,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
AUCTIONS_PATH = RAW_DIR / "treasury" / "treasury_auctions.csv"
PANEL_PATH = PROCESSED_DIR / "auction_week_panel.csv"
INVESTOR_CLASS_PATH = RAW_DIR / "treasury" / "investor_class_allotments.csv"


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load the three source datasets."""
    auctions = pd.read_csv(AUCTIONS_PATH, parse_dates=["auction_date", "issue_date"])
    panel = pd.read_csv(PANEL_PATH, parse_dates=["week_start"])
    investor = pd.read_csv(INVESTOR_CLASS_PATH)
    return auctions, panel, investor


# ---------------------------------------------------------------------------
# Figure 1 — Stacked area: weekly bill vs coupon amounts, bridge episodes
# ---------------------------------------------------------------------------
def plot_maturity_split_timeseries(panel: pd.DataFrame) -> Path:
    """Stacked area chart of weekly bill_amount vs coupon_amount with bridge
    episode shading."""
    fig, ax = plt.subplots(figsize=(14, 5))

    weeks = panel["week_start"]
    bill_bn = panel["bill_amount"] / 1e9
    coupon_bn = panel["coupon_amount"] / 1e9

    ax.stackplot(
        weeks,
        bill_bn,
        coupon_bn,
        labels=["Bills", "Coupons"],
        colors=["#4C72B0", "#DD8452"],
        alpha=0.85,
    )

    # Shade bridge episodes
    bridge = panel["bridge_episode"]
    in_episode = False
    start = None
    for i, (w, b) in enumerate(zip(weeks, bridge)):
        if b and not in_episode:
            start = w
            in_episode = True
        elif not b and in_episode:
            ax.axvspan(start, weeks.iloc[i - 1], color="red", alpha=0.12, zorder=0)
            in_episode = False
    if in_episode:
        ax.axvspan(start, weeks.iloc[-1], color="red", alpha=0.12, zorder=0)

    ax.set_title("Weekly Issuance by Maturity Bucket", fontsize=13, fontweight="bold")
    ax.set_ylabel("Awarded Amount ($bn)")
    ax.set_xlabel("")
    ax.legend(loc="upper left", frameon=True)
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    # Add a small note for bridge episode shading
    ax.annotate(
        "Red shading = bridge episode",
        xy=(0.99, 0.97),
        xycoords="axes fraction",
        ha="right",
        va="top",
        fontsize=8,
        color="red",
        alpha=0.7,
    )

    fig.tight_layout()
    out = FIGURES_DIR / "maturity_split_timeseries.png"
    fig.savefig(out, dpi=200)
    plt.close(fig)
    return out


# ---------------------------------------------------------------------------
# Figure 2 — Dealer share by instrument group (box plots)
# ---------------------------------------------------------------------------
def plot_dealer_share_by_instrument(
    auctions: pd.DataFrame, investor: pd.DataFrame
) -> Path:
    """Box plots of dealer_share by instrument_group, merging raw auctions
    with investor-class allotment data on cusip + issue_date."""
    # Convert issue_date to string in both frames for a safe merge key
    auctions = auctions.copy()
    investor = investor.copy()
    auctions["issue_date_str"] = auctions["issue_date"].astype(str)
    investor["issue_date_str"] = investor["issue_date"].astype(str)

    merged = auctions.merge(
        investor[["cusip", "issue_date_str", "dealer_share"]],
        on=["cusip", "issue_date_str"],
        how="inner",
    )

    # Focus on the five core groups
    groups = ["bills", "nominal_coupons", "bonds", "tips", "frns"]
    merged = merged[merged["instrument_group"].isin(groups)].copy()

    # Nicer labels
    label_map = {
        "bills": "Bills",
        "nominal_coupons": "Coupons",
        "bonds": "Bonds",
        "tips": "TIPS",
        "frns": "FRNs",
    }
    merged["group_label"] = merged["instrument_group"].map(label_map)

    # Order for plotting
    order = ["Bills", "Coupons", "Bonds", "TIPS", "FRNs"]
    data_by_group = [
        merged.loc[merged["group_label"] == g, "dealer_share"].dropna().values
        for g in order
    ]

    fig, ax = plt.subplots(figsize=(9, 5))

    palette = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B3"]
    bp = ax.boxplot(
        data_by_group,
        tick_labels=order,
        patch_artist=True,
        widths=0.5,
        showfliers=False,
        medianprops=dict(color="black", linewidth=1.5),
    )
    for patch, color in zip(bp["boxes"], palette):
        patch.set_facecolor(color)
        patch.set_alpha(0.75)

    # Overlay individual medians as text
    for i, d in enumerate(data_by_group, start=1):
        med = np.median(d)
        ax.annotate(
            f"{med:.1%}",
            xy=(i, med),
            xytext=(0, 8),
            textcoords="offset points",
            ha="center",
            fontsize=8,
            fontweight="bold",
        )

    ax.set_title(
        "Dealer Share of Allotment by Instrument Group",
        fontsize=13,
        fontweight="bold",
    )
    ax.set_ylabel("Dealer Share")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0%}"))
    ax.set_ylim(0, 1.05)
    ax.grid(axis="y", alpha=0.3)

    # Count annotations at the bottom
    for i, (g, d) in enumerate(zip(order, data_by_group), start=1):
        ax.text(i, -0.06, f"n={len(d)}", ha="center", fontsize=7, color="gray",
                transform=ax.get_xaxis_transform())

    fig.tight_layout()
    out = FIGURES_DIR / "dealer_share_by_instrument.png"
    fig.savefig(out, dpi=200)
    plt.close(fig)
    return out


# ---------------------------------------------------------------------------
# Figure 3 — Bid-to-cover and tail by maturity bucket (time series panels)
# ---------------------------------------------------------------------------
def plot_btc_tail_by_maturity(auctions: pd.DataFrame) -> Path:
    """Two-panel time series showing bid_to_cover and tail_bp across maturity
    buckets (bills, coupons/notes, bonds)."""
    df = auctions.copy()
    # Map instrument_group to three broad buckets
    bucket_map = {
        "bills": "Bills",
        "cmb": "Bills",
        "nominal_coupons": "Coupons (Notes)",
        "bonds": "Bonds",
        "tips": "TIPS",
        "frns": "FRNs",
    }
    df["bucket"] = df["instrument_group"].map(bucket_map)

    # Focus on the three main maturity buckets for clarity
    focus = ["Bills", "Coupons (Notes)", "Bonds"]
    df = df[df["bucket"].isin(focus)].copy()
    df = df.sort_values("auction_date")

    colors = {"Bills": "#4C72B0", "Coupons (Notes)": "#DD8452", "Bonds": "#55A868"}

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    # --- Panel A: Bid-to-Cover ---
    ax = axes[0]
    for bucket in focus:
        sub = df[df["bucket"] == bucket]
        # Rolling median for smoother visual (window ~ 20 auctions)
        rolling = sub.set_index("auction_date")["bid_to_cover"].rolling("90D").median()
        ax.plot(rolling.index, rolling.values, label=bucket, color=colors[bucket],
                linewidth=1.2, alpha=0.85)
    ax.set_ylabel("Bid-to-Cover (90-day rolling median)")
    ax.set_title(
        "Auction Demand by Maturity Bucket", fontsize=13, fontweight="bold"
    )
    ax.legend(loc="upper right", frameon=True)
    ax.grid(axis="y", alpha=0.3)

    # --- Panel B: Tail (basis points) ---
    ax = axes[1]
    for bucket in focus:
        sub = df[df["bucket"] == bucket].dropna(subset=["tail_bp"])
        rolling = sub.set_index("auction_date")["tail_bp"].rolling("90D").median()
        ax.plot(rolling.index, rolling.values, label=bucket, color=colors[bucket],
                linewidth=1.2, alpha=0.85)
    ax.set_ylabel("Tail (bp, 90-day rolling median)")
    ax.set_xlabel("")
    ax.legend(loc="upper right", frameon=True)
    ax.grid(axis="y", alpha=0.3)
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    fig.tight_layout()
    out = FIGURES_DIR / "btc_tail_by_maturity.png"
    fig.savefig(out, dpi=200)
    plt.close(fig)
    return out


# ---------------------------------------------------------------------------
# Table — Summary statistics by instrument_group
# ---------------------------------------------------------------------------
def make_maturity_split_stats(auctions: pd.DataFrame) -> Path:
    """Summary statistics of key auction metrics by instrument_group."""
    groups = ["bills", "nominal_coupons", "bonds", "tips", "frns"]
    df = auctions[auctions["instrument_group"].isin(groups)].copy()

    metrics = ["awarded_amount", "bid_to_cover", "tail_bp"]
    rows = []
    for group in groups:
        sub = df[df["instrument_group"] == group]
        row = {"instrument_group": group, "count": len(sub)}
        for m in metrics:
            s = sub[m].dropna()
            row[f"{m}_mean"] = s.mean()
            row[f"{m}_median"] = s.median()
            row[f"{m}_std"] = s.std()
        rows.append(row)

    stats = pd.DataFrame(rows)

    # Reorder columns for readability
    col_order = ["instrument_group", "count"]
    for m in metrics:
        col_order += [f"{m}_mean", f"{m}_median", f"{m}_std"]
    stats = stats[col_order]

    out = TABLES_DIR / "maturity_split_stats.csv"
    stats.to_csv(out, index=False)
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ensure_project_directories()
    print("Loading data ...")
    auctions, panel, investor = load_data()

    print("Plotting maturity split time series ...")
    p = plot_maturity_split_timeseries(panel)
    print(f"  -> {p}")

    print("Plotting dealer share by instrument ...")
    p = plot_dealer_share_by_instrument(auctions, investor)
    print(f"  -> {p}")

    print("Plotting bid-to-cover / tail by maturity ...")
    p = plot_btc_tail_by_maturity(auctions)
    print(f"  -> {p}")

    print("Building maturity split stats table ...")
    p = make_maturity_split_stats(auctions)
    print(f"  -> {p}")

    print("Done.")


if __name__ == "__main__":
    main()
