"""Stress-flag overlay: tag panel weeks with macro regime indicators.

Each flag marks a distinct macro environment that may condition the
dealer-bridge mechanism:

    qt_period               Fed is shrinking its SOMA Treasury portfolio
    tga_rebuild             Treasury is flooding bills to rebuild its cash balance
    weak_bank_absorption    Commercial banks pulling back from Treasuries
    risk_off_window         Auction tails indicate market stress / poor reception
"""

from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt          # noqa: E402
import matplotlib.dates as mdates        # noqa: E402
import numpy as np                       # noqa: E402
import pandas as pd                      # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
STRESS_FLAGS: list[str] = [
    "qt_period",
    "tga_rebuild",
    "weak_bank_absorption",
    "risk_off_window",
]

_CONSECUTIVE_WEEKS_THRESHOLD = 4

# Expanding-window quantile minimum observations before producing a value.
_EXPANDING_MIN_PERIODS = 13

# Historical QT date ranges based on Fed announcements.
# QT1: Announced 2017-06-14 (https://www.federalreserve.gov/newsevents/pressreleases/monetary20170614a.htm)
#       Began Oct 2017, ended Sept 2019 when the Fed resumed purchases.
# QT2: Announced 2022-05-04 (https://www.federalreserve.gov/newsevents/pressreleases/monetary20220504a.htm)
#       Began June 2022, still ongoing.
_QT_RANGES: list[tuple[str, str | None]] = [
    ("2017-10-01", "2019-09-30"),   # QT1
    ("2022-06-01", None),            # QT2 – ongoing
]


# ===================================================================
# 1.  add_stress_flags
# ===================================================================

def _consecutive_negative_runs(series: pd.Series, n: int = _CONSECUTIVE_WEEKS_THRESHOLD) -> pd.Series:
    """Return a boolean Series that is True when *series* has been negative
    for *n* or more consecutive observations (inclusive of the current row).

    Uses a rolling-window approach: a week qualifies when every value in the
    trailing window of size *n* is strictly negative.
    """
    is_negative = series.lt(0)
    # Rolling min over booleans: 1 only when all n values are True.
    rolling_all_neg = is_negative.rolling(window=n, min_periods=n).min()
    return rolling_all_neg.fillna(0).astype(bool)


def add_stress_flags(panel: pd.DataFrame) -> pd.DataFrame:
    """Add boolean stress-flag columns to the auction-week panel.

    Parameters
    ----------
    panel : DataFrame
        Must contain at minimum ``week_start``.  The function degrades
        gracefully when expected columns are missing (flags are set to False).

    Returns
    -------
    DataFrame
        A copy of *panel* with four new boolean columns, one per stress flag.
    """
    df = panel.copy()
    df["week_start"] = pd.to_datetime(df["week_start"])
    df = df.sort_values("week_start").reset_index(drop=True)

    # ---- qt_period --------------------------------------------------------
    # Based on Fed announcement dates for balance-sheet normalisation.
    # See _QT_RANGES constant for source URLs.
    qt_mask = pd.Series(False, index=df.index)
    for start, end in _QT_RANGES:
        qt_start = pd.Timestamp(start)
        if end is not None:
            qt_end = pd.Timestamp(end)
            qt_mask = qt_mask | (
                (df["week_start"] >= qt_start) & (df["week_start"] <= qt_end)
            )
        else:
            qt_mask = qt_mask | (df["week_start"] >= qt_start)
    df["qt_period"] = qt_mask

    # ---- tga_rebuild ------------------------------------------------------
    # Heavy bill-issuance weeks: bill_share > 0.65 AND awarded_amount_total
    # is above its expanding-window 75th percentile (no look-ahead bias).
    if {"bill_share", "awarded_amount_total"}.issubset(df.columns):
        expanding_p75 = (
            df["awarded_amount_total"]
            .expanding(min_periods=_EXPANDING_MIN_PERIODS)
            .quantile(0.75)
        )
        df["tga_rebuild"] = (
            df["bill_share"].gt(0.65) & df["awarded_amount_total"].gt(expanding_p75)
        )
    else:
        df["tga_rebuild"] = False

    # ---- weak_bank_absorption ---------------------------------------------
    # H.8 bank Treasury securities declining WoW for 4+ consecutive weeks.
    if "bank_treasury_securities" in df.columns:
        bank_chg = df["bank_treasury_securities"].diff()
        df["weak_bank_absorption"] = _consecutive_negative_runs(bank_chg)
    else:
        df["weak_bank_absorption"] = False

    # ---- risk_off_window --------------------------------------------------
    # Weekly weighted tail (bp) above its expanding-window 90th percentile
    # (no look-ahead bias).
    if "weighted_tail_bp" in df.columns:
        expanding_p90 = (
            df["weighted_tail_bp"]
            .expanding(min_periods=_EXPANDING_MIN_PERIODS)
            .quantile(0.90)
        )
        df["risk_off_window"] = df["weighted_tail_bp"].gt(expanding_p90)
    else:
        df["risk_off_window"] = False

    return df


# ===================================================================
# 2.  summarize_stress_regimes
# ===================================================================

def summarize_stress_regimes(panel: pd.DataFrame) -> pd.DataFrame:
    """Compute summary statistics for each stress-flag regime.

    Parameters
    ----------
    panel : DataFrame
        Must already contain the four boolean stress-flag columns
        (call ``add_stress_flags`` first).

    Returns
    -------
    DataFrame
        One row per stress flag with columns:
        total_weeks_flagged, pct_of_sample, avg_inv_change_flagged,
        avg_inv_change_unflagged, bridge_rate_flagged, bridge_rate_unflagged.
    """
    # Filter to weeks with observed dealer data to avoid counting pre-2013
    # NaN-inventory weeks in bridge rate denominators.
    df = panel.copy()
    has_dealer = df.get("pd_treasury_inventory", pd.Series(dtype=float)).notna()
    df = df[has_dealer].copy()
    n_weeks = len(df)

    inv_change = df.get("inventory_change", pd.Series(np.nan, index=df.index))
    bridge = df.get("bridge_episode", pd.Series(False, index=df.index)).astype(bool)

    rows: list[dict] = []
    for flag in STRESS_FLAGS:
        if flag not in df.columns:
            continue
        mask = df[flag].astype(bool)
        total_flagged = int(mask.sum())
        total_unflagged = n_weeks - total_flagged

        rows.append(
            {
                "stress_flag": flag,
                "total_weeks_flagged": total_flagged,
                "pct_of_sample": round(total_flagged / n_weeks * 100, 2) if n_weeks else 0.0,
                "avg_inv_change_flagged": (
                    float(inv_change.loc[mask].mean()) if total_flagged else np.nan
                ),
                "avg_inv_change_unflagged": (
                    float(inv_change.loc[~mask].mean()) if total_unflagged else np.nan
                ),
                "bridge_rate_flagged": (
                    float(bridge.loc[mask].mean()) if total_flagged else np.nan
                ),
                "bridge_rate_unflagged": (
                    float(bridge.loc[~mask].mean()) if total_unflagged else np.nan
                ),
            }
        )

    return pd.DataFrame(rows)


# ===================================================================
# 3.  generate_stress_figures
# ===================================================================

_FLAG_COLORS = {
    "qt_period": "#d62728",
    "tga_rebuild": "#ff7f0e",
    "weak_bank_absorption": "#9467bd",
    "risk_off_window": "#2ca02c",
}

_FLAG_LABELS = {
    "qt_period": "QT period",
    "tga_rebuild": "TGA rebuild",
    "weak_bank_absorption": "Weak bank absorption",
    "risk_off_window": "Risk-off window",
}


def generate_stress_figures(
    panel: pd.DataFrame,
    figures_dir: str | Path,
) -> dict[str, Path]:
    """Produce two stress-regime visualisations and save to *figures_dir*.

    Returns a dict mapping short names to the Path of each saved figure.
    """
    figures_dir = Path(figures_dir)
    figures_dir.mkdir(parents=True, exist_ok=True)

    df = panel.copy()
    df["week_start"] = pd.to_datetime(df["week_start"])
    df = df.sort_values("week_start").reset_index(drop=True)

    result: dict[str, Path] = {}

    # ------------------------------------------------------------------
    # Figure 1: stress_regime_timeline.png
    # Horizontal coloured spans for each flag; dealer inventory overlaid.
    # ------------------------------------------------------------------
    fig, ax1 = plt.subplots(figsize=(14, 5))

    dates = df["week_start"]

    # Plot dealer inventory as a line (right y-axis might be busy; keep it
    # on primary axis but scale nicely).
    has_inventory = "pd_treasury_inventory" in df.columns and df["pd_treasury_inventory"].notna().any()
    if has_inventory:
        ax1.plot(dates, df["pd_treasury_inventory"], color="black", linewidth=0.8,
                 label="PD Treasury inventory", zorder=3)
        ax1.set_ylabel("PD Treasury inventory ($ millions)")
    else:
        # Fallback: use inventory_change cumsum as proxy.
        if "inventory_change" in df.columns:
            ax1.plot(dates, df["inventory_change"].cumsum(), color="black",
                     linewidth=0.8, label="Cumulative inv. change", zorder=3)
            ax1.set_ylabel("Cumulative inventory change")

    # Overlay horizontal colour bands for each stress flag.
    for i, flag in enumerate(STRESS_FLAGS):
        if flag not in df.columns:
            continue
        mask = df[flag].astype(bool)
        if not mask.any():
            continue
        color = _FLAG_COLORS[flag]
        label_used = False
        # Walk through contiguous True blocks.
        starts = mask.astype(int).diff().fillna(mask.astype(int))
        block_starts = df.index[starts == 1].tolist()
        block_ends_raw = df.index[starts == -1].tolist()
        # If the mask is True at the last row, close the final block.
        if mask.iloc[-1]:
            block_ends_raw.append(len(df))
        for bs, be in zip(block_starts, block_ends_raw):
            ax1.axvspan(
                dates.iloc[bs],
                dates.iloc[min(be, len(df) - 1)],
                alpha=0.18,
                color=color,
                label=_FLAG_LABELS.get(flag, flag) if not label_used else None,
                zorder=1,
            )
            label_used = True

    ax1.set_xlabel("Week")
    ax1.set_title("Stress Regime Timeline with Dealer Inventory")
    ax1.xaxis.set_major_locator(mdates.YearLocator(2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax1.legend(loc="upper left", fontsize=8)
    fig.tight_layout()

    path_timeline = figures_dir / "stress_regime_timeline.png"
    fig.savefig(path_timeline, dpi=150)
    plt.close(fig)
    result["stress_regime_timeline"] = path_timeline

    # ------------------------------------------------------------------
    # Figure 2: stress_bridge_conditioning.png
    # Bar chart: bridge episode rate under each condition.
    # ------------------------------------------------------------------
    bridge = df.get("bridge_episode", pd.Series(False, index=df.index)).astype(bool)

    categories: list[str] = []
    rates: list[float] = []

    # "No stress" = none of the four flags active.
    active_flags = [f for f in STRESS_FLAGS if f in df.columns]
    if active_flags:
        no_stress_mask = ~df[active_flags].any(axis=1)
    else:
        no_stress_mask = pd.Series(True, index=df.index)

    categories.append("No stress")
    rates.append(float(bridge.loc[no_stress_mask].mean()) if no_stress_mask.sum() else 0.0)

    # Individual flags.
    for flag in STRESS_FLAGS:
        if flag not in df.columns:
            continue
        mask = df[flag].astype(bool)
        categories.append(_FLAG_LABELS.get(flag, flag))
        rates.append(float(bridge.loc[mask].mean()) if mask.sum() else 0.0)

    # Multiple flags (2+ active simultaneously).
    if active_flags:
        multi_mask = df[active_flags].sum(axis=1) >= 2
        categories.append("Multiple flags")
        rates.append(float(bridge.loc[multi_mask].mean()) if multi_mask.sum() else 0.0)

    fig2, ax2 = plt.subplots(figsize=(9, 5))
    x_pos = np.arange(len(categories))
    colors = (
        ["#7f7f7f"]
        + [_FLAG_COLORS.get(f, "#1f77b4") for f in STRESS_FLAGS if f in df.columns]
        + (["#e377c2"] if active_flags else [])
    )
    ax2.bar(x_pos, rates, color=colors, edgecolor="white", linewidth=0.5)
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(categories, rotation=30, ha="right", fontsize=9)
    ax2.set_ylabel("Bridge episode rate (episodes / week)")
    ax2.set_title("Bridge Episode Rate by Stress Regime")
    ax2.set_ylim(bottom=0)
    fig2.tight_layout()

    path_conditioning = figures_dir / "stress_bridge_conditioning.png"
    fig2.savefig(path_conditioning, dpi=150)
    plt.close(fig2)
    result["stress_bridge_conditioning"] = path_conditioning

    return result


# ===================================================================
# 4.  generate_stress_table
# ===================================================================

def generate_stress_table(
    panel: pd.DataFrame,
    tables_dir: str | Path,
) -> Path:
    """Write the stress-regime summary table to *tables_dir*.

    Returns the Path to the saved CSV.
    """
    tables_dir = Path(tables_dir)
    tables_dir.mkdir(parents=True, exist_ok=True)

    summary = summarize_stress_regimes(panel)
    out_path = tables_dir / "stress_regime_summary.csv"
    summary.to_csv(out_path, index=False)
    return out_path


# ===================================================================
# CLI convenience (python -m bidbridge.features.stress_flags)
# ===================================================================

def _main() -> None:  # pragma: no cover
    from bidbridge.paths import FIGURES_DIR, PROCESSED_DIR, TABLES_DIR

    panel_path = PROCESSED_DIR / "auction_week_panel.csv"
    panel = pd.read_csv(panel_path)
    panel = add_stress_flags(panel)

    summary = summarize_stress_regimes(panel)
    print(summary.to_string(index=False))

    figs = generate_stress_figures(panel, FIGURES_DIR)
    for name, path in figs.items():
        print(f"  Saved figure: {path}")

    tbl = generate_stress_table(panel, TABLES_DIR)
    print(f"  Saved table:  {tbl}")


if __name__ == "__main__":
    _main()
