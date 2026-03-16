"""Bridge metrics for the auction-week panel.

All thresholds use expanding historical windows so that the classification
at time t depends only on data available up to time t (no look-ahead).
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.replace({0: pd.NA})
    return numerator / denominator


def add_bridge_metrics(panel: pd.DataFrame) -> pd.DataFrame:
    """Add bridge metrics and episode flags to the auction-week panel.

    Bridge episode definition:
      A week where (1) supply exceeds its expanding historical median,
      (2) dealer inventory increases, and (3) the increase is unusually
      large relative to a trailing 13-week window (z > 1).

    All percentile thresholds use expanding windows so they are time-feasible:
    the threshold at week t is computed from weeks 1..t only.
    """
    df = panel.copy().sort_values("week_start").reset_index(drop=True)

    # --- Inventory change ---
    if "pd_treasury_inventory" in df.columns:
        df["inventory_change"] = df["pd_treasury_inventory"].diff()
    else:
        df["inventory_change"] = np.nan

    # --- Dealer bridge ratio ---
    if "awarded_amount_total" in df.columns:
        awarded_M = df["awarded_amount_total"] / 1e6
        df["dealer_bridge_ratio"] = safe_divide(df["inventory_change"], awarded_M)

        # Rolling 52-week median: adapts to regime shifts in issuance levels
        # (e.g., the post-2020 structural increase in supply). An expanding
        # window becomes non-discriminating when issuance levels shift up
        # permanently. The rolling window keeps the threshold current while
        # still being time-feasible (uses only past data via min_periods).
        rolling_median = df["awarded_amount_total"].rolling(
            window=52, min_periods=13,
        ).median()
        df["heavy_supply"] = df["awarded_amount_total"] >= rolling_median
    else:
        df["dealer_bridge_ratio"] = np.nan
        df["heavy_supply"] = False

    # --- Financing intensity ---
    if {"pd_financing_usage", "pd_treasury_inventory"}.issubset(df.columns):
        df["financing_intensity"] = safe_divide(
            df["pd_financing_usage"], df["pd_treasury_inventory"]
        )
    else:
        df["financing_intensity"] = np.nan

    # --- Inventory persistence (2-week rolling mean of changes) ---
    if "inventory_change" in df.columns:
        df["inventory_persistence_proxy"] = (
            df["inventory_change"].rolling(window=2, min_periods=1).mean()
        )
    else:
        df["inventory_persistence_proxy"] = np.nan

    # --- Weak end-investor absorption (rolling 52-week p25) ---
    if "nondealer_share" in df.columns:
        rolling_p25 = df["nondealer_share"].rolling(window=52, min_periods=13).quantile(0.25)
        df["weak_end_investor_absorption"] = df["nondealer_share"] <= rolling_p25
    else:
        df["weak_end_investor_absorption"] = False

    # --- Inventory accumulation z-score (trailing 13-week window) ---
    rolling_mean = df["inventory_change"].rolling(13, min_periods=4).mean()
    rolling_std = df["inventory_change"].rolling(13, min_periods=4).std().replace(0, np.nan)
    df["inventory_change_zscore"] = (df["inventory_change"] - rolling_mean) / rolling_std

    # --- Bridge episode ---
    df["bridge_episode"] = (
        df["heavy_supply"]
        & (df["inventory_change"] > 0)
        & (df["inventory_change_zscore"] > 1.0)
    )

    return df
