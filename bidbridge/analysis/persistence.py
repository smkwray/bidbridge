"""Persistence analysis for dealer inventory after bridge episodes.

Measures how long dealer inventory stays elevated after bridge episodes,
supporting the "persistence followed by normalization" thesis (observable
signature #4). Provides impulse-response functions, half-life estimation
via exponential decay, and autocorrelation diagnostics.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Impulse response
# ---------------------------------------------------------------------------

def compute_episode_impulse_response(
    panel: pd.DataFrame,
    window: int = 12,
) -> pd.DataFrame:
    """Average inventory-change impulse response around bridge episodes.

    For each bridge episode, extracts inventory_change in a [-4, +window]
    week window (relative to the episode week), then averages across all
    episodes to produce the cumulative impulse response.

    Parameters
    ----------
    panel : DataFrame
        The auction-week panel with ``bridge_episode`` and ``inventory_change``.
    window : int
        Number of post-episode weeks to include (default 12).

    Returns
    -------
    DataFrame
        Columns: event_week, avg_inv_change, cumulative_change, se, n_episodes.
    """
    p = panel.reset_index(drop=True)
    bridge_idx = p.index[p["bridge_episode"].astype(bool)].tolist()

    if not bridge_idx:
        logger.warning("No bridge episodes found in panel.")
        return pd.DataFrame(
            columns=["event_week", "avg_inv_change", "cumulative_change", "se", "n_episodes"],
        )

    pre_window = 4
    records: list[dict] = []
    for idx in bridge_idx:
        for w in range(-pre_window, window + 1):
            row_idx = idx + w
            if 0 <= row_idx < len(p):
                val = p.loc[row_idx, "inventory_change"]
                if pd.notna(val):
                    records.append({"event_week": w, "inv_change": val})

    edf = pd.DataFrame(records)
    if edf.empty:
        return pd.DataFrame(
            columns=["event_week", "avg_inv_change", "cumulative_change", "se", "n_episodes"],
        )

    agg = (
        edf.groupby("event_week")["inv_change"]
        .agg(["mean", "std", "count"])
        .reset_index()
    )
    agg.columns = ["event_week", "avg_inv_change", "std", "n_episodes"]
    agg["se"] = agg["std"] / np.sqrt(agg["n_episodes"])
    agg["cumulative_change"] = agg["avg_inv_change"].cumsum()

    return agg[["event_week", "avg_inv_change", "cumulative_change", "se", "n_episodes"]]


# ---------------------------------------------------------------------------
# 2. Half-life via exponential decay
# ---------------------------------------------------------------------------

def compute_inventory_halflife(panel: pd.DataFrame) -> dict:
    """Estimate how quickly abnormal inventory decays after bridge episodes.

    For each bridge episode at time t, compute the *detrended* excess
    inventory level:
        excess[k] = detrended_inventory[t+k] - detrended_inventory[t-1]

    where detrended_inventory = pd_treasury_inventory minus a 26-week
    rolling mean (removing the secular balance-sheet trend). Then fit
    an exponential decay:
        avg_excess[k] = A * exp(-k / tau) for k = 0..12

    Detrending prevents the secular growth in dealer inventories ($98B to
    $500B over the sample) from dominating the decay estimate.

    Returns
    -------
    dict
        Keys: halflife_weeks, decay_rate (1/tau), A_initial, r_squared.
    """
    p = panel.reset_index(drop=True)
    bridge_idx = p.index[p["bridge_episode"].astype(bool)].tolist()

    if "pd_treasury_inventory" not in p.columns:
        return _empty_halflife()

    # Detrend: remove 26-week rolling mean to isolate cyclical component
    inv_raw = p["pd_treasury_inventory"].astype(float)
    trend = inv_raw.rolling(26, min_periods=4).mean()
    detrended = inv_raw - trend

    max_lag = 12
    lag_values: dict[int, list[float]] = {k: [] for k in range(max_lag + 1)}

    for idx in bridge_idx:
        baseline_idx = idx - 1
        if baseline_idx < 0:
            continue
        baseline = detrended.iloc[baseline_idx]
        if pd.isna(baseline):
            continue

        for k in range(max_lag + 1):
            row_idx = idx + k
            if 0 <= row_idx < len(p):
                val = detrended.iloc[row_idx]
                if pd.notna(val):
                    lag_values[k].append(val - baseline)

    # Compute average excess inventory at each lag
    lags = []
    means = []
    for k in sorted(lag_values):
        vals = lag_values[k]
        if vals:
            lags.append(k)
            means.append(np.mean(vals))

    lags_arr = np.array(lags, dtype=float)
    means_arr = np.array(means, dtype=float)

    if len(means_arr) < 3 or means_arr[0] <= 0:
        logger.warning("Insufficient or non-positive excess inventory for half-life fit.")
        return _empty_halflife()

    # Only keep strictly positive for log transform
    mask = means_arr > 0
    if mask.sum() < 3:
        return _empty_halflife()

    log_y = np.log(means_arr[mask])
    k_fit = lags_arr[mask]

    # log(excess) = log(A) - k / tau
    X = np.column_stack([np.ones(len(k_fit)), -k_fit])
    coeffs, _, _, _ = np.linalg.lstsq(X, log_y, rcond=None)

    log_A = coeffs[0]
    inv_tau = coeffs[1]

    A_initial = np.exp(log_A)
    tau = 1.0 / inv_tau if inv_tau > 0 else np.nan
    halflife = tau * np.log(2) if np.isfinite(tau) else np.nan
    decay_rate = inv_tau if inv_tau > 0 else np.nan

    y_hat = X @ coeffs
    ss_res = np.sum((log_y - y_hat) ** 2)
    ss_tot = np.sum((log_y - log_y.mean()) ** 2)
    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    return {
        "halflife_weeks": float(halflife),
        "decay_rate": float(decay_rate),
        "A_initial": float(A_initial),
        "r_squared": float(r_squared),
    }


def _empty_halflife() -> dict:
    return {
        "halflife_weeks": np.nan,
        "decay_rate": np.nan,
        "A_initial": np.nan,
        "r_squared": np.nan,
    }


# ---------------------------------------------------------------------------
# 3. Autocorrelation
# ---------------------------------------------------------------------------

def compute_autocorrelation(panel: pd.DataFrame, max_lag: int = 12) -> pd.DataFrame:
    """Autocorrelation and partial autocorrelation of inventory_change.

    Parameters
    ----------
    panel : DataFrame
        The auction-week panel.
    max_lag : int
        Maximum lag to compute (default 12).

    Returns
    -------
    DataFrame
        Columns: lag, acf, pacf, acf_ci_upper, acf_ci_lower.
    """
    series = panel["inventory_change"].dropna().to_numpy()
    n = len(series)

    if n < max_lag + 1:
        logger.warning("Series too short for autocorrelation at max_lag=%d", max_lag)
        return pd.DataFrame(columns=["lag", "acf", "pacf", "acf_ci_upper", "acf_ci_lower"])

    mean = series.mean()
    var = np.sum((series - mean) ** 2) / n  # biased variance (standard for ACF)

    # ACF
    acf_vals = np.empty(max_lag)
    for k in range(1, max_lag + 1):
        cov_k = np.sum((series[: n - k] - mean) * (series[k:] - mean)) / n
        acf_vals[k - 1] = cov_k / var if var > 0 else 0.0

    # Bartlett 95% CI:  +/- 1.96 / sqrt(n)
    ci_bound = 1.96 / np.sqrt(n)

    # PACF via Durbin-Levinson recursion
    pacf_vals = np.empty(max_lag)
    phi_prev = np.array([], dtype=float)

    for k in range(1, max_lag + 1):
        if k == 1:
            pacf_vals[0] = acf_vals[0]
            phi_prev = np.array([acf_vals[0]])
        else:
            # phi_{k,k} = (r_k - sum phi_{k-1,j} * r_{k-j}) / (1 - sum phi_{k-1,j} * r_j)
            num = acf_vals[k - 1] - np.sum(phi_prev * acf_vals[k - 2 :: -1][: len(phi_prev)])
            den = 1.0 - np.sum(phi_prev * acf_vals[: len(phi_prev)])
            phi_kk = num / den if abs(den) > 1e-12 else 0.0
            pacf_vals[k - 1] = phi_kk

            # Update phi for next iteration
            phi_new = phi_prev - phi_kk * phi_prev[::-1]
            phi_prev = np.append(phi_new, phi_kk)

    lags = np.arange(1, max_lag + 1)
    return pd.DataFrame({
        "lag": lags,
        "acf": acf_vals,
        "pacf": pacf_vals,
        "acf_ci_upper": ci_bound,
        "acf_ci_lower": -ci_bound,
    })


# ---------------------------------------------------------------------------
# 4. Figures
# ---------------------------------------------------------------------------

def generate_persistence_figures(
    panel: pd.DataFrame,
    figures_dir: Path,
) -> dict[str, Path]:
    """Generate three persistence-analysis figures.

    Returns a dict mapping figure name to its Path.
    """
    figures_dir = Path(figures_dir)
    figures_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    # ---- Figure 1: Cumulative impulse response with confidence bands ----
    irf = compute_episode_impulse_response(panel, window=12)
    if not irf.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        weeks = irf["event_week"].to_numpy()
        cumul = irf["cumulative_change"].to_numpy()
        # Cumulative SE via error propagation (sqrt of cumulative sum of se^2)
        se_cum = np.sqrt(np.cumsum(irf["se"].to_numpy() ** 2))

        ax.plot(weeks, cumul, "o-", color="steelblue", linewidth=1.5, markersize=4, label="Cumulative response")
        ax.fill_between(
            weeks,
            cumul - 1.96 * se_cum,
            cumul + 1.96 * se_cum,
            alpha=0.2,
            color="steelblue",
            label="95% CI",
        )
        ax.axhline(0, color="gray", linewidth=0.5, linestyle="--")
        ax.axvline(0, color="red", linewidth=1, linestyle="--", alpha=0.5, label="Bridge episode")
        ax.set_xlabel("Weeks relative to bridge episode")
        ax.set_ylabel("Cumulative inventory change ($M)")
        ax.set_title("Impulse response: Dealer inventory after bridge episodes")
        ax.legend()
        plt.tight_layout()
        out = figures_dir / "persistence_impulse_response.png"
        fig.savefig(out, dpi=150)
        plt.close(fig)
        paths["impulse_response"] = out
        logger.info("Saved %s", out)
    else:
        logger.warning("Skipping impulse response figure: no data.")

    # ---- Figure 2: ACF and PACF bar charts (2 panels) ----
    acf_df = compute_autocorrelation(panel, max_lag=12)
    if not acf_df.empty:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

        lags = acf_df["lag"].to_numpy()
        acf_vals = acf_df["acf"].to_numpy()
        pacf_vals = acf_df["pacf"].to_numpy()
        ci_upper = acf_df["acf_ci_upper"].iloc[0]

        # ACF panel
        ax1.bar(lags, acf_vals, color="steelblue", alpha=0.7, width=0.6)
        ax1.axhline(ci_upper, color="red", linewidth=0.8, linestyle="--", alpha=0.6, label="95% CI")
        ax1.axhline(-ci_upper, color="red", linewidth=0.8, linestyle="--", alpha=0.6)
        ax1.axhline(0, color="gray", linewidth=0.5)
        ax1.set_xlabel("Lag (weeks)")
        ax1.set_ylabel("Autocorrelation")
        ax1.set_title("ACF of inventory change")
        ax1.set_xticks(lags)
        ax1.legend()

        # PACF panel
        ax2.bar(lags, pacf_vals, color="darkorange", alpha=0.7, width=0.6)
        ax2.axhline(ci_upper, color="red", linewidth=0.8, linestyle="--", alpha=0.6, label="95% CI")
        ax2.axhline(-ci_upper, color="red", linewidth=0.8, linestyle="--", alpha=0.6)
        ax2.axhline(0, color="gray", linewidth=0.5)
        ax2.set_xlabel("Lag (weeks)")
        ax2.set_ylabel("Partial autocorrelation")
        ax2.set_title("PACF of inventory change")
        ax2.set_xticks(lags)
        ax2.legend()

        plt.tight_layout()
        out = figures_dir / "persistence_autocorrelation.png"
        fig.savefig(out, dpi=150)
        plt.close(fig)
        paths["autocorrelation"] = out
        logger.info("Saved %s", out)
    else:
        logger.warning("Skipping autocorrelation figure: no data.")

    # ---- Figure 3: Spaghetti plot of individual episode traces + average ----
    p = panel.reset_index(drop=True)
    bridge_idx = p.index[p["bridge_episode"].astype(bool)].tolist()
    window = 12
    pre_window = 4

    if bridge_idx:
        fig, ax = plt.subplots(figsize=(12, 6))

        # Individual episode traces
        for i, idx in enumerate(bridge_idx):
            weeks_trace = []
            vals_trace = []
            for w in range(-pre_window, window + 1):
                row_idx = idx + w
                if 0 <= row_idx < len(p):
                    val = p.loc[row_idx, "inventory_change"]
                    if pd.notna(val):
                        weeks_trace.append(w)
                        vals_trace.append(val)
            if weeks_trace:
                label = "Individual episodes" if i == 0 else None
                ax.plot(
                    weeks_trace, vals_trace,
                    color="gray", alpha=0.15, linewidth=0.7, label=label,
                )

        # Average trace
        if not irf.empty:
            ax.plot(
                irf["event_week"], irf["avg_inv_change"],
                "o-", color="steelblue", linewidth=2.5, markersize=5,
                label="Episode average", zorder=10,
            )
            # SE band around the average
            avg = irf["avg_inv_change"].to_numpy()
            se = irf["se"].to_numpy()
            ax.fill_between(
                irf["event_week"],
                avg - 1.96 * se,
                avg + 1.96 * se,
                alpha=0.25, color="steelblue", zorder=9,
            )

        ax.axhline(0, color="gray", linewidth=0.5, linestyle="--")
        ax.axvline(0, color="red", linewidth=1, linestyle="--", alpha=0.5, label="Bridge episode")
        ax.set_xlabel("Weeks relative to bridge episode")
        ax.set_ylabel("Inventory change ($M)")
        ax.set_title(f"Individual episode inventory paths (n={len(bridge_idx)} episodes)")
        ax.legend(loc="upper right")
        plt.tight_layout()
        out = figures_dir / "persistence_episode_traces.png"
        fig.savefig(out, dpi=150)
        plt.close(fig)
        paths["episode_traces"] = out
        logger.info("Saved %s", out)
    else:
        logger.warning("Skipping episode traces figure: no bridge episodes.")

    return paths


# ---------------------------------------------------------------------------
# 5. Summary table
# ---------------------------------------------------------------------------

def generate_persistence_table(
    panel: pd.DataFrame,
    tables_dir: Path,
) -> Path:
    """Generate persistence_summary.csv with key persistence statistics.

    Columns: halflife_weeks, decay_rate, mean_episode_duration,
    peak_inventory_change, weeks_to_normalization.

    Returns the Path to the written CSV.
    """
    tables_dir = Path(tables_dir)
    tables_dir.mkdir(parents=True, exist_ok=True)

    hl = compute_inventory_halflife(panel)

    p = panel.reset_index(drop=True)
    bridge_idx = p.index[p["bridge_episode"].astype(bool)].tolist()

    # ---- Mean episode duration (consecutive bridge_episode=True runs) ----
    episodes_flag = p["bridge_episode"].astype(bool).to_numpy()
    run_lengths: list[int] = []
    current_run = 0
    for val in episodes_flag:
        if val:
            current_run += 1
        else:
            if current_run > 0:
                run_lengths.append(current_run)
            current_run = 0
    if current_run > 0:
        run_lengths.append(current_run)
    mean_episode_duration = float(np.mean(run_lengths)) if run_lengths else np.nan

    # ---- Peak inventory change at bridge episodes ----
    if bridge_idx:
        ep_inv = p.loc[bridge_idx, "inventory_change"]
        peak_inventory_change = float(ep_inv.max())
    else:
        peak_inventory_change = np.nan

    # ---- Weeks to normalization ----
    # Average the post-episode inventory change path and find the first week
    # where the cumulative change crosses back toward zero (or the trailing
    # mean), approximating normalization.
    irf = compute_episode_impulse_response(panel, window=12)
    weeks_to_norm = np.nan
    if not irf.empty:
        post = irf[irf["event_week"] > 0].reset_index(drop=True)
        if not post.empty:
            # Look for the first post-episode week where cumulative change
            # stops increasing (i.e. avg_inv_change flips sign or is close
            # to zero relative to the episode-week level).
            trailing_mean = panel["inventory_change"].mean()
            for _, row in post.iterrows():
                if row["avg_inv_change"] <= trailing_mean:
                    weeks_to_norm = float(row["event_week"])
                    break
            # If it never crosses, report the full window length
            if np.isnan(weeks_to_norm):
                weeks_to_norm = float(post["event_week"].max())

    summary = pd.DataFrame([{
        "halflife_weeks": round(hl["halflife_weeks"], 2) if np.isfinite(hl["halflife_weeks"]) else np.nan,
        "decay_rate": round(hl["decay_rate"], 4) if np.isfinite(hl["decay_rate"]) else np.nan,
        "A_initial": round(hl["A_initial"], 2) if np.isfinite(hl["A_initial"]) else np.nan,
        "r_squared": round(hl["r_squared"], 4) if np.isfinite(hl["r_squared"]) else np.nan,
        "mean_episode_duration": round(mean_episode_duration, 2) if np.isfinite(mean_episode_duration) else np.nan,
        "peak_inventory_change": round(peak_inventory_change, 2) if np.isfinite(peak_inventory_change) else np.nan,
        "weeks_to_normalization": round(weeks_to_norm, 1) if np.isfinite(weeks_to_norm) else np.nan,
        "n_bridge_episodes": len(bridge_idx),
    }])

    out = tables_dir / "persistence_summary.csv"
    summary.to_csv(out, index=False)
    logger.info("Saved persistence summary to %s", out)
    return out
