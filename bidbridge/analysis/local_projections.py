"""Jorda-style local projections for dealer inventory impulse responses.

Estimates the dynamic causal effect of *ex ante* supply shocks on cumulative
dealer inventory changes using the local projections (LP) method of Jorda
(2005).  For each horizon h = 0, 1, ..., H a separate OLS regression is
estimated:

    cum_inv_change_{t,t+h} = alpha_h
                            + beta_h  * shock_t
                            + theta_h * shock_t * soft_demand_{t-1}
                            + Gamma_h * X_{t-1}
                            + u_{t+h}

The supply shock is defined using ONLY pre-auction information:

    announced_supply_shock_t = 1{announced_amount_total_t > expanding-p75_t}

The interaction ``shock * lagged_soft_demand`` captures the "bridge pressure"
treatment -- weeks where large announced supply coincides with weak
end-investor demand (non-dealer share below its expanding-window p25, lagged
one week to ensure strict ex ante timing).

Standard errors use HAC (Newey-West / Bartlett kernel) via statsmodels to
account for serial correlation from overlapping horizons.

Regime effects are identified via interaction terms (``shock * qt_period``)
on the full contiguous panel.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt          # noqa: E402
import matplotlib.gridspec as gridspec   # noqa: E402
import numpy as np                       # noqa: E402
import pandas as pd                      # noqa: E402
import statsmodels.api as sm             # noqa: E402

from bidbridge.features.stress_flags import add_stress_flags

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 0.  Ex ante supply shock construction (expanding-window, centralized)
# ---------------------------------------------------------------------------

def compute_announced_supply_shock(panel: pd.DataFrame) -> pd.Series:
    """Return a boolean Series flagging ex ante supply-shock weeks.

    A week qualifies as a supply shock when ``announced_amount_total``
    (the offering amount known *before* the auction) exceeds its
    **expanding-window** 75th percentile -- i.e., the threshold at time *t*
    is computed using only data from the start of the sample through *t*.

    This avoids (a) using any auction *outcome* (bid-to-cover, tails, etc.)
    in the shock definition and (b) using future information to set the
    threshold.

    Note: the expanding quantile at time *t* includes *t* itself, so the
    current week's announced supply participates in its own threshold.
    This is standard for expanding-window indicators and does not
    constitute look-ahead bias (no future data is used), but it means
    the threshold is not strictly "prior history only."  Shifting the
    quantile by one row would give a purely pre-t threshold at the cost
    of losing the first observation; the current choice is deliberate.

    Parameters
    ----------
    panel : DataFrame
        Must be sorted by ``week_start`` and contain
        ``announced_amount_total``.

    Returns
    -------
    pd.Series[bool]
        Aligned with *panel*'s index.
    """
    supply = panel["announced_amount_total"].astype(float)

    # Expanding-window 75th percentile (uses only data up to and including t)
    expanding_p75 = supply.expanding(min_periods=1).quantile(0.75)

    shock = supply > expanding_p75
    return shock


def _compute_lagged_soft_demand(panel: pd.DataFrame) -> pd.Series:
    """Return a boolean Series for soft end-investor demand, lagged 1 week.

    Soft demand = nondealer_share below its expanding-window 25th percentile.
    The result is shifted by +1 (i.e., the value at row t reflects the
    condition at t-1) so that only pre-auction information is used.

    Parameters
    ----------
    panel : DataFrame
        Must be sorted by ``week_start`` and contain ``nondealer_share``.

    Returns
    -------
    pd.Series[bool]
        Aligned with *panel*'s index.  First row is NaN -> False after fill.
    """
    nd_share = panel["nondealer_share"].astype(float)
    expanding_p25 = nd_share.expanding(min_periods=1).quantile(0.25)

    soft = nd_share < expanding_p25
    # Lag by 1 week (shift forward in time so row t gets t-1 value)
    lagged_soft = soft.shift(1).fillna(False).astype(bool)
    return lagged_soft


def _prepare_lp_panel(panel: pd.DataFrame) -> pd.DataFrame:
    """Prepare a sorted panel with shock variables and lagged controls.

    Centralizes all variable construction so that ``run_local_projections``
    and ``run_local_projections_by_regime`` use identical definitions.

    Parameters
    ----------
    panel : DataFrame
        Raw auction-week panel.

    Returns
    -------
    DataFrame
        Sorted copy with additional columns:
        ``shock``, ``lagged_soft_demand``, ``shock_x_soft``,
        ``L_supply_M``, ``L_dealer_share``, ``L_trend_years``,
        ``L_soma_change_B``, ``qt_period``, ``shock_x_qt``.
    """
    df = add_stress_flags(panel)
    df["week_start"] = pd.to_datetime(df["week_start"])
    df = df.sort_values("week_start").reset_index(drop=True)

    # ---- Shock variables (ex ante, expanding-window) ----------------------
    df["shock"] = compute_announced_supply_shock(df).astype(int)
    df["lagged_soft_demand"] = _compute_lagged_soft_demand(df).astype(int)
    df["shock_x_soft"] = df["shock"] * df["lagged_soft_demand"]

    # ---- Controls (all lagged by 1 week) ----------------------------------
    df["supply_M"] = df["announced_amount_total"] / 1e6
    df["L_supply_M"] = df["supply_M"].shift(1)

    df["L_dealer_share"] = df["dealer_share_allotment"].shift(1)

    df["trend_years"] = (
        (df["week_start"] - df["week_start"].min()).dt.days / 365.25
    )
    df["L_trend_years"] = df["trend_years"].shift(1)

    # SOMA weekly change in billions
    if "soma_treasury_total" in df.columns:
        df["soma_change_B"] = df["soma_treasury_total"].diff() / 1e9
    else:
        df["soma_change_B"] = 0.0
    df["L_soma_change_B"] = df["soma_change_B"].shift(1)

    # ---- Regime variable --------------------------------------------------
    df["qt_period"] = df["qt_period"].astype(int)
    df["shock_x_qt"] = df["shock"] * df["qt_period"]

    return df


# ---------------------------------------------------------------------------
# 1.  Cumulative outcome builder
# ---------------------------------------------------------------------------

def _cumulative_outcome(
    df: pd.DataFrame,
    h: int,
    outcome: str = "inventory_change",
) -> pd.Series:
    """Compute cumulative outcome from t to t+h (inclusive).

    cum_y_{t,t+h} = sum_{j=0}^{h} outcome_{t+j}

    Uses a forward-rolling sum so that the value at row t is the sum of
    outcome[t], outcome[t+1], ..., outcome[t+h].

    Parameters
    ----------
    df : DataFrame
        Must contain *outcome* column, sorted by ``week_start``.
    h : int
        Horizon (number of additional weeks).
    outcome : str
        Column name of the per-period outcome.

    Returns
    -------
    pd.Series
        Aligned with *df*'s index.  Rows near the end will be NaN.
    """
    raw = df[outcome].astype(float)
    if h == 0:
        return raw.copy()
    # Reverse, do rolling sum of (h+1), reverse back
    cum = raw[::-1].rolling(window=h + 1, min_periods=h + 1).sum()[::-1]
    return cum.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 2.  Contiguity mask
# ---------------------------------------------------------------------------

def _contiguity_mask(df: pd.DataFrame, h: int) -> pd.Series:
    """Return a boolean mask where the observation at t+h is exactly h weeks
    after t, ensuring no calendar gaps corrupt the shifted outcome."""
    if h == 0:
        return pd.Series(True, index=df.index)
    future_week = df["week_start"].shift(-h)
    expected_gap = pd.Timedelta(weeks=h)
    return (future_week - df["week_start"]) == expected_gap


# ---------------------------------------------------------------------------
# 3.  Core local projection estimator
# ---------------------------------------------------------------------------

_CONTROL_COLS = ["L_supply_M", "L_dealer_share", "L_trend_years", "L_soma_change_B"]


def _run_projection_spec(
    df: pd.DataFrame,
    shock_col: str,
    interaction_col: str,
    max_horizon: int,
    outcome: str,
    regime_label: str,
) -> pd.DataFrame:
    """Estimate a local projection path for a chosen shock specification."""
    rows: list[dict[str, Any]] = []
    working = df.copy()

    for h in range(max_horizon + 1):
        working[f"_cum_y_h{h}"] = _cumulative_outcome(working, h, outcome=outcome)
        contig = _contiguity_mask(working, h)

        est_cols = [f"_cum_y_h{h}", shock_col, interaction_col] + _CONTROL_COLS
        mask = working[est_cols].notna().all(axis=1) & contig
        est = working.loc[mask].copy()
        if len(est) < 10:
            working.drop(columns=[f"_cum_y_h{h}"], inplace=True)
            continue

        y = est[f"_cum_y_h{h}"].astype(float)
        X_cols = [shock_col, interaction_col] + _CONTROL_COLS
        X = sm.add_constant(est[X_cols].astype(float))

        model = sm.OLS(y, X).fit(
            cov_type="HAC",
            cov_kwds={"maxlags": h + 1},
        )
        ci = model.conf_int().loc[shock_col]
        rows.append({
            "horizon": h,
            "beta": model.params[shock_col],
            "se": model.bse[shock_col],
            "t_stat": model.tvalues[shock_col],
            "p_value": model.pvalues[shock_col],
            "ci_lower": ci.iloc[0],
            "ci_upper": ci.iloc[1],
            "n_obs": int(model.nobs),
            "r_squared": model.rsquared,
            "regime": regime_label,
        })
        working.drop(columns=[f"_cum_y_h{h}"], inplace=True)

    return pd.DataFrame(rows)


def run_local_projections(
    panel: pd.DataFrame,
    max_horizon: int = 12,
    outcome: str = "inventory_change",
) -> pd.DataFrame:
    """Estimate cumulative impulse response of dealer inventory to ex ante
    supply shocks via local projections.

    Specification per horizon h:
        cum_inv_change_{t,t+h} = alpha_h + beta_h * shock_t
                                + theta_h * shock_t * soft_demand_{t-1}
                                + Gamma_h * X_{t-1} + u_{t+h}

    Parameters
    ----------
    panel : DataFrame
        The auction-week panel.
    max_horizon : int
        Maximum horizon h (inclusive).
    outcome : str
        Per-period outcome column (cumulated internally).

    Returns
    -------
    DataFrame
        One row per horizon with columns: horizon, beta, se, t_stat, p_value,
        ci_lower, ci_upper, n_obs, r_squared, regime.
    """
    df = _prepare_lp_panel(panel)

    return _run_projection_spec(
        df,
        shock_col="shock",
        interaction_col="shock_x_soft",
        max_horizon=max_horizon,
        outcome=outcome,
        regime_label="full_sample",
    )


def run_local_projection_placebos(
    panel: pd.DataFrame,
    max_horizon: int = 12,
    outcome: str = "inventory_change",
) -> pd.DataFrame:
    """Run lead and shifted-event placebo LP specifications."""
    df = _prepare_lp_panel(panel)
    placebo_specs = [
        ("lead1_shock", "lead1_shock_x_soft", "lead_placebo_h1", -1),
        ("lead4_shock", "lead4_shock_x_soft", "shifted_placebo_h4", -4),
    ]

    results: list[pd.DataFrame] = []
    for shock_col, interact_col, label, shift in placebo_specs:
        df[shock_col] = df["shock"].shift(shift).fillna(0).astype(int)
        df[interact_col] = df[shock_col] * df["lagged_soft_demand"]
        placebo_df = _run_projection_spec(
            df,
            shock_col=shock_col,
            interaction_col=interact_col,
            max_horizon=max_horizon,
            outcome=outcome,
            regime_label=label,
        )
        if not placebo_df.empty:
            placebo_df["placebo_type"] = label
            results.append(placebo_df)

    if results:
        return pd.concat(results, ignore_index=True)
    return pd.DataFrame(columns=[
        "horizon", "beta", "se", "t_stat", "p_value", "ci_lower",
        "ci_upper", "n_obs", "r_squared", "regime", "placebo_type",
    ])


# ---------------------------------------------------------------------------
# 4.  Regime-conditional local projections (interaction approach)
# ---------------------------------------------------------------------------

def run_local_projections_by_regime(
    panel: pd.DataFrame,
    max_horizon: int = 12,
) -> dict[str, pd.DataFrame]:
    """Run local projections with QT-regime interaction on the full panel.

    Specification per horizon h:
        cum_inv_change_{t,t+h} = alpha_h + beta_h * shock_t
                                + theta_h * shock_t * soft_demand_{t-1}
                                + delta_h * shock_t * qt_period_t
                                + Gamma_h * X_{t-1}
                                + phi_h * qt_period_t + u_{t+h}

    - beta_h captures the baseline (non-QT) supply shock effect.
    - beta_h + delta_h captures the total QT-period shock effect, tested via
      statsmodels ``t_test('shock + shock_x_qt = 0')``.

    Returns
    -------
    dict[str, DataFrame]
        Keys: ``"full_sample"``, ``"qt_period"``, ``"non_qt_period"``.
    """
    df = _prepare_lp_panel(panel)
    outcome = "inventory_change"

    qt_has_variation = df["qt_period"].nunique() > 1
    if not qt_has_variation:
        logger.warning("qt_period has no variation; falling back to full-sample LP.")
        full = run_local_projections(panel, max_horizon=max_horizon, outcome=outcome)
        return {
            "full_sample": full,
            "non_qt_period": full.copy(),
            "qt_period": pd.DataFrame(
                columns=["horizon", "beta", "se", "t_stat", "p_value",
                         "ci_lower", "ci_upper", "n_obs", "r_squared", "regime"]
            ),
        }

    baseline_rows: list[dict[str, Any]] = []
    qt_rows: list[dict[str, Any]] = []

    for h in range(max_horizon + 1):
        df[f"_cum_y_h{h}"] = _cumulative_outcome(df, h, outcome=outcome)
        contig = _contiguity_mask(df, h)

        est_cols = (
            [f"_cum_y_h{h}", "shock", "shock_x_soft", "shock_x_qt", "qt_period"]
            + _CONTROL_COLS
        )
        mask = df[est_cols].notna().all(axis=1) & contig
        est = df.loc[mask].copy()

        if len(est) < 10:
            logger.warning("Horizon h=%d: only %d obs, skipping.", h, len(est))
            df.drop(columns=[f"_cum_y_h{h}"], inplace=True)
            continue

        y = est[f"_cum_y_h{h}"].astype(float)
        X_cols = ["shock", "shock_x_soft", "shock_x_qt", "qt_period"] + _CONTROL_COLS
        X = sm.add_constant(est[X_cols].astype(float))

        model = sm.OLS(y, X).fit(
            cov_type="HAC",
            cov_kwds={"maxlags": h + 1},
        )

        # ---- Baseline (non-QT) effect: beta_h on "shock" -----------------
        beta_b = model.params["shock"]
        se_b = model.bse["shock"]
        t_b = model.tvalues["shock"]
        p_b = model.pvalues["shock"]
        ci_b = model.conf_int().loc["shock"]

        baseline_rows.append({
            "horizon": h,
            "beta": beta_b,
            "se": se_b,
            "t_stat": t_b,
            "p_value": p_b,
            "ci_lower": ci_b.iloc[0],
            "ci_upper": ci_b.iloc[1],
            "n_obs": int(model.nobs),
            "r_squared": model.rsquared,
            "regime": "non_qt_baseline",
        })

        # ---- QT total effect: shock + shock_x_qt -------------------------
        # Use statsmodels t_test to compute the linear combination
        t_result = model.t_test("shock + shock_x_qt = 0")
        total_beta = float(np.squeeze(t_result.effect))
        total_se = float(np.squeeze(t_result.sd))
        total_t = float(np.squeeze(t_result.tvalue))
        total_p = float(np.squeeze(t_result.pvalue))
        total_ci = np.atleast_2d(t_result.conf_int(alpha=0.05))

        qt_rows.append({
            "horizon": h,
            "beta": total_beta,
            "se": total_se,
            "t_stat": total_t,
            "p_value": total_p,
            "ci_lower": float(total_ci[0, 0]),
            "ci_upper": float(total_ci[0, 1]),
            "n_obs": int(model.nobs),
            "r_squared": model.rsquared,
            "regime": "qt_period",
        })

        df.drop(columns=[f"_cum_y_h{h}"], inplace=True)

    results: dict[str, pd.DataFrame] = {}

    # The actual full-sample LP (no interaction terms) — run it separately
    # so it reflects the unconditional supply-shock effect.
    results["full_sample"] = run_local_projections(
        panel, max_horizon=max_horizon, outcome=outcome,
    )

    # The non-QT baseline from the interaction model (β on shock, holding
    # shock×QT = 0). This is the effect OUTSIDE QT periods.
    non_qt_df = pd.DataFrame(baseline_rows)
    non_qt_df["regime"] = "non_qt_period"
    results["non_qt_period"] = non_qt_df

    if qt_rows:
        results["qt_period"] = pd.DataFrame(qt_rows)
    else:
        logger.warning("No QT interaction results produced.")
        results["qt_period"] = pd.DataFrame(
            columns=["horizon", "beta", "se", "t_stat", "p_value",
                     "ci_lower", "ci_upper", "n_obs", "r_squared", "regime"]
        )

    return results


# ---------------------------------------------------------------------------
# 5.  Figures
# ---------------------------------------------------------------------------

def generate_lp_figures(
    lp_results: dict[str, pd.DataFrame],
    figures_dir: str | Path,
) -> dict[str, Path]:
    """Generate local projection impulse response figures.

    Produces three figures:
      1. ``lp_impulse_response.png``  -- Cumulative IRF with 95% HAC CI bands
      2. ``lp_regime_comparison.png`` -- Full-sample vs QT-period IRFs overlaid
      3. ``lp_beta_distribution.png`` -- Coefficient bar chart + histogram

    Parameters
    ----------
    lp_results : dict[str, DataFrame]
        Output of ``run_local_projections_by_regime``.
    figures_dir : str or Path
        Directory in which to save the figures.

    Returns
    -------
    dict[str, Path]
        Mapping of figure short names to their file paths.
    """
    figures_dir = Path(figures_dir)
    figures_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    # ---- Figure 1: Cumulative IRF (full sample) ---------------------------
    full = lp_results.get("full_sample")
    if full is not None and not full.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        h = full["horizon"].to_numpy()
        beta = full["beta"].to_numpy()
        ci_lo = full["ci_lower"].to_numpy()
        ci_hi = full["ci_upper"].to_numpy()

        ax.plot(h, beta, "o-", color="steelblue", linewidth=1.8, markersize=5,
                label="LP coefficient", zorder=5)
        ax.fill_between(h, ci_lo, ci_hi, alpha=0.2, color="steelblue",
                        label="95% CI (HAC)", zorder=3)
        ax.axhline(0, color="gray", linewidth=0.6, linestyle="--")
        ax.set_xlabel("Weeks after supply shock")
        ax.set_ylabel("Cumulative dealer inventory response ($M)")
        ax.set_title("Local Projection IRF: Ex Ante Supply Shock")
        ax.set_xticks(h)
        ax.legend()
        plt.tight_layout()

        out = figures_dir / "lp_impulse_response.png"
        fig.savefig(out, dpi=150)
        plt.close(fig)
        paths["lp_impulse_response"] = out
        logger.info("Saved %s", out)
    else:
        logger.warning("Skipping LP IRF figure: no full-sample results.")

    # ---- Figure 2: Regime comparison (baseline vs QT) ---------------------
    baseline = lp_results.get("full_sample")
    qt = lp_results.get("qt_period")

    has_baseline = baseline is not None and not baseline.empty
    has_qt = qt is not None and not qt.empty

    if has_baseline or has_qt:
        fig, ax = plt.subplots(figsize=(11, 6))

        if has_baseline:
            h_b = baseline["horizon"].to_numpy()
            beta_b = baseline["beta"].to_numpy()
            ci_lo_b = baseline["ci_lower"].to_numpy()
            ci_hi_b = baseline["ci_upper"].to_numpy()
            ax.plot(h_b, beta_b, "o-", color="#2ca02c", linewidth=1.5,
                    markersize=4, label=r"Full-sample $\beta_h$", zorder=5)
            ax.fill_between(h_b, ci_lo_b, ci_hi_b, alpha=0.12,
                            color="#2ca02c", zorder=2)

        if has_qt:
            h_q = qt["horizon"].to_numpy()
            beta_q = qt["beta"].to_numpy()
            ci_lo_q = qt["ci_lower"].to_numpy()
            ci_hi_q = qt["ci_upper"].to_numpy()
            ax.plot(h_q, beta_q, "s-", color="#d62728", linewidth=1.5,
                    markersize=4,
                    label=r"QT period ($\beta_h + \delta_h$)", zorder=5)
            ax.fill_between(h_q, ci_lo_q, ci_hi_q, alpha=0.12,
                            color="#d62728", zorder=2)

        ax.axhline(0, color="gray", linewidth=0.6, linestyle="--")
        ax.set_xlabel("Weeks after supply shock")
        ax.set_ylabel("Cumulative dealer inventory response ($M)")
        ax.set_title("Local Projection IRF by Monetary-Policy Regime")

        all_h: list[int] = []
        if has_baseline:
            all_h.extend(baseline["horizon"].tolist())
        if has_qt:
            all_h.extend(qt["horizon"].tolist())
        if all_h:
            ax.set_xticks(sorted(set(all_h)))
        ax.legend()
        plt.tight_layout()

        out = figures_dir / "lp_regime_comparison.png"
        fig.savefig(out, dpi=150)
        plt.close(fig)
        paths["lp_regime_comparison"] = out
        logger.info("Saved %s", out)
    else:
        logger.warning("Skipping LP regime comparison figure: no results.")

    # ---- Figure 3: Shock distribution (time series + histogram) -----------
    # This figure needs the panel data, which we do not have here.  We create
    # a stub that downstream callers can populate, or we extract horizon-level
    # info.  Since we only receive lp_results (a dict of DataFrames), we
    # cannot reconstruct the shock time series.  We will generate this figure
    # in a helper that accepts the panel.
    # For compatibility, create an empty entry; the CLI can call
    # generate_shock_distribution_figure separately if it has the panel.
    #
    # However, to keep the interface simple and match the spec, we generate
    # a diagnostic figure from the LP results themselves: a coefficient
    # trajectory with significance markers.
    if has_baseline:
        fig, (ax_left, ax_right) = plt.subplots(
            1, 2, figsize=(10, 5),
            gridspec_kw={"width_ratios": [3, 1], "wspace": 0.25},
        )
        h_vals = baseline["horizon"].to_numpy()
        betas = baseline["beta"].to_numpy()
        pvals = baseline["p_value"].to_numpy()

        colors = ["steelblue" if p < 0.05 else "lightgray" for p in pvals]
        ax_left.bar(h_vals, betas, color=colors, edgecolor="white",
                    linewidth=0.5, zorder=3)
        ax_left.axhline(0, color="gray", linewidth=0.6, linestyle="--")
        ax_left.set_xlabel("Horizon (weeks)")
        ax_left.set_ylabel(r"$\beta_h$ (supply shock coefficient)")
        ax_left.set_title("Shock effect by horizon")
        ax_left.set_xticks(h_vals)

        # Right panel: histogram of betas
        ax_right.hist(betas, bins=max(5, len(betas) // 2), orientation="horizontal",
                      color="steelblue", edgecolor="white", alpha=0.7)
        ax_right.axhline(0, color="gray", linewidth=0.6, linestyle="--")
        ax_right.set_xlabel("Count")
        ax_right.set_yticklabels([])
        ax_right.set_title("Distribution")
        fig.subplots_adjust(left=0.08, right=0.96, bottom=0.12, top=0.92)

        out = figures_dir / "lp_beta_distribution.png"
        fig.savefig(out, dpi=150)
        plt.close(fig)
        paths["lp_beta_distribution"] = out
        logger.info("Saved %s", out)

    return paths


def generate_shock_distribution_figure(
    panel: pd.DataFrame,
    figures_dir: str | Path,
) -> Path | None:
    """Generate the shock time-series + histogram figure from the panel.

    This is a supplementary figure showing *when* the ex ante supply shock
    fires and its frequency distribution.

    Parameters
    ----------
    panel : DataFrame
        Raw auction-week panel.
    figures_dir : str or Path
        Directory for saved figure.

    Returns
    -------
    Path or None
        Path to ``lp_shock_distribution.png``, or None on failure.
    """
    figures_dir = Path(figures_dir)
    figures_dir.mkdir(parents=True, exist_ok=True)

    df = _prepare_lp_panel(panel)

    fig = plt.figure(figsize=(12, 5))
    gs = gridspec.GridSpec(1, 2, width_ratios=[3, 1], wspace=0.05)

    # Left: time series of the shock
    ax_ts = fig.add_subplot(gs[0])
    dates = df["week_start"]
    shock = df["shock"].astype(float)
    ax_ts.fill_between(dates, 0, shock, step="mid", alpha=0.4,
                       color="steelblue", label="Supply shock")
    # Overlay bridge pressure (shock AND soft demand)
    bridge_pressure = df["shock_x_soft"].astype(float)
    ax_ts.fill_between(dates, 0, bridge_pressure, step="mid", alpha=0.6,
                       color="#d62728", label="+ soft demand (bridge pressure)")
    ax_ts.set_xlabel("Week")
    ax_ts.set_ylabel("Shock indicator (0/1)")
    ax_ts.set_title("Ex Ante Supply Shock Time Series")
    ax_ts.legend(loc="upper left", fontsize=8)

    # Right: histogram of announced_amount_total with threshold
    ax_hist = fig.add_subplot(gs[1])
    supply = df["announced_amount_total"] / 1e9  # billions
    ax_hist.hist(supply.dropna(), bins=30, orientation="horizontal",
                 color="steelblue", edgecolor="white", alpha=0.7)
    # Mark the final expanding p75 (approximate, shown as reference)
    final_p75 = supply.expanding(min_periods=1).quantile(0.75).iloc[-1]
    ax_hist.axhline(final_p75, color="#d62728", linewidth=1.2, linestyle="--",
                    label=f"Final p75 = ${final_p75:.1f}B")
    ax_hist.set_xlabel("Count")
    ax_hist.set_ylabel("Announced amount ($B)")
    ax_hist.set_title("Supply distribution")
    ax_hist.legend(fontsize=8)
    plt.tight_layout()

    out = figures_dir / "lp_shock_distribution.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)
    return out


# ---------------------------------------------------------------------------
# 6.  Table
# ---------------------------------------------------------------------------

def generate_lp_table(
    lp_results: dict[str, pd.DataFrame],
    tables_dir: str | Path,
    file_name: str = "lp_results.csv",
) -> Path:
    """Save LP results to ``lp_results.csv``.

    Combines full-sample (baseline), QT-period, and non-QT-period results
    into a single table with a ``regime`` column.

    Parameters
    ----------
    lp_results : dict[str, DataFrame]
        Output of ``run_local_projections_by_regime``.
    tables_dir : str or Path
        Directory for saved CSV.

    Returns
    -------
    Path
        Path to ``lp_results.csv``.
    """
    tables_dir = Path(tables_dir)
    tables_dir.mkdir(parents=True, exist_ok=True)

    expected_cols = [
        "horizon", "beta", "se", "t_stat", "p_value",
        "ci_lower", "ci_upper", "n_obs", "r_squared", "regime",
    ]

    frames: list[pd.DataFrame] = []
    for key in ("full_sample", "qt_period", "non_qt_period"):
        df = lp_results.get(key, pd.DataFrame())
        if not df.empty:
            df = df.copy()
            # Ensure regime column is set (may already be from estimation)
            df["regime"] = key
            frames.append(df[expected_cols])

    if frames:
        combined = pd.concat(frames, ignore_index=True)
    else:
        combined = pd.DataFrame(columns=expected_cols)

    out = tables_dir / file_name
    combined.to_csv(out, index=False)
    logger.info("Saved LP results table to %s", out)
    return out


def generate_lp_placebo_table(
    placebo_results: pd.DataFrame,
    tables_dir: str | Path,
    file_name: str = "lp_placebo_results.csv",
) -> Path:
    """Write LP placebo/falsification results to disk."""
    tables_dir = Path(tables_dir)
    tables_dir.mkdir(parents=True, exist_ok=True)

    out = tables_dir / file_name
    placebo_results.to_csv(out, index=False)
    logger.info("Saved LP placebo table to %s", out)
    return out
