"""Regression analysis for the BidBridge panel.

All regressions use numpy-only OLS with heteroskedasticity-robust (HC1)
standard errors to avoid adding statsmodels as a dependency.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from bidbridge.features.stress_flags import add_stress_flags


def _ols_robust(
    y: np.ndarray,
    X: np.ndarray,
    term_names: list[str],
) -> pd.DataFrame:
    """OLS with HC1 (White) heteroskedasticity-robust standard errors.

    Returns a DataFrame with columns: term, coefficient, std_error, t_stat, p_value.
    """
    n, k = X.shape
    beta, residuals, rank, sv = np.linalg.lstsq(X, y, rcond=None)

    # Residuals
    e = y - X @ beta

    # HC1 robust covariance: (X'X)^{-1} X' diag(e^2) X (X'X)^{-1} * n/(n-k)
    # Use a pseudoinverse so the descriptive pipeline remains runnable when
    # public-data regressors become collinear in a given sample window.
    XtX_inv = np.linalg.pinv(X.T @ X)
    meat = X.T @ np.diag(e**2) @ X
    dof = max(n - k, 1)
    V_hc1 = XtX_inv @ meat @ XtX_inv * (n / dof)

    se = np.sqrt(np.diag(V_hc1))
    t_stat = beta / se

    # Two-sided p-value using normal approximation (large sample)
    from math import erfc, sqrt
    p_values = [erfc(abs(t) / sqrt(2)) for t in t_stat]

    # R-squared
    ss_res = np.sum(e**2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    result = pd.DataFrame({
        "term": term_names,
        "coefficient": beta,
        "std_error": se,
        "t_stat": t_stat,
        "p_value": p_values,
    })

    # Store metadata as attrs
    result.attrs["n_obs"] = n
    result.attrs["r_squared"] = r_squared
    result.attrs["residual_std"] = np.sqrt(ss_res / dof)

    return result


def run_demo_bridge_regression(panel: pd.DataFrame) -> pd.DataFrame:
    """Basic bridge regression: inventory_change ~ supply + dealer_share + refunding.

    Kept for backward compatibility with the demo pipeline.
    """
    df = panel.dropna(
        subset=["inventory_change", "awarded_amount_total", "dealer_share_allotment"]
    ).copy()

    X = np.column_stack([
        np.ones(len(df)),
        df["awarded_amount_total"].to_numpy(),
        df["dealer_share_allotment"].to_numpy(),
        df["refunding_week"].astype(int).to_numpy(),
    ])
    y = df["inventory_change"].to_numpy()
    terms = ["intercept", "awarded_amount_total", "dealer_share_allotment", "refunding_week"]

    return _ols_robust(y, X, terms)


def run_extended_bridge_regression(panel: pd.DataFrame) -> pd.DataFrame:
    """Extended regression with SOMA, H.8, and time controls.

    inventory_change ~ supply_M + dealer_share + refunding + d_soma + bank_holdings + trend
    """
    required = ["inventory_change", "awarded_amount_total", "dealer_share_allotment"]
    df = panel.dropna(subset=required).copy()

    # Normalize supply to millions for interpretable coefficients
    df["supply_M"] = df["awarded_amount_total"] / 1e6

    # SOMA change (week-over-week, in billions for readability)
    if "soma_treasury_total" in df.columns:
        df["d_soma_B"] = (df["soma_treasury_total"].diff() / 1e9).fillna(0)
    else:
        df["d_soma_B"] = 0.0

    # Bank holdings change (week-over-week, in millions)
    if "bank_treasury_securities" in df.columns:
        df["d_bank_M"] = df["bank_treasury_securities"].diff().fillna(0)
    else:
        df["d_bank_M"] = 0.0

    # Time trend (years since start)
    df["trend"] = (
        (df["week_start"] - df["week_start"].min()).dt.days / 365.25
    )

    # Drop rows where any regressor is NaN
    regressors = ["supply_M", "dealer_share_allotment", "refunding_week",
                  "d_soma_B", "d_bank_M", "trend"]
    df = df.dropna(subset=regressors + ["inventory_change"])

    X = np.column_stack([
        np.ones(len(df)),
        df["supply_M"].to_numpy(),
        df["dealer_share_allotment"].to_numpy(),
        df["refunding_week"].astype(int).to_numpy(),
        df["d_soma_B"].to_numpy(),
        df["d_bank_M"].to_numpy(),
        df["trend"].to_numpy(),
    ])
    y = df["inventory_change"].to_numpy()
    terms = [
        "intercept", "supply_M", "dealer_share", "refunding_week",
        "d_soma_B", "d_bank_holdings_M", "trend_years",
    ]

    return _ols_robust(y, X, terms)


def run_refunding_test(panel: pd.DataFrame) -> pd.DataFrame:
    """Statistical comparison of refunding vs ordinary weeks.

    Returns a summary table with means, std, t-test results for key variables.
    """
    df = panel.dropna(subset=["pd_treasury_inventory"]).copy()

    ref = df[df["refunding_week"]]
    ordi = df[~df["refunding_week"]]

    test_vars = [
        ("inventory_change", "Inventory change ($M)"),
        ("awarded_amount_total", "Weekly awarded ($)"),
        ("dealer_share_allotment", "Dealer share"),
        ("weighted_bid_to_cover", "Bid-to-cover"),
        ("weighted_tail_bp", "Tail (bp)"),
    ]

    rows = []
    for col, label in test_vars:
        if col not in df.columns:
            continue

        r_vals = ref[col].dropna()
        o_vals = ordi[col].dropna()

        if len(r_vals) < 2 or len(o_vals) < 2:
            continue

        # Welch's t-test (unequal variances)
        r_mean, o_mean = r_vals.mean(), o_vals.mean()
        r_std, o_std = r_vals.std(ddof=1), o_vals.std(ddof=1)
        r_n, o_n = len(r_vals), len(o_vals)

        se_diff = np.sqrt(r_std**2 / r_n + o_std**2 / o_n)
        t_stat = (r_mean - o_mean) / se_diff if se_diff > 0 else 0.0

        # Welch-Satterthwaite degrees of freedom
        num = (r_std**2 / r_n + o_std**2 / o_n) ** 2
        den = (r_std**2 / r_n)**2 / (r_n - 1) + (o_std**2 / o_n)**2 / (o_n - 1)
        dof = num / den if den > 0 else r_n + o_n - 2

        # P-value (two-sided, normal approx for large samples)
        from math import erfc, sqrt
        p_value = erfc(abs(t_stat) / sqrt(2))

        rows.append({
            "variable": label,
            "refunding_mean": round(r_mean, 4),
            "ordinary_mean": round(o_mean, 4),
            "difference": round(r_mean - o_mean, 4),
            "t_stat": round(t_stat, 3),
            "p_value": round(p_value, 4),
            "refunding_n": r_n,
            "ordinary_n": o_n,
        })

    return pd.DataFrame(rows)


def run_interaction_regression(panel: pd.DataFrame) -> pd.DataFrame:
    """Interaction regression testing whether supply effects are larger during
    QT and TGA-rebuild periods.

    Model:
        inventory_change ~ supply_M + dealer_share + refunding + qt_period
                         + tga_rebuild + supply_M*qt_period + supply_M*tga_rebuild
                         + d_soma_B + trend

    Stress flags are added via ``add_stress_flags()`` before estimation.
    Interaction terms are skipped when the underlying flag has no variation
    (all-zero column).
    """
    df = add_stress_flags(panel)

    required = ["inventory_change", "awarded_amount_total", "dealer_share_allotment"]
    df = df.dropna(subset=required).copy()

    # Normalize supply to millions
    df["supply_M"] = df["awarded_amount_total"] / 1e6

    # SOMA change (week-over-week, in billions)
    if "soma_treasury_total" in df.columns:
        df["d_soma_B"] = (df["soma_treasury_total"].diff() / 1e9).fillna(0)
    else:
        df["d_soma_B"] = 0.0

    # Time trend (years since start)
    df["week_start"] = pd.to_datetime(df["week_start"])
    df["trend"] = (
        (df["week_start"] - df["week_start"].min()).dt.days / 365.25
    )

    # Ensure boolean flags are int for regression
    df["qt_period"] = df["qt_period"].astype(int)
    df["tga_rebuild"] = df["tga_rebuild"].astype(int)

    # Build regressor list and term names dynamically to handle zero-variation flags
    regressors = ["supply_M", "dealer_share_allotment", "refunding_week"]
    terms = ["intercept", "supply_M", "dealer_share", "refunding_week"]

    # Add qt_period level and interaction if there is variation
    qt_has_variation = df["qt_period"].nunique() > 1
    if qt_has_variation:
        regressors.append("qt_period")
        terms.append("qt_period")

    tga_has_variation = df["tga_rebuild"].nunique() > 1
    if tga_has_variation:
        regressors.append("tga_rebuild")
        terms.append("tga_rebuild")

    # Interaction terms
    if qt_has_variation:
        df["supply_M_x_qt"] = df["supply_M"] * df["qt_period"]
        regressors.append("supply_M_x_qt")
        terms.append("supply_M x qt_period")

    if tga_has_variation:
        df["supply_M_x_tga"] = df["supply_M"] * df["tga_rebuild"]
        regressors.append("supply_M_x_tga")
        terms.append("supply_M x tga_rebuild")

    regressors += ["d_soma_B", "trend"]
    terms += ["d_soma_B", "trend_years"]

    # Drop rows where any regressor is NaN
    df = df.dropna(subset=regressors + ["inventory_change"])

    X = np.column_stack([
        np.ones(len(df)),
        *[df[r].astype(float).to_numpy() for r in regressors],
    ])
    y = df["inventory_change"].to_numpy()

    return _ols_robust(y, X, terms)


def run_subsample_regressions(panel: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Run the extended bridge regression on substantively interesting subsamples.

    Subsamples
    ----------
    - pre_2020 / post_2020 : split at 2020-01-01
    - qt / non_qt          : QT-period flag from ``add_stress_flags``
    - refunding / ordinary : refunding_week flag already in the panel

    Returns a dict mapping subsample name to the ``run_extended_bridge_regression``
    result DataFrame.  Subsamples with fewer than 30 observations are skipped.
    """
    df = add_stress_flags(panel)
    df["week_start"] = pd.to_datetime(df["week_start"])

    subsamples: dict[str, pd.DataFrame] = {}

    def _safe_regression(subset: pd.DataFrame) -> pd.DataFrame | None:
        """Try extended regression; on singular matrix, drop zero-variance
        regressors from the extended spec rather than switching estimands."""
        try:
            return run_extended_bridge_regression(subset)
        except np.linalg.LinAlgError:
            # Drop regressors with zero variance in this subsample
            # but keep the same model family as the extended spec
            try:
                sub = subset.copy()
                sub["supply_M"] = sub["awarded_amount_total"] / 1e6
                if "soma_treasury_total" in sub.columns:
                    sub["d_soma_B"] = (sub["soma_treasury_total"].diff() / 1e9).fillna(0)
                else:
                    sub["d_soma_B"] = 0.0
                if "bank_treasury_securities" in sub.columns:
                    sub["d_bank_M"] = sub["bank_treasury_securities"].diff().fillna(0)
                else:
                    sub["d_bank_M"] = 0.0
                sub["trend"] = (
                    (sub["week_start"] - sub["week_start"].min()).dt.days / 365.25
                )
                sub["refunding_int"] = sub["refunding_week"].astype(int)

                # Build regressor list, dropping zero-variance columns
                candidates = ["supply_M", "dealer_share_allotment", "refunding_int",
                              "d_soma_B", "d_bank_M", "trend"]
                sub = sub.dropna(subset=candidates + ["inventory_change"])
                regressors = [c for c in candidates if sub[c].std() > 0]

                if len(regressors) < 1 or len(sub) < len(regressors) + 2:
                    return None

                X = np.column_stack([np.ones(len(sub))] + [sub[c].to_numpy() for c in regressors])
                y = sub["inventory_change"].to_numpy()
                return _ols_robust(y, X, ["intercept"] + regressors)
            except (np.linalg.LinAlgError, ValueError):
                return None

    # --- Pre-2020 vs post-2020 ------------------------------------------------
    cutoff = pd.Timestamp("2020-01-01")
    pre = df[df["week_start"] < cutoff]
    post = df[df["week_start"] >= cutoff]
    if len(pre) >= 30:
        result = _safe_regression(pre)
        if result is not None:
            subsamples["pre_2020"] = result
    if len(post) >= 30:
        result = _safe_regression(post)
        if result is not None:
            subsamples["post_2020"] = result

    # --- QT vs non-QT ---------------------------------------------------------
    if df["qt_period"].any():
        qt_sub = df[df["qt_period"].astype(bool)]
        non_qt_sub = df[~df["qt_period"].astype(bool)]
        if len(qt_sub) >= 30:
            result = _safe_regression(qt_sub)
            if result is not None:
                subsamples["qt"] = result
        if len(non_qt_sub) >= 30:
            result = _safe_regression(non_qt_sub)
            if result is not None:
                subsamples["non_qt"] = result

    # --- Refunding vs ordinary ------------------------------------------------
    if "refunding_week" in df.columns:
        ref_sub = df[df["refunding_week"].astype(bool)]
        ord_sub = df[~df["refunding_week"].astype(bool)]
        if len(ref_sub) >= 30:
            result = _safe_regression(ref_sub)
            if result is not None:
                subsamples["refunding"] = result
        if len(ord_sub) >= 30:
            result = _safe_regression(ord_sub)
            if result is not None:
                subsamples["ordinary"] = result

    return subsamples


def run_all_regressions(panel: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Run all regression specifications and return results dict."""
    results = {}
    results["basic"] = run_demo_bridge_regression(panel)
    results["extended"] = run_extended_bridge_regression(panel)
    results["refunding_test"] = run_refunding_test(panel)
    results["interaction"] = run_interaction_regression(panel)
    results["subsamples"] = run_subsample_regressions(panel)
    return results
