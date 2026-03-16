"""Maturity-bucket panel fixed-effects regression module.

Estimates within-week, across-curve effects of auction supply on primary
dealer inventory using a two-way fixed-effects specification:

    delta_position_{b,t} = alpha_b + tau_t + beta * announced_amount_{b,t}
                         + theta * announced_amount_{b,t} * lagged_soft_demand_{b,t-1}
                         + epsilon_{b,t}

Where:
    b = maturity bucket (bills, short_coupon, belly_coupon, long_coupon, tips, frns)
    t = week
    alpha_b = bucket fixed effect
    tau_t = week fixed effect

The key insight: if dealers absorb supply where it lands on the curve, beta
should be positive and significant within buckets, controlling for week-level
shocks via week FE.

Dealer position mapping (NY Fed reports aggregate coupon, not by sub-bucket):
    pd_bills_position  -> bills
    pd_coupon_position -> short_coupon + belly_coupon + long_coupon
                          (split proportionally by awarded_amount)
    pd_tips_position   -> tips
    pd_frn_position    -> frns (when available)
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np               # noqa: E402
import pandas as pd              # noqa: E402

logger = logging.getLogger(__name__)

# Coupon sub-buckets that share pd_coupon_position
_COUPON_BUCKETS = {"short_coupon", "belly_coupon", "long_coupon"}

# All recognized maturity buckets
_ALL_BUCKETS = {"bills", "short_coupon", "belly_coupon", "long_coupon", "tips", "frns"}


# ---------------------------------------------------------------------------
# 1.  Build bucket-level outcome panel
# ---------------------------------------------------------------------------

def build_bucket_outcomes(
    maturity_panel: pd.DataFrame,
    dealer_stats: pd.DataFrame,
) -> pd.DataFrame:
    """Merge maturity-bucket panel with bucket-level dealer positions from NY Fed.

    Parameters
    ----------
    maturity_panel : DataFrame
        The (week_start, maturity_bucket) panel produced by
        ``bidbridge.features.maturity_panel.build_maturity_panel`` or loaded
        from ``data/processed/maturity_bucket_panel.csv``.
    dealer_stats : DataFrame
        NY Fed primary dealer statistics with columns: week_start,
        pd_bills_position, pd_coupon_position, pd_tips_position,
        and optionally pd_frn_position.

    Returns
    -------
    DataFrame
        Panel with columns: week_start, maturity_bucket, announced_amount,
        awarded_amount, dealer_share, delta_position, lagged_dealer_share.
    """
    mp = maturity_panel.copy()
    ds = dealer_stats.copy()

    # Ensure datetime types
    mp["week_start"] = pd.to_datetime(mp["week_start"])
    ds["week_start"] = pd.to_datetime(ds["week_start"])

    # ---- Map dealer positions to maturity buckets -------------------------
    #
    # NY Fed reports:
    #   pd_bills_position  -> bills
    #   pd_coupon_position -> short_coupon + belly_coupon + long_coupon
    #   pd_tips_position   -> tips
    #   pd_frn_position    -> frns (often missing in early data)
    #
    # For coupon sub-buckets, split pd_coupon_position proportionally by
    # awarded_amount within each week.

    # Step 1: Compute coupon bucket share weights per week
    coupon_mask = mp["maturity_bucket"].isin(_COUPON_BUCKETS)
    coupon_weekly = (
        mp.loc[coupon_mask]
        .groupby("week_start")["awarded_amount"]
        .sum()
        .rename("coupon_total_awarded")
    )

    mp = mp.merge(coupon_weekly, on="week_start", how="left")
    # Recompute mask after merge to ensure alignment
    coupon_mask = mp["maturity_bucket"].isin(_COUPON_BUCKETS)
    mp["coupon_share_weight"] = np.where(
        coupon_mask & (mp["coupon_total_awarded"] > 0),
        mp["awarded_amount"] / mp["coupon_total_awarded"],
        0.0,
    )

    # Step 2: Merge dealer stats onto each (week, bucket) row
    ds_cols = ["week_start", "pd_bills_position", "pd_coupon_position",
               "pd_tips_position"]
    if "pd_frn_position" in ds.columns:
        ds_cols.append("pd_frn_position")

    # De-duplicate dealer stats by week_start (take last observation)
    ds_dedup = ds[ds_cols].drop_duplicates(subset=["week_start"], keep="last")

    panel = mp.merge(ds_dedup, on="week_start", how="inner")

    # Step 2b: Expand to a balanced panel — every (week_start, maturity_bucket)
    # combination gets a row so that zero-supply weeks are represented.  This
    # restores within-week variation across all 6 buckets for the FE regression.
    all_weeks = pd.DataFrame({"week_start": panel["week_start"].unique()})
    all_buckets = pd.DataFrame({"maturity_bucket": panel["maturity_bucket"].unique()})
    balanced = all_weeks.merge(all_buckets, how="cross")
    panel = balanced.merge(panel, on=["week_start", "maturity_bucket"], how="left")
    panel["awarded_amount"] = panel["awarded_amount"].fillna(0)
    panel["announced_amount"] = panel["announced_amount"].fillna(0)
    # dealer_share is left as NaN for non-auction weeks (no auction -> no share)

    # Re-merge dealer stats for rows added by balancing (they have week_start
    # but may lack the dealer-stat columns after the left join above).
    ds_merge_cols = [c for c in ds_dedup.columns if c != "week_start"]
    for col in ds_merge_cols:
        if col in panel.columns:
            # Fill NaNs introduced by balancing from ds_dedup
            panel[col] = panel[col].combine_first(
                panel[["week_start"]].merge(ds_dedup[["week_start", col]], on="week_start", how="left")[col]
            )

    # Recompute coupon_share_weight for balanced rows (needed for position assignment)
    coupon_mask_bal = panel["maturity_bucket"].isin(_COUPON_BUCKETS)
    coupon_weekly_bal = (
        panel.loc[coupon_mask_bal]
        .groupby("week_start")["awarded_amount"]
        .sum()
        .rename("coupon_total_awarded_bal")
    )
    panel = panel.merge(coupon_weekly_bal, on="week_start", how="left")
    panel["coupon_share_weight"] = np.where(
        coupon_mask_bal & (panel["coupon_total_awarded_bal"] > 0),
        panel["awarded_amount"] / panel["coupon_total_awarded_bal"],
        0.0,
    )
    panel.drop(columns=["coupon_total_awarded_bal"], inplace=True, errors="ignore")

    # Step 3: Assign bucket-level position
    def _assign_position(row: pd.Series) -> float:
        bucket = row["maturity_bucket"]
        if bucket == "bills":
            return row.get("pd_bills_position", np.nan)
        if bucket == "tips":
            return row.get("pd_tips_position", np.nan)
        if bucket == "frns":
            return row.get("pd_frn_position", np.nan)
        if bucket in _COUPON_BUCKETS:
            coupon_pos = row.get("pd_coupon_position", np.nan)
            weight = row.get("coupon_share_weight", 0.0)
            if pd.notna(coupon_pos) and weight > 0:
                return coupon_pos * weight
            return np.nan
        return np.nan

    panel["bucket_position"] = panel.apply(_assign_position, axis=1)

    # Step 4: Compute delta_position = position_{b,t} - position_{b,t-1}
    panel = panel.sort_values(["maturity_bucket", "week_start"]).reset_index(drop=True)
    panel["delta_position"] = (
        panel.groupby("maturity_bucket")["bucket_position"].diff()
    )

    # Step 5: Lagged dealer share (soft demand proxy)
    panel["lagged_dealer_share"] = (
        panel.groupby("maturity_bucket")["dealer_share"].shift(1)
    )

    # Step 6: Compute soft demand indicator — above-median dealer share
    # signals soft non-dealer demand (dealers had to absorb more).
    #
    # Use an expanding-window median per bucket so the threshold at time t
    # is computed from data up to t only (avoids look-ahead bias from the
    # previous full-sample median approach).
    panel = panel.sort_values(["maturity_bucket", "week_start"]).reset_index(drop=True)
    expanding_median = (
        panel.groupby("maturity_bucket")["dealer_share"]
        .transform(lambda s: s.expanding(min_periods=1).median())
    )
    panel["soft_demand"] = (panel["dealer_share"] > expanding_median).astype(int)
    panel["lagged_soft_demand"] = (
        panel.groupby("maturity_bucket")["soft_demand"].shift(1)
    )

    # Select and return clean columns
    keep_cols = [
        "week_start", "maturity_bucket",
        "announced_amount", "awarded_amount",
        "dealer_share", "bucket_position", "delta_position",
        "lagged_dealer_share", "lagged_soft_demand",
        "weighted_bid_to_cover", "refunding_week",
    ]
    keep_cols = [c for c in keep_cols if c in panel.columns]

    result = panel[keep_cols].copy()
    logger.info(
        "build_bucket_outcomes: %d rows, %d weeks, %d buckets",
        len(result),
        result["week_start"].nunique(),
        result["maturity_bucket"].nunique(),
    )
    return result


# ---------------------------------------------------------------------------
# 2.  Panel FE regression suite
# ---------------------------------------------------------------------------

def run_bucket_fe_regression(panel: pd.DataFrame) -> dict[str, object]:
    """Run panel fixed-effects regressions of dealer inventory changes on supply.

    Specifications:
        1. 'pooled'     : OLS without fixed effects (baseline)
        2. 'bucket_fe'  : Bucket (entity) fixed effects only
        3. 'twoway_fe'  : Bucket + week (entity + time) fixed effects
        4. 'interaction': Two-way FE + supply x lagged_soft_demand interaction

    Uses ``linearmodels.panel.PanelOLS`` with clustered standard errors by
    bucket.  Falls back to ``statsmodels`` OLS with dummies if linearmodels
    is unavailable.

    Parameters
    ----------
    panel : DataFrame
        Output of ``build_bucket_outcomes``.  Must contain: week_start,
        maturity_bucket, delta_position, announced_amount, and
        lagged_dealer_share.

    Returns
    -------
    dict
        Keys: 'pooled', 'bucket_fe', 'twoway_fe', 'interaction'.
        Values: fitted model result objects (PanelOLS or OLS results).
    """
    df = panel.copy()
    df = df.dropna(subset=["delta_position", "announced_amount"]).copy()

    if len(df) < 20:
        raise ValueError(
            f"Insufficient observations after dropping NaN: {len(df)} rows."
        )

    # Normalize supply to billions for interpretable coefficients
    df["supply_B"] = df["announced_amount"] / 1e9

    # Fill lagged variables that are NaN at series start
    df["lagged_dealer_share"] = df["lagged_dealer_share"].fillna(
        df["dealer_share"]
    )
    df["lagged_soft_demand"] = df["lagged_soft_demand"].fillna(0)

    # Interaction term
    df["supply_x_soft_demand"] = df["supply_B"] * df["lagged_soft_demand"]

    # NOTE: With only 6 maturity buckets the number of clusters is well below
    # the ~30-cluster rule of thumb for cluster-robust standard errors.
    # Entity-clustered SEs may therefore be downward-biased.  Results should
    # be interpreted with caution.  As a robustness check the linearmodels
    # path also computes Driscoll-Kraay (kernel) SEs, which are consistent
    # under cross-sectional and temporal dependence regardless of cluster
    # count.

    # Attempt to use linearmodels for proper panel FE estimation
    try:
        return _run_with_linearmodels(df)
    except Exception as exc:
        logger.warning(
            "linearmodels failed (%s), falling back to statsmodels.", exc
        )
        return _run_with_statsmodels(df)


def _run_with_linearmodels(df: pd.DataFrame) -> dict[str, object]:
    """Panel FE estimation via linearmodels.panel.PanelOLS."""
    from linearmodels.panel import PanelOLS, PooledOLS

    # Set multi-index required by linearmodels: (entity, time)
    df = df.copy()
    df["maturity_bucket"] = df["maturity_bucket"].astype("category")
    df["week_id"] = pd.Categorical(df["week_start"])

    df = df.set_index(["maturity_bucket", "week_start"])

    dep = df["delta_position"]
    results = {}

    # ---- Spec 1: Pooled OLS (no FE) --------------------------------------
    exog_pooled = df[["supply_B", "lagged_dealer_share"]].copy()
    exog_pooled.insert(0, "const", 1.0)
    mod_pooled = PooledOLS(dep, exog_pooled)
    results["pooled"] = mod_pooled.fit(cov_type="clustered", cluster_entity=True)
    logger.info("Pooled OLS: n=%d", results["pooled"].nobs)

    # ---- Spec 2: Bucket FE only ------------------------------------------
    exog_fe = df[["supply_B", "lagged_dealer_share"]]
    mod_bucket = PanelOLS(dep, exog_fe, entity_effects=True)
    results["bucket_fe"] = mod_bucket.fit(cov_type="clustered", cluster_entity=True)
    logger.info("Bucket FE: n=%d", results["bucket_fe"].nobs)

    # ---- Spec 3: Two-way FE (bucket + week) ------------------------------
    mod_twoway = PanelOLS(dep, exog_fe, entity_effects=True, time_effects=True)
    results["twoway_fe"] = mod_twoway.fit(cov_type="clustered", cluster_entity=True)
    logger.info("Two-way FE: n=%d", results["twoway_fe"].nobs)

    # ---- Spec 4: Two-way FE + interaction --------------------------------
    exog_interact = df[["supply_B", "lagged_dealer_share", "supply_x_soft_demand"]]
    mod_interact = PanelOLS(
        dep, exog_interact, entity_effects=True, time_effects=True
    )
    results["interaction"] = mod_interact.fit(
        cov_type="clustered", cluster_entity=True
    )
    logger.info("Interaction: n=%d", results["interaction"].nobs)

    # ---- Robustness: Driscoll-Kraay (kernel) SEs --------------------------
    # With only 6 entity clusters, cluster-robust SEs may be unreliable.
    # Driscoll-Kraay SEs are consistent under both cross-sectional and
    # temporal dependence without requiring a large number of clusters.
    try:
        results["twoway_fe_driscoll_kraay"] = mod_twoway.fit(
            cov_type="kernel"
        )
        results["interaction_driscoll_kraay"] = mod_interact.fit(
            cov_type="kernel"
        )
        logger.info(
            "Driscoll-Kraay robustness check computed for twoway_fe and interaction specs."
        )
    except Exception as exc:
        logger.warning(
            "Driscoll-Kraay SEs could not be computed: %s", exc
        )

    return results


def _run_with_statsmodels(df: pd.DataFrame) -> dict[str, object]:
    """Fallback: panel FE via statsmodels OLS with dummy variables."""
    import statsmodels.api as sm

    df = df.copy()
    results = {}

    # Create dummy columns
    bucket_dummies = pd.get_dummies(df["maturity_bucket"], prefix="d_bkt", drop_first=True)
    week_dummies = pd.get_dummies(df["week_start"].astype(str), prefix="d_wk", drop_first=True)

    dep = df["delta_position"].values

    # ---- Spec 1: Pooled OLS ----------------------------------------------
    X_pooled = sm.add_constant(df[["supply_B", "lagged_dealer_share"]])
    mod_pooled = sm.OLS(dep, X_pooled).fit(cov_type="cluster",
                                            cov_kwds={"groups": df["maturity_bucket"]})
    results["pooled"] = mod_pooled

    # ---- Spec 2: Bucket FE only ------------------------------------------
    X_bucket = pd.concat([df[["supply_B", "lagged_dealer_share"]], bucket_dummies], axis=1)
    mod_bucket = sm.OLS(dep, X_bucket).fit(cov_type="cluster",
                                            cov_kwds={"groups": df["maturity_bucket"]})
    results["bucket_fe"] = mod_bucket

    # ---- Spec 3: Two-way FE (bucket + week) ------------------------------
    X_twoway = pd.concat([df[["supply_B", "lagged_dealer_share"]],
                          bucket_dummies, week_dummies], axis=1)
    mod_twoway = sm.OLS(dep, X_twoway).fit(cov_type="cluster",
                                            cov_kwds={"groups": df["maturity_bucket"]})
    results["twoway_fe"] = mod_twoway

    # ---- Spec 4: Two-way FE + interaction --------------------------------
    X_interact = pd.concat([
        df[["supply_B", "lagged_dealer_share", "supply_x_soft_demand"]],
        bucket_dummies, week_dummies,
    ], axis=1)
    mod_interact = sm.OLS(dep, X_interact).fit(
        cov_type="cluster", cov_kwds={"groups": df["maturity_bucket"]}
    )
    results["interaction"] = mod_interact

    return results


# ---------------------------------------------------------------------------
# 3.  Per-bucket regressions (for heterogeneity figure)
# ---------------------------------------------------------------------------

def _run_per_bucket_regressions(panel: pd.DataFrame) -> pd.DataFrame:
    """Estimate supply -> delta_position OLS separately for each bucket.

    Returns a DataFrame with one row per bucket containing: maturity_bucket,
    beta, se, t_stat, p_value, n_obs.
    """
    import statsmodels.api as sm

    df = panel.dropna(subset=["delta_position", "announced_amount"]).copy()
    df["supply_B"] = df["announced_amount"] / 1e9

    rows = []
    for bucket, g in df.groupby("maturity_bucket"):
        if len(g) < 10:
            logger.warning("Bucket %s: only %d obs, skipping.", bucket, len(g))
            continue

        X = sm.add_constant(g[["supply_B"]])
        y = g["delta_position"].values
        try:
            res = sm.OLS(y, X).fit(cov_type="HC1")
            beta = res.params["supply_B"]
            se = res.bse["supply_B"]
            t = res.tvalues["supply_B"]
            p = res.pvalues["supply_B"]
        except Exception as exc:
            logger.warning("Bucket %s regression failed: %s", bucket, exc)
            continue

        rows.append({
            "maturity_bucket": bucket,
            "beta": beta,
            "se": se,
            "t_stat": t,
            "p_value": p,
            "n_obs": int(res.nobs),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 4.  Figures
# ---------------------------------------------------------------------------

def _extract_coef(result, param_name: str) -> dict:
    """Extract coefficient, SE, and CI from a fitted result object.

    Works with both linearmodels PanelOLS results and statsmodels OLS results.
    """
    try:
        # linearmodels style
        beta = float(result.params[param_name])
        se = float(result.std_errors[param_name])
    except (AttributeError, KeyError):
        try:
            # statsmodels style
            beta = float(result.params[param_name])
            se = float(result.bse[param_name])
        except (AttributeError, KeyError):
            return {"beta": np.nan, "se": np.nan, "ci_lower": np.nan, "ci_upper": np.nan}

    return {
        "beta": beta,
        "se": se,
        "ci_lower": beta - 1.96 * se,
        "ci_upper": beta + 1.96 * se,
    }


def _extract_nobs(result) -> int:
    """Extract number of observations from a fitted result object."""
    for attr in ("nobs", "nobs_all"):
        val = getattr(result, attr, None)
        if val is not None:
            return int(val)
    return 0


def _extract_r2(result) -> float:
    """Extract R-squared from a fitted result."""
    for attr in ("rsquared", "rsquared_overall", "r_squared", "rsquared_within"):
        val = getattr(result, attr, None)
        if val is not None:
            return float(val)
    return np.nan


def generate_panel_fe_figures(
    results: dict[str, object],
    panel: pd.DataFrame,
    figures_dir: str | Path,
) -> dict[str, Path]:
    """Generate panel FE regression figures.

    Parameters
    ----------
    results : dict
        Output of ``run_bucket_fe_regression``.
    panel : DataFrame
        Output of ``build_bucket_outcomes`` (used for per-bucket regressions).
    figures_dir : str or Path
        Directory in which to save figures.

    Returns
    -------
    dict[str, Path]
        Mapping of figure short names to file paths.
    """
    figures_dir = Path(figures_dir)
    figures_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    # ---- Figure 1: Coefficient comparison across specs --------------------
    spec_names = ["pooled", "bucket_fe", "twoway_fe", "interaction"]
    spec_labels = ["Pooled OLS", "Bucket FE", "Two-way FE", "Two-way FE\n+ Interaction"]
    param = "supply_B"

    coefs = []
    for spec in spec_names:
        res = results.get(spec)
        if res is not None:
            coefs.append(_extract_coef(res, param))
        else:
            coefs.append({"beta": np.nan, "se": np.nan,
                          "ci_lower": np.nan, "ci_upper": np.nan})

    betas = [c["beta"] for c in coefs]
    ci_lo = [c["ci_lower"] for c in coefs]
    ci_hi = [c["ci_upper"] for c in coefs]

    valid = [i for i, b in enumerate(betas) if not np.isnan(b)]

    if valid:
        fig, ax = plt.subplots(figsize=(10, 5))
        x_pos = np.arange(len(spec_names))
        colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]

        for i in valid:
            ax.errorbar(
                x_pos[i], betas[i],
                yerr=[[betas[i] - ci_lo[i]], [ci_hi[i] - betas[i]]],
                fmt="o", markersize=8, capsize=6, capthick=1.5,
                color=colors[i], linewidth=1.5, zorder=5,
            )

        ax.axhline(0, color="gray", linewidth=0.6, linestyle="--", zorder=1)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(spec_labels)
        ax.set_ylabel("Coefficient on announced supply ($B)")
        ax.set_title("Supply -> Dealer Inventory: Panel FE Specifications")
        ax.grid(axis="y", alpha=0.3)
        plt.tight_layout()

        out = figures_dir / "panel_fe_coefficients.png"
        fig.savefig(out, dpi=150)
        plt.close(fig)
        paths["panel_fe_coefficients"] = out
        logger.info("Saved %s", out)
    else:
        logger.warning("No valid coefficients for coefficient comparison figure.")

    # ---- Figure 2: Bucket-specific supply response bar chart ---------------
    bucket_results = _run_per_bucket_regressions(panel)

    if not bucket_results.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        br = bucket_results.sort_values("maturity_bucket")
        x_pos = np.arange(len(br))
        bar_colors = []
        for _, row in br.iterrows():
            if row["p_value"] < 0.05:
                bar_colors.append("#2ca02c")
            elif row["p_value"] < 0.10:
                bar_colors.append("#ff7f0e")
            else:
                bar_colors.append("#999999")

        ax.bar(x_pos, br["beta"].values, yerr=1.96 * br["se"].values,
               color=bar_colors, capsize=5, alpha=0.8, edgecolor="black",
               linewidth=0.5)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(br["maturity_bucket"].values, rotation=30, ha="right")
        ax.axhline(0, color="gray", linewidth=0.6, linestyle="--")
        ax.set_ylabel("Coefficient on announced supply ($B)")
        ax.set_title("Supply -> Dealer Inventory by Maturity Bucket")

        # Add significance legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor="#2ca02c", edgecolor="black", linewidth=0.5, label="p < 0.05"),
            Patch(facecolor="#ff7f0e", edgecolor="black", linewidth=0.5, label="p < 0.10"),
            Patch(facecolor="#999999", edgecolor="black", linewidth=0.5, label="p >= 0.10"),
        ]
        ax.legend(handles=legend_elements, loc="upper right")

        # Annotate observation counts
        for i, (_, row) in enumerate(br.iterrows()):
            ax.annotate(
                f"n={row['n_obs']:.0f}",
                (x_pos[i], 0),
                textcoords="offset points", xytext=(0, -18),
                ha="center", fontsize=7, color="gray",
            )

        ax.grid(axis="y", alpha=0.3)
        plt.tight_layout()

        out = figures_dir / "panel_fe_bucket_response.png"
        fig.savefig(out, dpi=150)
        plt.close(fig)
        paths["panel_fe_bucket_response"] = out
        logger.info("Saved %s", out)
    else:
        logger.warning("No per-bucket results for bucket response figure.")

    return paths


# ---------------------------------------------------------------------------
# 5.  Table
# ---------------------------------------------------------------------------

def generate_panel_fe_table(
    results: dict[str, object],
    tables_dir: str | Path,
) -> Path:
    """Save panel FE results to a CSV table with all specs side by side.

    Parameters
    ----------
    results : dict
        Output of ``run_bucket_fe_regression``.
    tables_dir : str or Path
        Directory in which to save the CSV.

    Returns
    -------
    Path
        Path to the written ``panel_fe_results.csv``.
    """
    tables_dir = Path(tables_dir)
    tables_dir.mkdir(parents=True, exist_ok=True)

    spec_names = ["pooled", "bucket_fe", "twoway_fe", "interaction"]
    params_of_interest = ["supply_B", "lagged_dealer_share", "supply_x_soft_demand"]
    param_labels = {
        "supply_B": "Announced supply ($B)",
        "lagged_dealer_share": "Lagged dealer share",
        "supply_x_soft_demand": "Supply x Soft demand (lag)",
        "const": "Constant",
    }

    rows = []
    for spec in spec_names:
        res = results.get(spec)
        if res is None:
            continue

        for param in params_of_interest:
            info = _extract_coef(res, param)
            if np.isnan(info["beta"]):
                continue
            rows.append({
                "specification": spec,
                "variable": param_labels.get(param, param),
                "coefficient": round(info["beta"], 4),
                "std_error": round(info["se"], 4),
                "ci_lower": round(info["ci_lower"], 4),
                "ci_upper": round(info["ci_upper"], 4),
            })

        # Add model-level statistics
        rows.append({
            "specification": spec,
            "variable": "N observations",
            "coefficient": _extract_nobs(res),
            "std_error": np.nan,
            "ci_lower": np.nan,
            "ci_upper": np.nan,
        })
        rows.append({
            "specification": spec,
            "variable": "R-squared",
            "coefficient": round(_extract_r2(res), 4),
            "std_error": np.nan,
            "ci_lower": np.nan,
            "ci_upper": np.nan,
        })

        # Add FE flags
        has_bucket_fe = spec in ("bucket_fe", "twoway_fe", "interaction")
        has_week_fe = spec in ("twoway_fe", "interaction")
        rows.append({
            "specification": spec,
            "variable": "Bucket FE",
            "coefficient": "Yes" if has_bucket_fe else "No",
            "std_error": np.nan,
            "ci_lower": np.nan,
            "ci_upper": np.nan,
        })
        rows.append({
            "specification": spec,
            "variable": "Week FE",
            "coefficient": "Yes" if has_week_fe else "No",
            "std_error": np.nan,
            "ci_lower": np.nan,
            "ci_upper": np.nan,
        })

    table = pd.DataFrame(rows)
    out = tables_dir / "panel_fe_results.csv"
    table.to_csv(out, index=False)
    logger.info("Saved panel FE results table to %s (%d rows)", out, len(table))
    return out
