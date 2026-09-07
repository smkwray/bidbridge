"""Maturity-bucket panel fixed-effects regression module.

Estimates within-week, across-curve effects of auction supply on primary
dealer inventory using a two-way fixed-effects specification:

    delta_position_{b,t} = alpha_b + tau_t + beta * announced_amount_{b,t}
                         + epsilon_{b,t}

Where:
    b = maturity bucket (bills, short_coupon, belly_coupon, long_coupon, tips, frns)
    t = week
    alpha_b = bucket fixed effect
    tau_t = week fixed effect

The primary estimate is supply-only so the fitted sample cannot be selected by
same-auction allotment outcomes.  A separately labeled robustness adds the
strict previous-calendar-week dealer share without substituting current values.

Dealer position mapping:
    pd_bills_position  -> bills
    Coupon maturity bands (by remaining maturity) -> short/belly/long buckets:
        short_coupon = pd_coupon_le2y + pd_coupon_2_3y       (<=3yr remaining)
        belly_coupon = pd_coupon_3_6y + pd_coupon_6_7y       (3-7yr remaining)
        long_coupon  = pd_coupon_7_11y + pd_coupon_11_21y + pd_coupon_gt21y (>7yr)
    pd_tips_position   -> tips
    pd_frn_position    -> frns (when available)

    Note: NY Fed bands are by *remaining* maturity, not original tenor. Each
    study bucket therefore contains newly-issued securities of matching tenor
    plus aging securities from longer tenors that have rolled down the curve.
    This is standard in empirical Treasury research and eliminates the
    mechanical endogeneity of the previous proportional-split approach.

Timing caveat:
    NY Fed PD positions are Wednesday as-of snapshots.  Auctions run Mon-Fri
    with T+1 settlement.  Thu/Fri auctions may not be reflected in the same
    week's Wednesday position.  This creates attenuation bias (beta biased
    toward zero), making estimates conservative.  A Thursday-start week
    robustness check would tighten alignment but is not yet implemented.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

plt.switch_backend("Agg")

logger = logging.getLogger(__name__)

# Coupon sub-buckets that share pd_coupon_position
_COUPON_BUCKETS = {"short_coupon", "belly_coupon", "long_coupon"}

# All recognized maturity buckets
_ALL_BUCKETS = {"bills", "short_coupon", "belly_coupon", "long_coupon", "tips", "frns"}

# Direct mapping from NY Fed coupon maturity bands to study buckets.
# Bands are by remaining maturity; see module docstring for caveats.
#
# long_coupon uses split bands (11-21yr + >21yr) when available (SBN2022+),
# falling back to the combined >11yr band (pd_coupon_gt11y) for pre-2022 data.
_BAND_TO_BUCKET = {
    "short_coupon": ["pd_coupon_le2y", "pd_coupon_2_3y"],
    "belly_coupon": ["pd_coupon_3_6y", "pd_coupon_6_7y"],
    "long_coupon": ["pd_coupon_7_11y", "pd_coupon_11_21y", "pd_coupon_gt21y"],
}

# Fallback for long_coupon when split bands are missing (pre-2022 series)
_LONG_COUPON_FALLBACK = ["pd_coupon_7_11y", "pd_coupon_gt11y"]

# Minimum columns needed to use granular path (short + belly + 7-11yr;
# long_coupon can use either split or combined >11yr)
GRANULAR_MIN_COLS = [
    "pd_coupon_le2y", "pd_coupon_2_3y",   # short_coupon
    "pd_coupon_3_6y", "pd_coupon_6_7y",   # belly_coupon
    "pd_coupon_7_11y",                     # long_coupon (always needed)
]


# ---------------------------------------------------------------------------
# 1.  Build bucket-level outcome panel
# ---------------------------------------------------------------------------

def build_bucket_outcomes(
    maturity_panel: pd.DataFrame,
    dealer_stats: pd.DataFrame,
    headline_strict: bool = False,
    week_definition: str = "monday",
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

    # ---- Detect whether granular coupon bands are available ---------------
    _has_granular = all(col in ds.columns for col in GRANULAR_MIN_COLS)
    if not _has_granular:
        if headline_strict:
            raise ValueError(
                "Granular coupon band columns are required for headline FE outputs."
            )
        logger.warning(
            "Granular coupon band columns not found in dealer_stats. "
            "Falling back to proportional split of pd_coupon_position. "
            "Re-run `bidbridge fetch` to get granular data and eliminate "
            "mechanical endogeneity in coupon-bucket position assignment."
        )

    # ---- Step 1: Merge dealer stats onto each (week, bucket) row ---------
    ds_cols = ["week_start", "pd_bills_position", "pd_tips_position"]
    if "pd_frn_position" in ds.columns:
        ds_cols.append("pd_frn_position")
    if _has_granular:
        # Include all available band columns (min set + optional split/combined)
        for col in GRANULAR_MIN_COLS:
            if col not in ds_cols:
                ds_cols.append(col)
        for col in ["pd_coupon_11_21y", "pd_coupon_gt21y", "pd_coupon_gt11y"]:
            if col in ds.columns and col not in ds_cols:
                ds_cols.append(col)
    else:
        ds_cols.append("pd_coupon_position")

    # De-duplicate dealer stats by week_start (take last observation)
    ds_dedup = ds[ds_cols].drop_duplicates(subset=["week_start"], keep="last")

    panel = mp.merge(ds_dedup, on="week_start", how="inner")

    # ---- Step 2: Expand to a balanced panel ------------------------------
    # Every (week_start, maturity_bucket) gets a row so that zero-supply
    # weeks are represented for the FE regression.
    # Use the union of weeks from maturity_panel and dealer_stats so that
    # weeks with dealer data but no auctions are not silently dropped.
    all_weeks_set = set(panel["week_start"].unique()) | set(ds_dedup["week_start"].unique())
    all_weeks = pd.DataFrame({"week_start": sorted(all_weeks_set)})
    all_buckets = pd.DataFrame({"maturity_bucket": list(_ALL_BUCKETS)})
    balanced = all_weeks.merge(all_buckets, how="cross")
    panel = balanced.merge(panel, on=["week_start", "maturity_bucket"], how="left")
    panel["awarded_amount"] = panel["awarded_amount"].fillna(0)
    panel["announced_amount"] = panel["announced_amount"].fillna(0)
    # dealer_share is left as NaN for non-auction weeks (no auction -> no share)

    # Re-merge dealer stats for rows added by balancing
    ds_merge_cols = [c for c in ds_dedup.columns if c != "week_start"]
    for col in ds_merge_cols:
        if col in panel.columns:
            panel[col] = panel[col].combine_first(
                panel[["week_start"]].merge(
                    ds_dedup[["week_start", col]], on="week_start", how="left"
                )[col]
            )

    # ---- Step 3: Assign bucket-level position ----------------------------
    if _has_granular:
        # Direct mapping: sum observed NY Fed maturity bands per bucket.
        # No dependence on auction volumes — eliminates mechanical endogeneity.
        def _assign_position(row: pd.Series) -> float:
            bucket = row["maturity_bucket"]
            if bucket == "bills":
                return row.get("pd_bills_position", np.nan)
            if bucket == "tips":
                return row.get("pd_tips_position", np.nan)
            if bucket == "frns":
                return row.get("pd_frn_position", np.nan)
            if bucket == "long_coupon":
                # Try split bands first (11-21yr + >21yr, available SBN2022+);
                # fall back to combined >11yr (pd_coupon_gt11y, pre-2022).
                band_cols = _BAND_TO_BUCKET["long_coupon"]
                vals = [row.get(c, np.nan) for c in band_cols]
                if all(pd.notna(v) for v in vals):
                    return sum(vals)
                # Fallback: 7-11yr + combined >11yr
                fb_vals = [row.get(c, np.nan) for c in _LONG_COUPON_FALLBACK]
                if all(pd.notna(v) for v in fb_vals):
                    return sum(fb_vals)
                return np.nan
            if bucket in _BAND_TO_BUCKET:
                band_cols = _BAND_TO_BUCKET[bucket]
                vals = [row.get(c, np.nan) for c in band_cols]
                # Require ALL constituent bands to be non-null.
                # A missing band would silently understate the position.
                if any(pd.isna(v) for v in vals):
                    return np.nan
                return sum(vals)
            return np.nan
    else:
        # Fallback: proportional split of aggregate pd_coupon_position.
        # WARNING: this creates mechanical endogeneity — the LHS depends on
        # awarded_amount which is collinear with the RHS announced_amount.
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

    # Step 5: Strict previous-calendar-week dealer share.  Use a dated join,
    # not a row shift, so a missing calendar week can never silently become a
    # longer and undeclared lag.  Missing values remain missing; the current
    # auction outcome is never substituted.
    lagged_share = panel[
        ["week_start", "maturity_bucket", "dealer_share"]
    ].copy()
    lagged_share["week_start"] = lagged_share["week_start"] + pd.Timedelta(days=7)
    lagged_share = lagged_share.rename(
        columns={"dealer_share": "lagged_dealer_share"}
    )
    panel = panel.merge(
        lagged_share,
        on=["week_start", "maturity_bucket"],
        how="left",
        validate="one_to_one",
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
    result.attrs["uses_granular_bands"] = bool(_has_granular)
    result.attrs["headline_fe_eligible"] = bool(_has_granular)
    result.attrs["week_definition"] = week_definition
    return result


# ---------------------------------------------------------------------------
# 2.  Panel FE regression suite
# ---------------------------------------------------------------------------

def _driscoll_kraay_bandwidth(n_periods: int) -> int:
    """Return the explicit Bartlett bandwidth used by linearmodels 7.0."""
    if n_periods < 1:
        raise ValueError("Driscoll-Kraay inference requires at least one period.")
    return int(np.floor(4 * (n_periods / 100) ** (2 / 9)))


def _strict_lag_matches_calendar_week(panel: pd.DataFrame) -> bool:
    """Verify that the stored lag is exactly t-7 days, including missingness."""
    expected = panel[["week_start", "maturity_bucket", "dealer_share"]].copy()
    expected["week_start"] = pd.to_datetime(expected["week_start"]) + pd.Timedelta(days=7)
    expected = expected.rename(columns={"dealer_share": "expected_lag"})
    check = panel.merge(
        expected,
        on=["week_start", "maturity_bucket"],
        how="left",
        validate="one_to_one",
    )
    actual = pd.to_numeric(check["lagged_dealer_share"], errors="coerce")
    wanted = pd.to_numeric(check["expected_lag"], errors="coerce")
    return bool(np.all(np.isclose(actual, wanted, equal_nan=True)))


def run_bucket_fe_regression(panel: pd.DataFrame) -> dict[str, object]:
    """Run panel fixed-effects regressions of dealer inventory changes on supply.

    The primary specification is supply-only with maturity-bucket and week
    fixed effects.  A separately labeled robustness adds only the strict
    previous-calendar-week dealer share and never substitutes the current
    auction outcome.

    Uses ``linearmodels.panel.PanelOLS``.  The headline covariance is
    Driscoll-Kraay with an explicit Bartlett kernel and bandwidth.

    Parameters
    ----------
    panel : DataFrame
        Output of ``build_bucket_outcomes``.  Must contain: week_start,
        maturity_bucket, delta_position, and announced_amount.  The optional
        strict-lag robustness also uses lagged_dealer_share.

    Returns
    -------
    dict
        Fitted supply-only models, the strict-lag robustness, and exact sample
        and covariance metadata.
    """
    df = panel.copy()
    n_theoretical = len(df)
    df = df.dropna(subset=["delta_position", "announced_amount"]).copy()
    n_actual = len(df)
    n_weeks = df["week_start"].nunique() if "week_start" in df.columns else 0
    n_buckets = df["maturity_bucket"].nunique() if "maturity_bucket" in df.columns else 0
    logger.info(
        "Panel FE estimation sample: %d of %d obs retained (%.1f%%), "
        "%d weeks x %d buckets",
        n_actual, n_theoretical,
        100 * n_actual / n_theoretical if n_theoretical > 0 else 0,
        n_weeks, n_buckets,
    )

    if len(df) < 20:
        raise ValueError(
            f"Insufficient observations after dropping NaN: {len(df)} rows."
        )

    # Normalize supply to billions for interpretable coefficients
    df["supply_B"] = df["announced_amount"] / 1e9

    if not _strict_lag_matches_calendar_week(panel):
        raise RuntimeError(
            "lagged_dealer_share is not the strict previous-calendar-week value."
        )
    strict_lag = df.dropna(subset=["lagged_dealer_share"]).copy()
    if len(strict_lag) < 20:
        raise ValueError(
            "Insufficient strict-lag observations after dropping missing values: "
            f"{len(strict_lag)} rows."
        )

    # NOTE: With only 6 maturity buckets the number of clusters is well below
    # the ~30-cluster rule of thumb for cluster-robust standard errors.
    # Entity-clustered SEs may therefore be downward-biased.  Results should
    # be interpreted with caution.  As a robustness check the linearmodels
    # path also computes Driscoll-Kraay (kernel) SEs, which are consistent
    # under cross-sectional and temporal dependence regardless of cluster
    # count.

    primary_bandwidth = _driscoll_kraay_bandwidth(int(df["week_start"].nunique()))
    strict_bandwidth = _driscoll_kraay_bandwidth(
        int(strict_lag["week_start"].nunique())
    )
    results = _run_with_linearmodels(
        df,
        strict_lag,
        primary_bandwidth=primary_bandwidth,
        strict_bandwidth=strict_bandwidth,
    )

    twoway = results.get("twoway_fe_supply_only_driscoll_kraay")
    n_estimated = _extract_nobs(twoway) if twoway else n_actual
    estimation_rows = df.dropna(subset=["delta_position", "supply_B"])
    if len(estimation_rows) != n_estimated:
        raise RuntimeError(
            "Could not bind the panel-FE estimation sample: "
            f"prepared {len(estimation_rows)} rows but the model used {n_estimated}."
        )

    n_raw_panel_weeks = int(panel["week_start"].nunique())
    _sample_info = {
        "n_theoretical": n_theoretical,
        "n_after_dropna": n_actual,
        "n_estimated": n_estimated,
        "pct_retained": round(100 * n_estimated / n_theoretical, 1) if n_theoretical else 0,
        "n_weeks": n_weeks,
        "n_buckets": n_buckets,
        "n_estimation_weeks": int(estimation_rows["week_start"].nunique()),
        "n_raw_panel_weeks": n_raw_panel_weeks,
        "first_difference_week_loss": (
            n_raw_panel_weeks - int(estimation_rows["week_start"].nunique())
        ),
        "sample_start": estimation_rows["week_start"].min().date().isoformat(),
        "sample_end": estimation_rows["week_start"].max().date().isoformat(),
    }
    strict_result = results["twoway_fe_strict_lag_driscoll_kraay"]
    strict_n = _extract_nobs(strict_result)
    if strict_n != len(strict_lag):
        raise RuntimeError(
            "Could not bind the strict-lag robustness sample: "
            f"prepared {len(strict_lag)} rows but the model used {strict_n}."
        )
    _strict_lag_sample_info = {
        "n_estimated": strict_n,
        "n_estimation_weeks": int(strict_lag["week_start"].nunique()),
        "sample_start": strict_lag["week_start"].min().date().isoformat(),
        "sample_end": strict_lag["week_start"].max().date().isoformat(),
        "current_share_substitutions": 0,
        "strict_previous_calendar_week_verified": True,
    }
    logger.info(
        "Panel FE sample: %d theoretical -> %d after dropna -> %d in estimation (%.1f%%)",
        n_theoretical, n_actual, n_estimated, _sample_info["pct_retained"],
    )

    results["_sample_info"] = _sample_info
    results["_strict_lag_sample_info"] = _strict_lag_sample_info
    results["_metadata"] = {
        "uses_granular_bands": bool(panel.attrs.get("uses_granular_bands", False)),
        "headline_fe_eligible": bool(panel.attrs.get("headline_fe_eligible", False)),
        "week_definition": panel.attrs.get("week_definition", "monday"),
        "headline_specification": "supply_only_two_way_fixed_effects",
        "headline_covariance": "Driscoll-Kraay",
        "driscoll_kraay_kernel": "bartlett",
        "driscoll_kraay_bandwidth": primary_bandwidth,
        "strict_lag_driscoll_kraay_bandwidth": strict_bandwidth,
        "current_share_substitutions": 0,
    }
    return results


def _run_with_linearmodels(
    df: pd.DataFrame,
    strict_lag: pd.DataFrame,
    *,
    primary_bandwidth: int,
    strict_bandwidth: int,
) -> dict[str, object]:
    """Panel FE estimation via linearmodels.panel.PanelOLS."""
    from linearmodels.panel import PanelOLS, PooledOLS

    # Set multi-index required by linearmodels: (entity, time)
    df = df.copy()
    df["maturity_bucket"] = df["maturity_bucket"].astype("category")
    df = df.set_index(["maturity_bucket", "week_start"])
    strict_lag = strict_lag.copy()
    strict_lag["maturity_bucket"] = strict_lag["maturity_bucket"].astype("category")
    strict_lag = strict_lag.set_index(["maturity_bucket", "week_start"])

    dep = df["delta_position"]
    results = {}

    # ---- Supply-only primary family --------------------------------------
    exog_pooled = df[["supply_B"]].copy()
    exog_pooled.insert(0, "const", 1.0)
    mod_pooled = PooledOLS(dep, exog_pooled)
    results["pooled_supply_only"] = mod_pooled.fit(
        cov_type="clustered", cluster_entity=True
    )

    exog_fe = df[["supply_B"]]
    mod_bucket = PanelOLS(dep, exog_fe, entity_effects=True)
    results["bucket_fe_supply_only"] = mod_bucket.fit(
        cov_type="clustered", cluster_entity=True
    )

    mod_twoway = PanelOLS(dep, exog_fe, entity_effects=True, time_effects=True)
    results["twoway_fe_supply_only"] = mod_twoway.fit(
        cov_type="clustered", cluster_entity=True
    )
    results["twoway_fe_supply_only_driscoll_kraay"] = mod_twoway.fit(
        cov_type="kernel",
        kernel="bartlett",
        bandwidth=primary_bandwidth,
    )

    # ---- Strict previous-calendar-week control robustness ----------------
    strict_dep = strict_lag["delta_position"]
    strict_exog = strict_lag[["supply_B", "lagged_dealer_share"]]
    mod_strict = PanelOLS(
        strict_dep,
        strict_exog,
        entity_effects=True,
        time_effects=True,
    )
    results["twoway_fe_strict_lag"] = mod_strict.fit(
        cov_type="clustered", cluster_entity=True
    )
    results["twoway_fe_strict_lag_driscoll_kraay"] = mod_strict.fit(
        cov_type="kernel",
        kernel="bartlett",
        bandwidth=strict_bandwidth,
    )

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
        except (KeyError, ValueError, np.linalg.LinAlgError) as exc:
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
    file_prefix: str = "panel_fe",
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
    spec_names = [
        "pooled_supply_only",
        "bucket_fe_supply_only",
        "twoway_fe_supply_only_driscoll_kraay",
        "twoway_fe_strict_lag_driscoll_kraay",
    ]
    spec_labels = [
        "Pooled\nsupply-only",
        "Bucket FE\nsupply-only",
        "Two-way FE\nsupply-only (DK)",
        "Two-way FE\nstrict lag (DK)",
    ]
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

        out = figures_dir / f"{file_prefix}_coefficients.png"
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

        out = figures_dir / f"{file_prefix}_bucket_response.png"
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
    file_name: str = "panel_fe_results.csv",
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

    metadata = results.get("_metadata", {})
    spec_names = [
        "pooled_supply_only",
        "bucket_fe_supply_only",
        "twoway_fe_supply_only_driscoll_kraay",
        "twoway_fe_supply_only",
        "twoway_fe_strict_lag_driscoll_kraay",
        "twoway_fe_strict_lag",
    ]
    params_by_spec = {
        "pooled_supply_only": ["supply_B"],
        "bucket_fe_supply_only": ["supply_B"],
        "twoway_fe_supply_only_driscoll_kraay": ["supply_B"],
        "twoway_fe_supply_only": ["supply_B"],
        "twoway_fe_strict_lag_driscoll_kraay": [
            "supply_B",
            "lagged_dealer_share",
        ],
        "twoway_fe_strict_lag": ["supply_B", "lagged_dealer_share"],
    }
    param_labels = {
        "supply_B": "Announced supply ($B)",
        "lagged_dealer_share": "Strict previous-calendar-week dealer share",
        "const": "Constant",
    }

    rows = []
    for spec in spec_names:
        res = results.get(spec)
        if res is None:
            continue

        for param in params_by_spec[spec]:
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
        rows.append({
            "specification": spec,
            "variable": "Covariance",
            "coefficient": "Driscoll-Kraay" if "driscoll_kraay" in spec else "Clustered by bucket",
            "std_error": np.nan,
            "ci_lower": np.nan,
            "ci_upper": np.nan,
        })
        is_strict_lag = "strict_lag" in spec
        rows.append({
            "specification": spec,
            "variable": "Estimator role",
            "coefficient": "strict-lag robustness" if is_strict_lag else "primary supply-only",
            "std_error": np.nan,
            "ci_lower": np.nan,
            "ci_upper": np.nan,
        })
        rows.append({
            "specification": spec,
            "variable": "Current-share substitutions",
            "coefficient": 0,
            "std_error": np.nan,
            "ci_lower": np.nan,
            "ci_upper": np.nan,
        })
        if "driscoll_kraay" in spec:
            rows.append({
                "specification": spec,
                "variable": "Driscoll-Kraay kernel",
                "coefficient": metadata.get("driscoll_kraay_kernel", "bartlett"),
                "std_error": np.nan,
                "ci_lower": np.nan,
                "ci_upper": np.nan,
            })
            rows.append({
                "specification": spec,
                "variable": "Driscoll-Kraay bandwidth",
                "coefficient": metadata.get(
                    "strict_lag_driscoll_kraay_bandwidth"
                    if is_strict_lag
                    else "driscoll_kraay_bandwidth"
                ),
                "std_error": np.nan,
                "ci_lower": np.nan,
                "ci_upper": np.nan,
            })
        rows.append({
            "specification": spec,
            "variable": "Week definition",
            "coefficient": metadata.get("week_definition", "monday"),
            "std_error": np.nan,
            "ci_lower": np.nan,
            "ci_upper": np.nan,
        })
        rows.append({
            "specification": spec,
            "variable": "Granular bands required",
            "coefficient": "Yes" if metadata.get("headline_fe_eligible", False) else "No",
            "std_error": np.nan,
            "ci_lower": np.nan,
            "ci_upper": np.nan,
        })

        # Add FE flags
        has_bucket_fe = spec != "pooled_supply_only"
        has_week_fe = spec.startswith("twoway_fe")
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
    out = tables_dir / file_name
    table.to_csv(out, index=False, lineterminator="\n")
    logger.info("Saved panel FE results table to %s (%d rows)", out, len(table))
    return out
