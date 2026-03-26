from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd

from bidbridge.analysis.regressions import (
    run_extended_bridge_regression,
    run_refunding_test,
)
from bidbridge.data.registry import get_source_registry


def _clean_value(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, dict):
        return {k: _clean_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean_value(v) for v in value]
    return value


def _serialize_lp(lp_results: dict[str, pd.DataFrame]) -> dict[str, list[dict[str, object]]]:
    payload: dict[str, list[dict[str, object]]] = {}
    for key, df in lp_results.items():
        if df.empty:
            payload[key] = []
            continue
        payload[key] = [
            {
                "h": int(row["horizon"]),
                "beta": float(row["beta"]),
                "se": float(row["se"]),
                "p": float(row["p_value"]),
                "ci_lo": float(row["ci_lower"]),
                "ci_hi": float(row["ci_upper"]),
            }
            for _, row in df.iterrows()
        ]
    return payload


def build_site_payload(
    panel: pd.DataFrame,
    lp_results: dict[str, pd.DataFrame],
    stress_summary: pd.DataFrame,
    bridge_summary: pd.DataFrame,
    pressure_monitor: pd.DataFrame | None = None,
    maturity_panel: pd.DataFrame | None = None,
) -> dict[str, object]:
    """Build the JSON payload consumed by the static site."""
    df = panel.copy()
    df["week_start"] = pd.to_datetime(df["week_start"])

    annual = (
        df.assign(year=df["week_start"].dt.year)
        .groupby("year")
        .agg(
            auctions=("auction_count", "sum"),
            awarded_B=("awarded_amount_total", lambda s: float(s.sum() / 1e9)),
            dealer_share=("dealer_share_allotment", "mean"),
            inventory_M=("pd_treasury_inventory", "mean"),
            bridge_episodes=("bridge_episode", "sum"),
        )
        .reset_index()
    )

    regression_extended = run_extended_bridge_regression(df.dropna(subset=["inventory_change"]).copy())
    regression_payload = {
        row["term"]: {
            "coef": float(row["coefficient"]),
            "se": float(row["std_error"]),
            "t": float(row["t_stat"]),
            "p": float(row["p_value"]),
        }
        for _, row in regression_extended.iterrows()
    }

    refunding_test = run_refunding_test(df)
    def _mean_or_zero(series: pd.Series) -> float:
        value = series.mean()
        return 0.0 if pd.isna(value) else float(value)

    _BUCKET_ORDER = ["bills", "short_coupon", "belly_coupon", "long_coupon", "tips", "frns"]
    if (
        maturity_panel is not None
        and "maturity_bucket" in maturity_panel.columns
        and "dealer_share" in maturity_panel.columns
    ):
        bucket_means = (
            maturity_panel.groupby("maturity_bucket")["dealer_share"]
            .mean()
            .to_dict()
        )
        maturity_payload = {
            b: {"avg_dealer_share": float(bucket_means.get(b, 0.0))}
            for b in _BUCKET_ORDER
        }
    else:
        # Fallback: overall mean (imprecise, but won't crash)
        overall = _mean_or_zero(df["dealer_share_allotment"])
        bill_share = df.get("bill_share", pd.Series(0.0, index=df.index))
        maturity_payload = {
            "bills": {"avg_dealer_share": _mean_or_zero(df.loc[bill_share > 0.5, "dealer_share_allotment"])},
        }
        for b in _BUCKET_ORDER[1:]:
            maturity_payload[b] = {"avg_dealer_share": overall}

    data_sources = [
        {
            "name": record.label,
            "provider": record.source_id.replace("_", " ").title(),
            "freq": record.frequency,
            "records": 0,
            "desc": record.notes,
            "fields": record.grain,
            "url": record.page_url,
        }
        for record in get_source_registry()
    ]

    payload = {
        "panel_stats": {
            "total_weeks": int(len(df)),
            "dealer_observed_weeks": int(df["pd_treasury_inventory"].notna().sum()) if "pd_treasury_inventory" in df.columns else 0,
            "date_range": (
                f"{df['week_start'].min().date()} to {df['week_start'].max().date()}"
                if len(df) else ""
            ),
            "bridge_episodes": int(df.get("bridge_episode", pd.Series(dtype=int)).sum()),
            "columns": int(len(df.columns)),
        },
        "lp_results": _serialize_lp(lp_results),
        "scatter": {
            "supply_B": (df["awarded_amount_total"] / 1e9).round(3).tolist(),
            "dealer_share_pct": (df["dealer_share_allotment"] * 100).round(2).tolist(),
            "weeks": df["week_start"].dt.strftime("%Y-%m-%d").tolist(),
        },
        "maturity_buckets": maturity_payload,
        "refunding_test": refunding_test.to_dict(orient="records"),
        "timeseries": {
            "weeks": df["week_start"].dt.strftime("%Y-%m-%d").tolist(),
            "supply_B": (df["awarded_amount_total"] / 1e9).round(3).tolist(),
            "inventory_B": (df.get("pd_treasury_inventory", pd.Series(0, index=df.index)) / 1e3).round(3).tolist(),
            "dealer_share_pct": (df.get("dealer_share_allotment", pd.Series(0, index=df.index)) * 100).round(2).tolist(),
            "bridge": df.get("bridge_episode", pd.Series(0, index=df.index)).astype(int).tolist(),
            "soma_T": (df.get("soma_treasury_total", pd.Series(0, index=df.index)) / 1e12).round(4).tolist(),
            "btc": df.get("weighted_bid_to_cover", pd.Series(0, index=df.index)).round(3).tolist(),
        },
        "annual_summary": annual.to_dict(orient="records"),
        "stress_summary": stress_summary.to_dict(orient="records"),
        "bridge_summary": bridge_summary.to_dict(orient="records"),
        "regression_extended": regression_payload,
        "data_sources": data_sources,
    }
    if pressure_monitor is not None:
        payload["pressure_monitor"] = pressure_monitor.assign(
            week_start=pressure_monitor["week_start"].astype(str)
        ).to_dict(orient="records")
    return _clean_value(payload)


def write_site_data(
    panel: pd.DataFrame,
    lp_results: dict[str, pd.DataFrame],
    stress_summary: pd.DataFrame,
    bridge_summary: pd.DataFrame,
    output_path: str | Path,
    pressure_monitor: pd.DataFrame | None = None,
    maturity_panel: pd.DataFrame | None = None,
) -> Path:
    """Write the site JSON payload."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_site_payload(
        panel,
        lp_results,
        stress_summary,
        bridge_summary,
        pressure_monitor=pressure_monitor,
        maturity_panel=maturity_panel,
    )
    output_path.write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")
    return output_path
