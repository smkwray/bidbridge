from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bidbridge.features.auction_week import normalize_week_definition, week_start


def build_upcoming_pressure_monitor(
    panel: pd.DataFrame,
    upcoming_auctions: pd.DataFrame,
    horizon_weeks: int = 4,
    week_definition: str = "monday",
) -> pd.DataFrame:
    """Score near-term auction weeks using existing bridge/heavy-supply logic."""
    keep_cols = [
        "week_start", "week_definition", "weeks_ahead", "auction_count",
        "total_offering_amount", "bill_share", "heavy_supply_threshold",
        "recent_bridge_rate", "recent_weak_demand_rate", "supply_size_score",
        "bill_share_score", "composite_pressure_score", "pressure_category",
    ]
    df = panel.copy()
    df["week_start"] = pd.to_datetime(df["week_start"])
    df = df.sort_values("week_start").reset_index(drop=True)

    upcoming = upcoming_auctions.copy()
    if upcoming.empty:
        return pd.DataFrame(columns=keep_cols)

    upcoming["auction_date"] = pd.to_datetime(upcoming["auction_date"], errors="coerce")
    upcoming["week_start"] = week_start(
        upcoming["auction_date"], week_definition=week_definition,
    )

    current_week = df["week_start"].max() if not df.empty else upcoming["week_start"].min()
    recent_panel = df.tail(13)

    def _mean_or_zero(series: pd.Series) -> float:
        value = series.mean()
        return 0.0 if pd.isna(value) else float(value)

    heavy_supply_threshold = float(
        df["announced_amount_total"].expanding(min_periods=13).quantile(0.75).iloc[-1]
    ) if len(df) else 0.0
    recent_bridge_rate = _mean_or_zero(recent_panel.get("bridge_episode", pd.Series(dtype=float)))
    recent_weak_demand_rate = _mean_or_zero(
        recent_panel.get("weak_end_investor_absorption", pd.Series(dtype=float))
    )

    grouped = (
        upcoming.groupby("week_start", sort=True)
        .agg(
            auction_count=("week_start", "size"),
            total_offering_amount=("offering_amount", "sum"),
            bill_amount=("security_type", lambda s: float(upcoming.loc[s.index, "offering_amount"][s.eq("Bill")].sum())),
        )
        .reset_index()
    )
    grouped["bill_share"] = (
        grouped["bill_amount"] / grouped["total_offering_amount"].replace({0: pd.NA})
    ).fillna(0.0)
    grouped["weeks_ahead"] = (
        (grouped["week_start"] - current_week) / pd.Timedelta(weeks=1)
    ).astype(int)
    grouped = grouped[
        (grouped["weeks_ahead"] >= 0) & (grouped["weeks_ahead"] < horizon_weeks)
    ].copy()

    if grouped.empty:
        return pd.DataFrame(columns=keep_cols)

    grouped["heavy_supply_threshold"] = heavy_supply_threshold
    grouped["recent_bridge_rate"] = recent_bridge_rate
    grouped["recent_weak_demand_rate"] = recent_weak_demand_rate
    if heavy_supply_threshold > 0:
        grouped["supply_size_score"] = (
            grouped["total_offering_amount"] / heavy_supply_threshold
        ).clip(lower=0.0, upper=1.0)
    else:
        grouped["supply_size_score"] = 0.0
    grouped["bill_share_score"] = grouped["bill_share"].clip(lower=0.0, upper=1.0)
    grouped["composite_pressure_score"] = (
        0.40 * grouped["supply_size_score"]
        + 0.20 * grouped["bill_share_score"]
        + 0.20 * grouped["recent_bridge_rate"].clip(lower=0.0, upper=1.0)
        + 0.20 * grouped["recent_weak_demand_rate"].clip(lower=0.0, upper=1.0)
    ).round(4)

    def _categorize(score: float) -> str:
        if score >= 0.66:
            return "high"
        if score >= 0.33:
            return "medium"
        return "low"

    grouped["pressure_category"] = grouped["composite_pressure_score"].map(_categorize)
    grouped["week_definition"] = normalize_week_definition(week_definition)

    return grouped[keep_cols].sort_values("week_start").reset_index(drop=True)


def write_upcoming_pressure_monitor(
    panel: pd.DataFrame,
    upcoming_auctions: pd.DataFrame,
    csv_path: str | Path,
    json_path: str | Path,
    horizon_weeks: int = 4,
    week_definition: str = "monday",
) -> dict[str, Path]:
    """Write upcoming bridge-pressure monitor artifacts."""
    csv_path = Path(csv_path)
    json_path = Path(json_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    monitor = build_upcoming_pressure_monitor(
        panel,
        upcoming_auctions,
        horizon_weeks=horizon_weeks,
        week_definition=week_definition,
    )
    monitor.to_csv(csv_path, index=False)
    json_records = monitor.astype(object).where(pd.notna(monitor), None)
    if "week_start" in json_records.columns:
        json_records["week_start"] = json_records["week_start"].astype(str)
    json_path.write_text(
        json.dumps(json_records.to_dict(orient="records"), indent=2, allow_nan=False),
        encoding="utf-8",
    )
    return {
        "pressure_monitor_csv": csv_path,
        "pressure_monitor_json": json_path,
    }
