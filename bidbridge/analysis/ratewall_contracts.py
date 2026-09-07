from __future__ import annotations

from pathlib import Path

import pandas as pd

CONTRACT_VERSION = "bidbridge_ratewall_dealer_warehousing_support_v1"
BLOCKED_USE = "final_holder_split;customer_identity;source_backed_private_bucket_split"

FIELDS = [
    "support_row_id",
    "contract_version",
    "producer_project",
    "producer_artifact",
    "route_component_id",
    "object_family",
    "source_family",
    "native_frequency",
    "week_start",
    "week_end",
    "awarded_amount_total",
    "dealer_allotment_share",
    "pd_treasury_inventory",
    "pd_financing_usage",
    "inventory_change",
    "dealer_bridge_ratio",
    "bridge_episode",
    "persistence_half_life_weeks",
    "observed_field_status",
    "route_interpretation_status",
    "evidence_tier",
    "measurement_stage",
    "mapping_burden",
    "assumption_status",
    "admissible_use",
    "blocked_use",
    "claim_boundary",
]


def _fmt(value: object) -> str:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return ""
    return f"{float(parsed):.6f}".rstrip("0").rstrip(".")


def build_ratewall_dealer_warehousing_support_contract(
    panel: pd.DataFrame,
    *,
    persistence_summary: pd.DataFrame | None = None,
    producer_artifact: str = "auction_week_panel.csv",
) -> pd.DataFrame:
    half_life = ""
    if persistence_summary is not None and not persistence_summary.empty:
        half_life = _fmt(persistence_summary.iloc[0].get("halflife_weeks", ""))

    rows: list[dict[str, str]] = []
    for _, row in panel.iterrows():
        week_start = str(row.get("week_start", ""))[:10]
        if not week_start:
            continue
        rows.append(
            {
                "support_row_id": f"bidbridge::dealer_warehousing::{week_start}",
                "contract_version": CONTRACT_VERSION,
                "producer_project": "bidbridge",
                "producer_artifact": producer_artifact,
                "route_component_id": "dealer_warehousing",
                "object_family": "dealer_inventory_financing_bridge",
                "source_family": (
                    "nyfed_primary_dealer_stats;treasury_investor_class_allotments"
                ),
                "native_frequency": "weekly",
                "week_start": week_start,
                "week_end": str(row.get("week_end", ""))[:10],
                "awarded_amount_total": _fmt(row.get("awarded_amount_total", "")),
                "dealer_allotment_share": _fmt(row.get("dealer_share_allotment", "")),
                "pd_treasury_inventory": _fmt(row.get("pd_treasury_inventory", "")),
                "pd_financing_usage": _fmt(row.get("pd_financing_usage", "")),
                "inventory_change": _fmt(row.get("inventory_change", "")),
                "dealer_bridge_ratio": _fmt(row.get("dealer_bridge_ratio", "")),
                "bridge_episode": str(row.get("bridge_episode", "false")).lower(),
                "persistence_half_life_weeks": half_life,
                "observed_field_status": "source_backed_measurement",
                "route_interpretation_status": "bounded_proxy",
                "evidence_tier": "bounded_proxy",
                "measurement_stage": "transient_bridge",
                "mapping_burden": "requires_terminal_holder_inference",
                "assumption_status": "bounded_by_observed_context",
                "admissible_use": "assumption_mode_support_ledger;assumption_mode_sensitivity",
                "blocked_use": BLOCKED_USE,
                "claim_boundary": (
                    "dealer_warehousing_timing_bound_not_terminal_holder_split"
                ),
            }
        )
    return pd.DataFrame(rows, columns=FIELDS)


def write_ratewall_dealer_warehousing_support_contract(
    *,
    panel_path: Path | str,
    output_path: Path | str,
    persistence_summary_path: Path | str | None = None,
) -> tuple[Path, pd.DataFrame]:
    panel_path = Path(panel_path)
    persistence = None
    if persistence_summary_path is not None:
        persistence_path = Path(persistence_summary_path)
        if not persistence_path.is_file():
            raise FileNotFoundError(
                f"Required persistence summary is missing: {persistence_path}"
            )
        persistence = pd.read_csv(persistence_path)
        if persistence.empty:
            raise ValueError(f"Required persistence summary is empty: {persistence_path}")
    frame = build_ratewall_dealer_warehousing_support_contract(
        pd.read_csv(panel_path),
        persistence_summary=persistence,
        producer_artifact=panel_path.name,
    )
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target, index=False, lineterminator="\n")
    return target, frame
