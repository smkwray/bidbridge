from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bidbridge.analysis.panel_fe import GRANULAR_MIN_COLS
from bidbridge.features.auction_week import choose_investor_merge_keys


def build_data_audit(
    auctions: pd.DataFrame,
    investor_class: pd.DataFrame,
    dealer_stats: pd.DataFrame,
) -> dict[str, object]:
    """Build a compact audit payload for fragile joins and FE eligibility."""
    auctions_df = auctions.copy()
    investor_df = investor_class.copy()
    dealer_df = dealer_stats.copy()

    if "issue_date" in auctions_df.columns:
        auctions_df["issue_date"] = pd.to_datetime(auctions_df["issue_date"], errors="coerce")
    if "issue_date" in investor_df.columns:
        investor_df["issue_date"] = pd.to_datetime(investor_df["issue_date"], errors="coerce")

    merge_keys = choose_investor_merge_keys(auctions_df, investor_df)
    investor_deduped = investor_df.drop_duplicates(subset=merge_keys, keep="last")
    merged = auctions_df.merge(
        investor_deduped,
        on=merge_keys,
        how="left",
        suffixes=("", "_ic"),
        indicator=True,
    )

    granular_cols_present = all(col in dealer_df.columns for col in GRANULAR_MIN_COLS)
    granular_missing_weeks = 0
    if granular_cols_present and "week_start" in dealer_df.columns:
        granular_missing_weeks = int(
            dealer_df[GRANULAR_MIN_COLS]
            .isna()
            .any(axis=1)
            .sum()
        )

    financing_imputed_count = 0
    if "pd_repo_treasury_raw" in dealer_df.columns and "pd_reverse_repo_treasury_raw" in dealer_df.columns:
        financing_imputed_count = int(
            (
                dealer_df["pd_repo_treasury_raw"].isna()
                | dealer_df["pd_reverse_repo_treasury_raw"].isna()
            ).sum()
        )

    total_dealer_weeks = int(len(dealer_df))
    audit = {
        "merge_keys": merge_keys,
        "auction_rows": int(len(auctions_df)),
        "investor_rows": int(len(investor_df)),
        "dealer_weeks": total_dealer_weeks,
        "unmatched_investor_rows": int((merged["_merge"] != "both").sum()),
        "unmatched_investor_rate": (
            float((merged["_merge"] != "both").mean()) if len(merged) else 0.0
        ),
        "granular_coupon_columns_present": bool(granular_cols_present),
        "granular_coupon_missing_weeks": granular_missing_weeks,
        "headline_fe_eligible": bool(granular_cols_present),
        "financing_forward_fill_count": financing_imputed_count,
        "financing_forward_fill_share": (
            float(financing_imputed_count / total_dealer_weeks) if total_dealer_weeks else 0.0
        ),
    }
    return audit


def write_data_audit(
    auctions: pd.DataFrame,
    investor_class: pd.DataFrame,
    dealer_stats: pd.DataFrame,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Write CSV and JSON audit artifacts."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    audit = build_data_audit(auctions, investor_class, dealer_stats)

    csv_path = output_dir / "data_audit.csv"
    json_path = output_dir / "data_audit.json"

    pd.DataFrame(
        [{"metric": key, "value": value} for key, value in audit.items()]
    ).to_csv(csv_path, index=False)
    json_path.write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "data_audit_csv": csv_path,
        "data_audit_json": json_path,
    }
