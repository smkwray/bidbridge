"""Promotion artifacts for the BidBridge dealer-warehousing association."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

PROMOTION_COLUMNS = [
    "claim",
    "artifact",
    "estimate",
    "unit",
    "stat",
    "status",
    "interpretation",
]

_PLACEBO_TREATMENT_LEADS = {
    "lead_placebo_h1": 1,
    "shifted_placebo_h4": 4,
}

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _num(value: object) -> float | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def _fmt_billions(millions: float | None) -> str:
    if millions is None:
        return ""
    return f"{millions / 1000:.2f}"


def _lp_row(lp: pd.DataFrame, regime: str) -> dict[str, object]:
    if not {"regime", "horizon"}.issubset(lp.columns):
        match = pd.DataFrame()
    else:
        match = lp[(lp["regime"] == regime) & (lp["horizon"] == 0)]
    if match.empty:
        return {
            "claim": f"lp_same_week_{regime}",
            "artifact": "outputs/tables/lp_results.csv",
            "estimate": "",
            "unit": "$B dealer-position change for high-supply indicator",
            "stat": "",
            "status": "missing",
            "interpretation": "LP h=0 estimate unavailable; rerun `bidbridge lp` after refreshing inputs.",
        }
    row = match.iloc[0]
    beta = _num(row.get("beta"))
    t_stat = _num(row.get("t_stat"))
    p_value = _num(row.get("p_value"))
    return {
        "claim": f"lp_same_week_{regime}",
        "artifact": "outputs/tables/lp_results.csv",
        "estimate": _fmt_billions(beta),
        "unit": "$B dealer-position change for high-supply indicator",
        "stat": f"t={t_stat:.2f}, p={p_value:.4f}" if t_stat is not None and p_value is not None else "",
        "status": "supporting" if p_value is not None and p_value < 0.05 else "weak",
        "interpretation": (
            "At h=0, the coefficient on the binary high-supply indicator "
            "(announced aggregate supply above its expanding-window 75th "
            "percentile computed through the current week) measures same-week "
            f"dealer-position change in {regime.replace('_', ' ')}."
        ),
    }


def _fe_row(fe: pd.DataFrame, artifact: str, claim: str, status: str) -> dict[str, object]:
    if not {"specification", "variable"}.issubset(fe.columns):
        match = pd.DataFrame()
    else:
        match = fe[
            (
                fe["specification"]
                == "twoway_fe_supply_only_driscoll_kraay"
            )
            & (fe["variable"] == "Announced supply ($B)")
        ]
    if match.empty:
        return {
            "claim": claim,
            "artifact": artifact,
            "estimate": "",
            "unit": "$M dealer position per $B announced supply",
            "stat": "",
            "status": "missing",
            "interpretation": "Headline maturity-bucket FE estimate unavailable.",
        }
    row = match.iloc[0]
    coefficient = _num(row.get("coefficient"))
    se = _num(row.get("std_error"))
    lo = _num(row.get("ci_lower"))
    hi = _num(row.get("ci_upper"))
    return {
        "claim": claim,
        "artifact": artifact,
        "estimate": f"{coefficient:.2f}" if coefficient is not None else "",
        "unit": "$M dealer position per $B announced supply",
        "stat": f"se={se:.2f}, 95% CI [{lo:.2f}, {hi:.2f}]" if None not in (se, lo, hi) else "",
        "status": status if lo is not None and hi is not None and lo > 0 else "weak",
        "interpretation": (
            "Within-week, across-maturity offering amounts are associated with "
            "dealer-position changes."
        ),
    }


def _refund_row(refunding: pd.DataFrame) -> dict[str, object]:
    if "variable" not in refunding.columns:
        match = pd.DataFrame()
    else:
        match = refunding[refunding["variable"] == "Inventory change ($M)"]
    if match.empty:
        return {
            "claim": "refunding_inventory_accumulation",
            "artifact": "outputs/tables/regression_refunding_test.csv",
            "estimate": "",
            "unit": "$B refunding-minus-ordinary inventory change",
            "stat": "",
            "status": "missing",
            "interpretation": "Refunding comparison unavailable.",
        }
    row = match.iloc[0]
    difference = _num(row.get("difference"))
    t_stat = _num(row.get("t_stat"))
    p_value = _num(row.get("p_value"))
    return {
        "claim": "refunding_inventory_accumulation",
        "artifact": "outputs/tables/regression_refunding_test.csv",
        "estimate": _fmt_billions(difference),
        "unit": "$B refunding-minus-ordinary inventory change",
        "stat": f"t={t_stat:.2f}, p={p_value:.4f}" if t_stat is not None and p_value is not None else "",
        "status": "supporting" if p_value is not None and p_value < 0.05 else "weak",
        "interpretation": (
            "Quarterly refunding weeks are associated with concentrated dealer "
            "inventory accumulation."
        ),
    }


def _maturity_row(maturity: pd.DataFrame) -> dict[str, object]:
    if {"maturity_bucket", "dealer_share_mean"}.issubset(maturity.columns):
        shares = maturity.set_index("maturity_bucket")["dealer_share_mean"]
    else:
        shares = pd.Series(dtype=float)
    bills = _num(shares.get("bills"))
    long_coupon = _num(shares.get("long_coupon"))
    spread = (bills - long_coupon) * 100 if bills is not None and long_coupon is not None else None
    return {
        "claim": "maturity_gradient",
        "artifact": "outputs/tables/maturity_bucket_summary.csv",
        "estimate": f"{spread:.1f}" if spread is not None else "",
        "unit": "percentage-point bill-minus-long dealer share",
        "stat": f"bills={bills:.1%}, long={long_coupon:.1%}" if bills is not None and long_coupon is not None else "",
        "status": "supporting" if spread is not None and spread > 0 else "weak",
        "interpretation": "Dealer auction absorption is highest in bills and lowest in long coupons.",
    }


def _placebo_overlap_summary(placebos: pd.DataFrame) -> dict[str, object]:
    """Classify placebo cells by whether the cumulative outcome precedes treatment."""
    required = {"placebo_type", "horizon", "p_value"}
    missing = required - set(placebos.columns)
    if placebos.empty or missing:
        raise ValueError(
            "LP placebo promotion requires non-empty columns: "
            + ", ".join(sorted(required))
        )

    work = placebos[list(required)].copy()
    work["treatment_lead"] = work["placebo_type"].map(_PLACEBO_TREATMENT_LEADS)
    if work["treatment_lead"].isna().any():
        unknown = sorted(work.loc[work["treatment_lead"].isna(), "placebo_type"].unique())
        raise ValueError(f"Unknown LP placebo treatment lead(s): {unknown}")
    work["horizon"] = pd.to_numeric(work["horizon"], errors="raise")
    work["p_value"] = pd.to_numeric(work["p_value"], errors="raise")
    if work[["horizon", "p_value"]].isna().any().any():
        raise ValueError("LP placebo horizons and p-values must be complete.")

    # A cumulative outcome through t+h is a clean lead check only when it ends
    # before the future treatment at t+k.
    work["non_overlapping"] = work["horizon"] < work["treatment_lead"]
    work["significant"] = work["p_value"] < 0.05
    non_overlapping = work[work["non_overlapping"]]
    significant_non_overlapping = non_overlapping[non_overlapping["significant"]]
    labels = []
    for row in significant_non_overlapping.itertuples(index=False):
        lead = int(row.treatment_lead)
        horizon = int(row.horizon)
        labels.append(f"lead-{lead} h={horizon}")
    return {
        "total_cells": len(work),
        "total_significant": int(work["significant"].sum()),
        "non_overlapping_cells": len(non_overlapping),
        "non_overlapping_significant": int(non_overlapping["significant"].sum()),
        "overlapping_cells": int((~work["non_overlapping"]).sum()),
        "significant_non_overlapping_labels": labels,
    }


def _placebo_row(placebos: pd.DataFrame) -> dict[str, object]:
    diagnostic = _placebo_overlap_summary(placebos)
    non_overlapping_hits = diagnostic["non_overlapping_significant"]
    non_overlapping_cells = diagnostic["non_overlapping_cells"]
    total_hits = diagnostic["total_significant"]
    total_cells = diagnostic["total_cells"]
    overlapping_cells = diagnostic["overlapping_cells"]
    labels = "; ".join(diagnostic["significant_non_overlapping_labels"])
    labels_clause = f" ({labels})" if labels else ""
    return {
        "claim": "lp_placebo_boundary",
        "artifact": "outputs/tables/lp_placebo_results.csv",
        "estimate": str(non_overlapping_hits),
        "unit": "significant non-overlapping lead/anticipation cells at 5%",
        "stat": (
            f"{non_overlapping_hits} of {non_overlapping_cells} non-overlapping; "
            f"{total_hits} of {total_cells} total; {overlapping_cells} overlapping"
        ),
        "status": "boundary",
        "interpretation": (
            "The LP does not provide clean timing validation: "
            f"{non_overlapping_hits} of {non_overlapping_cells} non-overlapping "
            f"lead/anticipation cells are significant{labels_clause}; {total_hits} of "
            f"{total_cells} full diagnostic cells are significant, and "
            f"{overlapping_cells} of {total_cells} overlap the shifted treatment "
            "with the cumulative outcome. Retain the h=0 binary high-supply estimate "
            "only as a descriptive supporting association, not as timing validation "
            "or an identifying result."
        ),
    }


def build_promotion_summary(tables_dir: Path) -> pd.DataFrame:
    """Build the compact claim ledger used by the promotion memo."""
    tables_dir = Path(tables_dir)
    lp = _read_csv(tables_dir / "lp_results.csv")
    placebos = _read_csv(tables_dir / "lp_placebo_results.csv")
    fe = _read_csv(tables_dir / "panel_fe_results.csv")
    fe_thursday = _read_csv(tables_dir / "panel_fe_results_thursday.csv")
    refunding = _read_csv(tables_dir / "regression_refunding_test.csv")
    maturity = _read_csv(tables_dir / "maturity_bucket_summary.csv")

    rows = [
        _lp_row(lp, "full_sample"),
        _lp_row(lp, "qt_period"),
        _lp_row(lp, "non_qt_period"),
        _fe_row(fe, "outputs/tables/panel_fe_results.csv", "maturity_bucket_fe_monday", "headline"),
        _fe_row(
            fe_thursday,
            "outputs/tables/panel_fe_results_thursday.csv",
            "maturity_bucket_fe_thursday",
            "robustness",
        ),
        _refund_row(refunding),
        _maturity_row(maturity),
        _placebo_row(placebos),
    ]
    return pd.DataFrame(rows, columns=PROMOTION_COLUMNS)


def write_promotion_summary(tables_dir: Path, output_path: Path) -> Path:
    summary = build_promotion_summary(tables_dir)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output_path, index=False, lineterminator="\n")
    return output_path


def write_promotion_memo(summary: pd.DataFrame, output_path: Path) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    def row(claim: str) -> pd.Series:
        match = summary[summary["claim"] == claim]
        return match.iloc[0] if not match.empty else pd.Series(dtype=object)

    lp_full = row("lp_same_week_full_sample")
    fe_monday = row("maturity_bucket_fe_monday")
    fe_thursday = row("maturity_bucket_fe_thursday")
    refunding = row("refunding_inventory_accumulation")
    maturity = row("maturity_gradient")
    placebo = row("lp_placebo_boundary")

    memo = f"""# BidBridge Promotion Memo

## Recommended use
Promote `bidbridge` only as evidence about the short-run aggregate primary-dealer warehousing association. The treatment is the offering amount announced for auctions occurring in a maturity-bucket week; the outcome is the contemporaneous change in aggregate primary-dealer Treasury positions.

## Evidence to carry forward
- Same-week LP support: at h=0, the coefficient on the binary high-supply indicator—announced aggregate supply above its expanding-window 75th percentile computed through the current week—is {lp_full.get('estimate', '')} {lp_full.get('unit', '')} ({lp_full.get('stat', '')}). This is not a continuous offering-amount slope.
- Maturity FE: Monday-start two-way FE estimates {fe_monday.get('estimate', '')} {fe_monday.get('unit', '')}; Thursday-start robustness estimates {fe_thursday.get('estimate', '')}.
- Refunding concentration: refunding weeks show {refunding.get('estimate', '')} {refunding.get('unit', '')} ({refunding.get('stat', '')}).
- Maturity gradient: bills exceed long coupons by {maturity.get('estimate', '')} {maturity.get('unit', '')} ({maturity.get('stat', '')}).

## Boundary
Do not claim terminal ownership, final-holder incidence, customer identity, bank preference, deposit destruction, settlement incidence, structural dealer-capacity elasticities, or a structural causal policy mechanism. {placebo.get('interpretation', '')} LP, refunding, maturity-gradient, persistence, Thursday-timing, and RateWall surfaces remain supporting evidence only.

## Thesis wording
In a maturity-bucket-by-week panel, larger Treasury offering amounts announced for auctions occurring in a given bucket-week are associated with larger contemporaneous changes in aggregate primary-dealer Treasury positions, after maturity-bucket and week fixed effects; inference uses Driscoll-Kraay standard errors. This is evidence of short-run aggregate dealer warehousing only.
"""
    output_path.write_text(memo, encoding="utf-8", newline="\n")
    return output_path


def write_promotion_artifacts(tables_dir: Path, summary_path: Path, memo_path: Path) -> dict[str, Path]:
    summary = build_promotion_summary(tables_dir)
    summary_path = Path(summary_path)
    memo_path = Path(memo_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    memo_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_path, index=False, lineterminator="\n")
    write_promotion_memo(summary, memo_path)
    return {"promotion_summary": summary_path, "promotion_memo": memo_path}
