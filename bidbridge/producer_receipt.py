"""Issue the commit-bound OPEN-07 BidBridge producer receipt.

The receipt path is deliberately specialized: it verifies the frozen dealer-
warehousing evidence chain without introducing a general artifact framework.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from bidbridge.analysis.panel_fe import generate_panel_fe_table, run_bucket_fe_regression
from bidbridge.analysis.promotion import write_promotion_artifacts
from bidbridge.analysis.ratewall_contracts import (
    CONTRACT_VERSION,
    write_ratewall_dealer_warehousing_support_contract,
)
from bidbridge.cli import _study_week_definition
from bidbridge.data.pipeline import (
    _harmonize_auctions,
    _harmonize_investor_class,
    resolve_data_path,
)
from bidbridge.data.sources.nyfed_pd import finalize_primary_dealer_dataframe
from bidbridge.features.maturity_panel import build_maturity_panel

RECEIPT_VERSION = "bidbridge_open07_producer_receipt_v1"
EXPECTED_PROMOTION_SHA256 = (
    "1a1e539a598c0ab4f9ba0cc0132272cc293bc12ffb1b1dbc1517ca98d3ab40b8"
)

ESTIMATOR_INPUTS = (
    "data/raw/treasury/treasury_auctions.csv",
    "data/raw/treasury/investor_class_allotments.csv",
    "data/raw/nyfed/primary_dealer_stats.csv",
    "configs/study.yml",
)
SOURCE_MANIFESTS = (
    "data/raw/treasury/treasury_auctions_manifest.json",
    "data/raw/treasury/investor_class_allotments_manifest.json",
    "data/raw/nyfed/primary_dealer_stats_manifest.json",
)
PROMOTION_INPUTS = (
    "data/processed/auction_week_panel.csv",
    "outputs/tables/lp_results.csv",
    "outputs/tables/lp_placebo_results.csv",
    "outputs/tables/panel_fe_results.csv",
    "outputs/tables/panel_fe_results_thursday.csv",
    "outputs/tables/regression_refunding_test.csv",
    "outputs/tables/maturity_bucket_summary.csv",
    "outputs/tables/persistence_summary.csv",
)
PROMOTED_OUTPUTS = (
    "outputs/tables/bidbridge_promotion_summary.csv",
    "outputs/bidbridge_promotion_memo.md",
    "outputs/tables/panel_fe_results.csv",
    "outputs/tables/panel_fe_results_thursday.csv",
)
RATEWALL_OUTPUT = "outputs/tables/bidbridge_ratewall_dealer_warehousing_support.csv"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_evidence(root: Path, relative_path: str, *, resolve_pointer: bool) -> dict[str, Any]:
    path = root / relative_path
    if not path.is_file():
        raise FileNotFoundError(f"Required receipt file is missing: {relative_path}")

    resolved = resolve_data_path(path) if resolve_pointer else path
    if resolve_pointer and resolved == path and path.stat().st_size <= 4096:
        try:
            pointer_text = path.read_text(encoding="utf-8").strip()
        except UnicodeDecodeError:
            pointer_text = ""
        looks_like_pointer = bool(
            pointer_text
            and "\n" not in pointer_text
            and "," not in pointer_text
            and ("/" in pointer_text or "\\" in pointer_text)
        )
        if looks_like_pointer and not Path(pointer_text).expanduser().is_file():
            raise FileNotFoundError(
                f"Required receipt content for {relative_path} is unavailable: "
                f"{pointer_text}"
            )
    if not resolved.is_file():
        raise FileNotFoundError(
            f"Required receipt content for {relative_path} is unavailable: {resolved}"
        )

    evidence: dict[str, Any] = {
        "path": relative_path,
        "bytes": resolved.stat().st_size,
        "sha256": _sha256(resolved),
    }
    if resolved != path:
        evidence["storage"] = "one_line_pointer"
        evidence["pointer_bytes"] = path.stat().st_size
        evidence["pointer_sha256"] = _sha256(path)
    else:
        evidence["storage"] = "project_file"
    return evidence


def _git_commit(root: Path, explicit_commit: str | None) -> tuple[str, str]:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        if not explicit_commit:
            raise RuntimeError(
                "Git metadata is unavailable; --producer-commit is required for a "
                "reconciled gitless runner checkout."
            )
        return explicit_commit, "explicit_commit_in_gitless_reconciled_checkout"

    head = result.stdout.strip()
    if explicit_commit and explicit_commit != head:
        raise RuntimeError(
            f"Explicit producer commit {explicit_commit} does not match local HEAD {head}."
        )
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if dirty:
        raise RuntimeError("Producer tree must be clean before receipt issuance:\n" + dirty)
    return head, "local_git_head_and_clean_tree"


def _compare_hash(actual: Path, retained: Path, label: str) -> str:
    actual_hash = _sha256(actual)
    retained_hash = _sha256(retained)
    if actual_hash != retained_hash:
        raise RuntimeError(
            f"{label} reproduction mismatch: generated {actual_hash}, retained {retained_hash}."
        )
    return actual_hash


def _reproduce_panel_fe(root: Path, temp_dir: Path) -> dict[str, dict[str, Any]]:
    auctions = _harmonize_auctions(root / "data/raw/treasury/treasury_auctions.csv")
    investor = _harmonize_investor_class(
        root / "data/raw/treasury/investor_class_allotments.csv"
    )
    dealer_path = resolve_data_path(root / "data/raw/nyfed/primary_dealer_stats.csv")
    dealer_raw = pd.read_csv(
        dealer_path,
        parse_dates=["as_of_date", "week_start", "week_end"],
    )

    samples: dict[str, dict[str, Any]] = {}
    for label, week_definition, retained_name in (
        ("headline_monday", _study_week_definition(), "panel_fe_results.csv"),
        ("robustness_thursday", "thursday", "panel_fe_results_thursday.csv"),
    ):
        maturity = build_maturity_panel(
            auctions,
            investor,
            week_definition=week_definition,
        )
        dealer = finalize_primary_dealer_dataframe(
            dealer_raw,
            week_definition=week_definition,
        )
        from bidbridge.analysis.panel_fe import build_bucket_outcomes

        outcomes = build_bucket_outcomes(
            maturity,
            dealer,
            headline_strict=True,
            week_definition=week_definition,
        )
        results = run_bucket_fe_regression(outcomes)
        reproduced = generate_panel_fe_table(
            results,
            temp_dir,
            file_name=retained_name,
        )
        retained = root / "outputs/tables" / retained_name
        output_hash = _compare_hash(reproduced, retained, label)
        info = dict(results["_sample_info"])
        info.update(
            {
                "frequency": "weekly",
                "panel_unit": "maturity_bucket_by_week",
                "dependent_variable": "change in aggregate primary-dealer Treasury position",
                "dependent_variable_unit": "USD millions",
                "treatment": "announced Treasury supply in the maturity bucket-week",
                "treatment_unit": "USD billions",
                "coefficient_unit": (
                    "USD millions dealer-position change per USD billion announced supply"
                ),
                "fixed_effects": ["maturity_bucket", "week"],
                "headline_covariance": "Driscoll-Kraay",
                "retained_output": f"outputs/tables/{retained_name}",
                "retained_output_sha256": output_hash,
            }
        )
        samples[label] = info
    return samples


def _reproduce_exports(root: Path, temp_dir: Path) -> dict[str, str]:
    temp_summary = temp_dir / "bidbridge_promotion_summary.csv"
    temp_memo = temp_dir / "bidbridge_promotion_memo.md"
    write_promotion_artifacts(root / "outputs/tables", temp_summary, temp_memo)

    hashes = {
        "promotion_summary": _compare_hash(
            temp_summary,
            root / "outputs/tables/bidbridge_promotion_summary.csv",
            "promotion summary",
        ),
        "promotion_memo": _compare_hash(
            temp_memo,
            root / "outputs/bidbridge_promotion_memo.md",
            "promotion memo",
        ),
    }
    if hashes["promotion_summary"] != EXPECTED_PROMOTION_SHA256:
        raise RuntimeError(
            "Promotion summary does not match the thesis-retained hash: "
            f"{hashes['promotion_summary']} != {EXPECTED_PROMOTION_SHA256}."
        )

    temp_ratewall = temp_dir / "bidbridge_ratewall_dealer_warehousing_support.csv"
    write_ratewall_dealer_warehousing_support_contract(
        panel_path=root / "data/processed/auction_week_panel.csv",
        persistence_summary_path=root / "outputs/tables/persistence_summary.csv",
        output_path=temp_ratewall,
    )
    hashes["ratewall_support"] = _compare_hash(
        temp_ratewall,
        root / RATEWALL_OUTPUT,
        "RateWall support export",
    )
    return hashes


def build_receipt(root: Path, producer_commit: str | None = None) -> dict[str, Any]:
    root = root.resolve()
    commit, commit_verification = _git_commit(root, producer_commit)

    estimator_inputs = [
        _file_evidence(root, path, resolve_pointer=True) for path in ESTIMATOR_INPUTS
    ]
    source_manifests = [
        _file_evidence(root, path, resolve_pointer=True) for path in SOURCE_MANIFESTS
    ]
    promotion_inputs = [
        _file_evidence(root, path, resolve_pointer=True) for path in PROMOTION_INPUTS
    ]
    promoted_outputs = [
        _file_evidence(root, path, resolve_pointer=False) for path in PROMOTED_OUTPUTS
    ]
    ratewall_output = _file_evidence(root, RATEWALL_OUTPUT, resolve_pointer=False)

    with tempfile.TemporaryDirectory(prefix="bidbridge-open07-") as temp:
        temp_dir = Path(temp)
        samples = _reproduce_panel_fe(root, temp_dir)
        export_hashes = _reproduce_exports(root, temp_dir)

    summary = pd.read_csv(root / "outputs/tables/bidbridge_promotion_summary.csv")
    return {
        "receipt_version": RECEIPT_VERSION,
        "producer_project": "bidbridge",
        "repository": "https://github.com/smkwray/bidbridge",
        "producer_commit": commit,
        "producer_commit_verification": commit_verification,
        "estimator": {
            "headline": "maturity-bucket two-way panel fixed effects",
            "entry_point": "bidbridge panel-fe",
            "implementation": "bidbridge/analysis/panel_fe.py",
            "supporting_surfaces": [
                "weekly local projections with ex ante high-supply shocks",
                "refunding-week inventory comparison",
                "dealer-allotment maturity gradient",
                "Thursday-start week-alignment robustness",
            ],
        },
        "input_files": estimator_inputs,
        "source_manifests": source_manifests,
        "promotion_input_files": promotion_inputs,
        "estimation_samples": samples,
        "promoted_outputs": promoted_outputs,
        "promoted_claim_rows": summary.to_dict(orient="records"),
        "retained_exporter": {
            "implementation": "bidbridge/analysis/ratewall_contracts.py",
            "contract_version": CONTRACT_VERSION,
            "command": "python -m bidbridge ratewall-support",
            "output": ratewall_output,
        },
        "exact_reproduction": {
            "commands_against_hash_bound_frozen_inputs": [
                "python -m bidbridge analyze",
                "python -m bidbridge lp",
                "python -m bidbridge panel-fe",
                "python -m bidbridge promotion",
                "python -m bidbridge ratewall-support",
            ],
            "receipt_command": (
                "python -m bidbridge.producer_receipt --producer-commit "
                f"{commit} --output receipts/open07_bidbridge_producer_receipt.json"
            ),
            "verified_reproduction_hashes": export_hashes,
            "network_fetch_included": False,
        },
        "environment": {
            name: importlib.metadata.version(name)
            for name in ("pandas", "statsmodels", "linearmodels", "numpy")
        },
        "superseded": True,
        "promotable": False,
        "claim_boundary": {
            "admissible": (
                "short-run aggregate primary-dealer warehousing and inventory response "
                "to announced Treasury supply pressure"
            ),
            "not_identified": [
                "terminal ownership",
                "final-holder incidence",
                "customer identity or bilateral trade",
                "bank preference",
                "deposit destruction or settlement incidence",
                "structural dealer-capacity elasticity",
                "structural causal policy mechanism",
            ],
            "lp_status": (
                "failed clean timing validation; descriptive support only"
            ),
        },
        "acceptance": {
            "promotion_summary_matches_thesis_hash": True,
            "headline_and_thursday_outputs_exactly_reproduced": True,
            "promotion_and_ratewall_exports_exactly_reproduced": True,
            "required_inputs_present_and_hashed": True,
        },
    }


def write_receipt(
    output_path: Path,
    *,
    root: Path,
    producer_commit: str | None = None,
) -> Path:
    raise ValueError("Superseded by OPEN-07 v5; v1 receipt issuance is disabled")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    parser.error("Superseded by OPEN-07 v5; v1 receipt issuance is disabled and non-promotable.")


if __name__ == "__main__":
    raise SystemExit(main())
