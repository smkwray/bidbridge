"""Issue the corrected reconstructed-input OPEN-07 producer receipt (v5)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from bidbridge.open07_reconstruction import (
    RECONSTRUCTED_INPUT,
    RECONSTRUCTION_MANIFEST,
    sha256,
)
from bidbridge.open07_reproduce import DEFAULT_EVIDENCE
from bidbridge.producer_receipt import (
    EXPECTED_PROMOTION_SHA256,
    _file_evidence,
    _git_commit,
)

RECEIPT_VERSION = "bidbridge_open07_reconstructed_input_receipt_v5"
ROOT = Path(__file__).resolve().parents[1]
EXPECTED_RECONSTRUCTED_INPUT_SHA256 = (
    "d73a723984db7cc4bfef7363e9a8f63a1a932404c91833fb2b6c33e3b3a9f681"
)
DEFAULT_REMRUN_EVIDENCE = Path("receipts/open07_v5_remrun_evidence.json")
DEFAULT_RECEIPT = Path("receipts/open07_bidbridge_reconstructed_input_receipt_v5.json")
REJECTED_V2_RECEIPT_SHA256 = (
    "87307c7f37cf919a8069f7df88df8dad6c542be3147f15badedc9554467cc867"
)
REJECTED_V2_PROMOTION_SHA256 = (
    "8963fedf353114d2983fd6e63ac8e36a646249be0209c52b73eb87b82110c17f"
)
REJECTED_V3_RECEIPT_SHA256 = (
    "0489e83daf6d29458b729c919eb9a3fe6520688b9e2749299036d94aaaa4375f"
)
REJECTED_V3_PROMOTION_SHA256 = (
    "e57c3480b5779ee9ed7fee993b30b4d838155ded5d04421616334439efd2f0ec"
)
REJECTED_V4_RECEIPT_SHA256 = (
    "0693bd738b2a860fa8b19ab4c225b0741d71d9c152ee8639758e9ad1366114a0"
)
REJECTED_V4_PROMOTION_SHA256 = (
    "d0158d8c948b381b2ab952f2128b24677f6033c995d68583eea67c268a9ae810"
)

OTHER_INPUTS = (
    "data/raw/treasury/investor_class_allotments.csv",
    "data/raw/treasury/investor_class_allotments_manifest.json",
    "data/raw/nyfed/primary_dealer_stats.csv",
    "data/raw/nyfed/primary_dealer_stats_manifest.json",
    "data/raw/nyfed/soma_holdings.csv",
    "data/raw/nyfed/soma_holdings_manifest.json",
    "configs/study.yml",
)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Required receipt evidence is missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"Receipt evidence must be a JSON object: {path}")
    return payload


def _validate_versioned_artifacts(
    root: Path,
    reproduction: dict[str, Any],
) -> list[dict[str, Any]]:
    verified: list[dict[str, Any]] = []
    for declared in reproduction.get("versioned_artifacts", []):
        path = root / declared["path"]
        if not path.is_file():
            raise FileNotFoundError(f"Versioned OPEN-07 artifact is missing: {path}")
        actual = sha256(path)
        if actual != declared["sha256"]:
            raise RuntimeError(
                f"Versioned OPEN-07 artifact hash mismatch for {declared['path']}: "
                f"{actual} != {declared['sha256']}"
            )
        verified.append(
            {
                "path": declared["path"],
                "bytes": path.stat().st_size,
                "sha256": actual,
                "generated_from": declared["generated_from"],
            }
        )
    if not verified:
        raise ValueError("Reproduction evidence declares no versioned artifacts.")
    return verified


def build_receipt(
    *,
    root: Path,
    reproduction_evidence_path: Path,
    remrun_evidence_path: Path,
    producer_commit: str | None = None,
) -> dict[str, Any]:
    root = root.resolve()
    commit, commit_verification = _git_commit(root, producer_commit)
    reconstruction = _load_json(root / RECONSTRUCTION_MANIFEST.relative_to(root))
    reproduction = _load_json(reproduction_evidence_path)
    remrun = _load_json(remrun_evidence_path)

    if reconstruction.get("replacement_normalized", {}).get("sha256") != (
        EXPECTED_RECONSTRUCTED_INPUT_SHA256
    ):
        raise RuntimeError("Unexpected reconstructed Treasury input hash.")
    if sha256(RECONSTRUCTED_INPUT) != EXPECTED_RECONSTRUCTED_INPUT_SHA256:
        raise RuntimeError("Reconstructed Treasury input bytes do not match its frozen hash.")
    if reproduction.get("producer_commit") != commit:
        raise RuntimeError(
            "Reproduction evidence commit does not match the receipt producer commit."
        )
    if reproduction.get("promotion_mode") is not False:
        raise RuntimeError(
            "Final receipt requires a verification reproduction run, not the promotion run."
        )
    acceptance = reproduction.get("acceptance", {})
    required_acceptance = (
        "raw_sources_equivalent_over_frozen_domain",
        "raw_to_normalized_exactly_reproduced",
        "reconstruction_manifest_exactly_reproduced",
        "normalized_to_outputs_exactly_reproduced",
        "all_versioned_artifacts_exactly_matched",
    )
    if not all(acceptance.get(key) is True for key in required_acceptance):
        raise RuntimeError("Reproduction evidence did not pass every required exact gate.")
    if acceptance.get("original_source_byte_identity_claimed") is not False:
        raise RuntimeError("Receipt must not claim original-source byte identity.")
    if remrun.get("command_exit_code") != 0 or remrun.get("completion_state") != "complete":
        raise RuntimeError("Remrun evidence does not prove a completed successful command.")
    if remrun.get("producer_commit") != commit:
        raise RuntimeError("Remrun evidence is not bound to the producer commit.")

    versioned_artifacts = _validate_versioned_artifacts(root, reproduction)
    artifacts_by_name = {Path(item["path"]).name: item for item in versioned_artifacts}
    required_promoted = (
        "bidbridge_promotion_summary.csv",
        "bidbridge_promotion_memo.md",
        "panel_fe_results.csv",
        "panel_fe_results_thursday.csv",
        "bidbridge_ratewall_dealer_warehousing_support.csv",
    )
    missing_promoted = [name for name in required_promoted if name not in artifacts_by_name]
    if missing_promoted:
        raise ValueError(f"Missing promoted v5 artifacts: {missing_promoted}")

    historical_summary = root / "outputs/tables/bidbridge_promotion_summary.csv"
    if sha256(historical_summary) != EXPECTED_PROMOTION_SHA256:
        raise RuntimeError("Historical thesis promotion hash no longer matches its frozen value.")
    v5_summary = root / artifacts_by_name["bidbridge_promotion_summary.csv"]["path"]
    promoted_rows = pd.read_csv(v5_summary).to_dict(orient="records")

    def _headline_row(name: str) -> pd.Series:
        path = root / artifacts_by_name[name]["path"]
        table = pd.read_csv(path)
        match = table[
            (table["specification"] == "twoway_fe_supply_only_driscoll_kraay")
            & (table["variable"] == "Announced supply ($B)")
        ]
        if len(match) != 1:
            raise RuntimeError(f"Missing unique corrected supply-only headline in {name}.")
        return match.iloc[0]

    monday = _headline_row("panel_fe_results.csv")
    thursday = _headline_row("panel_fe_results_thursday.csv")
    monday_positive_significant = bool(
        float(monday["coefficient"]) > 0 and float(monday["ci_lower"]) > 0
    )
    thursday_not_sign_reversing = bool(float(thursday["coefficient"]) >= 0)
    if not monday_positive_significant or not thursday_not_sign_reversing:
        raise RuntimeError(
            "Corrected headline failed the presealed economics gate: Monday must be "
            "positive with a 95% Driscoll-Kraay interval above zero and Thursday "
            "must not reverse sign."
        )
    for sample in reproduction["estimation_samples"].values():
        if sample.get("headline_specification") != "supply_only_two_way_fixed_effects":
            raise RuntimeError("Receipt sample is not bound to the supply-only headline.")
        strict = sample.get("strict_lag_robustness", {})
        if strict.get("current_share_substitutions") != 0:
            raise RuntimeError("Strict-lag robustness contains current-share substitutions.")
        if not sample.get("driscoll_kraay_kernel") or sample.get(
            "driscoll_kraay_bandwidth"
        ) is None:
            raise RuntimeError("Driscoll-Kraay kernel and bandwidth must be explicit.")

    memo_path = root / artifacts_by_name["bidbridge_promotion_memo.md"]["path"]
    memo_text = memo_path.read_text(encoding="utf-8").lower()
    forbidden_memo_claims = (
        "bridge between auction allotments and final holders",
        "distributed to longer-run holders",
        "before the supply is distributed",
        "explains the short-run dealer warehousing bridge",
    )
    if any(phrase in memo_text for phrase in forbidden_memo_claims):
        raise RuntimeError("Promotion memo crosses the settled final-holder boundary.")
    if "qt amplification" in memo_text:
        raise RuntimeError("Promotion memo claims unsupported QT amplification.")
    if "binary high-supply indicator" not in memo_text:
        raise RuntimeError("Promotion memo does not identify the LP binary treatment.")
    if "timing-consistent" in memo_text:
        raise RuntimeError("Promotion memo makes an unsupported LP timing-validation claim.")
    if "does not provide clean timing validation" not in memo_text:
        raise RuntimeError("Promotion memo omits the failed clean LP timing checks.")
    if "2 of 5 non-overlapping lead/anticipation cells are significant" not in memo_text:
        raise RuntimeError("Promotion memo omits the overlap-aware LP placebo count.")
    lp_rows = [
        row for row in promoted_rows if str(row.get("claim", "")).startswith("lp_same_week_")
    ]
    if not lp_rows or not all(
        "high-supply indicator" in str(row.get("interpretation", "")).lower()
        for row in lp_rows
    ):
        raise RuntimeError("Promoted LP rows do not identify the binary treatment.")
    placebo_rows = [
        row for row in promoted_rows if row.get("claim") == "lp_placebo_boundary"
    ]
    if len(placebo_rows) != 1:
        raise RuntimeError("Promoted summary lacks a unique LP placebo boundary row.")
    placebo_row = placebo_rows[0]
    if (
        str(placebo_row.get("estimate")) not in {"2", "2.0"}
        or "2 of 5 non-overlapping" not in str(placebo_row.get("stat", "")).lower()
        or "does not provide clean timing validation"
        not in str(placebo_row.get("interpretation", "")).lower()
    ):
        raise RuntimeError("Promoted LP placebo row is not overlap-aware.")

    reconstruction_inputs = [
        _file_evidence(root, item["path"], resolve_pointer=False)
        for item in (
            reconstruction["primary_source"],
            reconstruction["primary_query_metadata"],
            reconstruction["corroborating_source"],
        )
    ]
    reconstruction_inputs.append(
        _file_evidence(
            root,
            RECONSTRUCTED_INPUT.relative_to(root).as_posix(),
            resolve_pointer=False,
        )
    )
    other_inputs = [
        _file_evidence(root, path, resolve_pointer=True) for path in OTHER_INPUTS
    ]
    exact_gate_passed = all(
        acceptance.get(key) is True for key in required_acceptance
    )
    samples_bound = all(
        sample.get("estimation_rows_sha256")
        and sample.get("sample_start")
        and sample.get("sample_end")
        and sample.get("coefficient_unit")
        for sample in reproduction["estimation_samples"].values()
    )
    outputs_bound = len(versioned_artifacts) == len(
        reproduction["versioned_artifacts"]
    ) and all(item["sha256"] for item in versioned_artifacts)
    replacement_inputs_bound = all(item.get("sha256") for item in reconstruction_inputs)

    return {
        "receipt_version": RECEIPT_VERSION,
        "producer_project": "bidbridge",
        "repository": "https://github.com/smkwray/bidbridge",
        "producer_commit": commit,
        "producer_commit_verification": commit_verification,
        "input_lineage_status": "reconstructed_replacement",
        "producer_input_identity_changed": True,
        "original_normalized_input_recovered": False,
        "original_normalized_input_sha256": None,
        "original_manifest_sha256": None,
        "original_source_byte_identity_proven": False,
        "network_refetch_used": False,
        "equivalence_scope_end": reconstruction["equivalence_scope_end"],
        "replacement_source_inputs": reconstruction_inputs,
        "other_estimator_inputs": other_inputs,
        "optional_source_exclusions": reproduction["optional_source_exclusions"],
        "reconstruction_manifest": _file_evidence(
            root,
            RECONSTRUCTION_MANIFEST.relative_to(root).as_posix(),
            resolve_pointer=False,
        ),
        "reproduction_evidence": {
            "path": reproduction_evidence_path.relative_to(root).as_posix(),
            "bytes": reproduction_evidence_path.stat().st_size,
            "sha256": sha256(reproduction_evidence_path),
        },
        "remrun_evidence": {
            "path": remrun_evidence_path.relative_to(root).as_posix(),
            "bytes": remrun_evidence_path.stat().st_size,
            "sha256": sha256(remrun_evidence_path),
            "run_id": remrun.get("run_id"),
        },
        "estimator": {
            "headline": "supply-only maturity-bucket two-way panel fixed effects",
            "implementation": "bidbridge/analysis/panel_fe.py",
            "headline_covariance": "Driscoll-Kraay",
            "strict_lag_robustness": (
                "strict previous-calendar-week dealer share; zero current-share "
                "substitutions"
            ),
            "supporting_surfaces": [
                "weekly local projections with ex ante high-supply shocks",
                "refunding-week inventory comparison",
                "dealer-allotment maturity gradient",
                "Thursday-start week-alignment robustness",
            ],
        },
        "estimation_samples": reproduction["estimation_samples"],
        "versioned_chain_artifacts": versioned_artifacts,
        "promoted_outputs": [artifacts_by_name[name] for name in required_promoted],
        "promoted_claim_rows": promoted_rows,
        "historical_thesis_artifact": {
            "path": "outputs/tables/bidbridge_promotion_summary.csv",
            "sha256": EXPECTED_PROMOTION_SHA256,
            "status": "verified_historical_legacy_v1",
            "reason": (
                "The historical summary consumed a March 16 maturity table with no "
                "committed exact producer; the corrected v5 chain regenerates and "
                "binds that support surface."
            ),
        },
        "rejected_v2_audit_object": {
            "receipt_path": (
                "receipts/open07_bidbridge_reconstructed_input_receipt_v2.json"
            ),
            "receipt_sha256": REJECTED_V2_RECEIPT_SHA256,
            "promotion_summary_path": (
                "outputs/open07_v2/tables/bidbridge_promotion_summary.csv"
            ),
            "promotion_summary_sha256": REJECTED_V2_PROMOTION_SHA256,
            "status": "rejected_by_final_economics_audit_and_superseded",
        },
        "rejected_v3_audit_object": {
            "receipt_path": (
                "receipts/open07_bidbridge_reconstructed_input_receipt_v3.json"
            ),
            "receipt_sha256": REJECTED_V3_RECEIPT_SHA256,
            "promotion_summary_path": (
                "outputs/open07_v3/tables/bidbridge_promotion_summary.csv"
            ),
            "promotion_summary_sha256": REJECTED_V3_PROMOTION_SHA256,
            "status": "rejected_for_lp_promotion_wording_and_superseded_by_v4",
        },
        "rejected_v4_audit_object": {
            "receipt_path": (
                "receipts/open07_bidbridge_reconstructed_input_receipt_v4.json"
            ),
            "receipt_sha256": REJECTED_V4_RECEIPT_SHA256,
            "promotion_summary_path": (
                "outputs/open07_v4/tables/bidbridge_promotion_summary.csv"
            ),
            "promotion_summary_sha256": REJECTED_V4_PROMOTION_SHA256,
            "status": "rejected_for_unsupported_lp_timing_validation_and_superseded_by_v5",
        },
        "exact_reproduction": {
            "reconstruction_command": "python -B -m bidbridge.open07_reconstruction",
            "promotion_command": (
                "python -B -m bidbridge.open07_reproduce --producer-commit "
                f"{commit} --promote"
            ),
            "verification_command": (
                "python -B -m bidbridge.open07_reproduce --producer-commit "
                f"{commit}"
            ),
            "receipt_command": (
                "python -B -m bidbridge.open07_reconstructed_receipt "
                f"--producer-commit {commit}"
            ),
        },
        "environment": reproduction["environment"],
        "claim_boundary": {
            "admissible": (
                "short-run aggregate primary-dealer warehousing association with "
                "offering amounts announced for auctions occurring in the bucket-week"
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
        },
        "lineage_disclosure": (
            "This receipt binds the exact bytes and SHA-256 hashes of the replacement "
            "FiscalData extracts and the versioned reconstructed BidBridge normalized "
            "input used for OPEN-07. The original BidBridge normalized Treasury-auctions "
            "CSV and source manifest were not recovered; their bytes, SHA-256 hashes, and "
            "acquisition timestamp are not attested. Equivalence is limited to the "
            "declared frozen estimation domain and the enumerated samples and outputs. "
            "Producer input identity changed. The v5 estimator retains the corrected "
            "supply-only primary specification and identifies the supporting LP treatment "
            "as a binary high-supply indicator; the promoted claim and identification "
            "boundaries remain limited to aggregate short-run dealer warehousing."
        ),
        "acceptance": {
            "replacement_inputs_present_and_hashed": replacement_inputs_bound,
            "complete_chain_exactly_reproduced": exact_gate_passed,
            "samples_dates_units_and_row_digests_bound": samples_bound,
            "promoted_outputs_present_and_hashed": outputs_bound,
            "verified_clean_local_receipt_issuer": (
                commit_verification == "local_git_head_and_clean_tree"
            ),
            "original_source_byte_identity_not_claimed": (
                acceptance.get("original_source_byte_identity_claimed") is False
            ),
            "corrected_monday_positive_with_ci_above_zero": (
                monday_positive_significant
            ),
            "corrected_thursday_not_sign_reversing": thursday_not_sign_reversing,
            "owner_final_hash_promotion_recorded": False,
        },
    }


def write_receipt(
    output_path: Path,
    *,
    root: Path,
    reproduction_evidence_path: Path,
    remrun_evidence_path: Path,
    producer_commit: str | None = None,
) -> Path:
    payload = build_receipt(
        root=root,
        reproduction_evidence_path=reproduction_evidence_path,
        remrun_evidence_path=remrun_evidence_path,
        producer_commit=producer_commit,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return output_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--producer-commit")
    parser.add_argument("--reproduction-evidence", type=Path, default=DEFAULT_EVIDENCE)
    parser.add_argument("--remrun-evidence", type=Path, default=DEFAULT_REMRUN_EVIDENCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_RECEIPT)
    args = parser.parse_args(argv)
    reproduction = (
        args.reproduction_evidence
        if args.reproduction_evidence.is_absolute()
        else ROOT / args.reproduction_evidence
    )
    remrun = (
        args.remrun_evidence
        if args.remrun_evidence.is_absolute()
        else ROOT / args.remrun_evidence
    )
    output = args.output if args.output.is_absolute() else ROOT / args.output
    write_receipt(
        output,
        root=ROOT,
        reproduction_evidence_path=reproduction,
        remrun_evidence_path=remrun,
        producer_commit=args.producer_commit,
    )
    print(f"producer_receipt={output}")
    print(f"sha256={sha256(output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
