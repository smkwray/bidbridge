"""Reproduce and retain the complete reconstructed-input OPEN-07 evidence chain."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from bidbridge.analysis.local_projections import (
    generate_lp_placebo_table,
    generate_lp_table,
    run_local_projection_placebos,
    run_local_projections_by_regime,
)
from bidbridge.analysis.maturity_summary import write_open07_maturity_bucket_summary
from bidbridge.analysis.outputs import run_all_analysis
from bidbridge.analysis.panel_fe import (
    build_bucket_outcomes,
    generate_panel_fe_table,
    run_bucket_fe_regression,
)
from bidbridge.analysis.persistence import generate_persistence_table
from bidbridge.analysis.promotion import write_promotion_artifacts
from bidbridge.analysis.ratewall_contracts import (
    write_ratewall_dealer_warehousing_support_contract,
)
from bidbridge.cli import _study_week_definition
from bidbridge.data.pipeline import (
    _harmonize_auctions,
    _harmonize_investor_class,
    build_panel,
)
from bidbridge.data.sources.nyfed_pd import finalize_primary_dealer_dataframe
from bidbridge.features.maturity_panel import build_maturity_panel
from bidbridge.open07_reconstruction import (
    CORROBORATING_SOURCE,
    PRIMARY_METADATA,
    PRIMARY_SOURCE,
    RECONSTRUCTED_INPUT,
    RECONSTRUCTION_MANIFEST,
    reconstruct,
    sha256,
)

REPRODUCTION_VERSION = "bidbridge_open07_reconstructed_chain_v5"
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVIDENCE = ROOT / "receipts/open07_v5_reproduction_evidence.json"
SCRATCH_REPORT = "receipts/open07_v5_scratch_report.json"

CHAIN_ARTIFACTS = (
    "data/processed/auction_week_panel.csv",
    "data/processed/maturity_bucket_panel.csv",
    "data/processed/maturity_wide_panel.csv",
    "outputs/tables/lp_results.csv",
    "outputs/tables/lp_placebo_results.csv",
    "outputs/tables/panel_fe_results.csv",
    "outputs/tables/panel_fe_results_thursday.csv",
    "outputs/tables/regression_refunding_test.csv",
    "outputs/tables/maturity_bucket_summary.csv",
    "outputs/tables/persistence_summary.csv",
    "outputs/tables/bidbridge_promotion_summary.csv",
    "outputs/bidbridge_promotion_memo.md",
    "outputs/tables/bidbridge_ratewall_dealer_warehousing_support.csv",
)

def _digest_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _artifact_evidence(root: Path, relative_path: str) -> dict[str, Any]:
    path = root / relative_path
    if not path.is_file():
        raise FileNotFoundError(f"Expected reproduced artifact is missing: {relative_path}")
    return {
        "path": relative_path,
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
    }


def _producer_surface_paths(root: Path) -> list[str]:
    paths = [path for path in (root / "bidbridge").rglob("*.py") if path.is_file()]
    paths.extend(path for path in (root / "configs").glob("*.*") if path.is_file())
    paths.append(root / "pyproject.toml")
    return sorted(path.relative_to(root).as_posix() for path in paths)


def _estimation_rows(outcomes: pd.DataFrame) -> pd.DataFrame:
    rows = outcomes.dropna(subset=["delta_position", "announced_amount"]).copy()
    rows["supply_B"] = rows["announced_amount"] / 1e9
    rows = rows.dropna(subset=["delta_position", "supply_B"])
    columns = [
        "maturity_bucket",
        "week_start",
        "delta_position",
        "supply_B",
    ]
    rows = rows[columns].sort_values(
        ["week_start", "maturity_bucket"], kind="mergesort"
    )
    rows["week_start"] = pd.to_datetime(rows["week_start"]).dt.strftime("%Y-%m-%d")
    return rows.reset_index(drop=True)


def _strict_lag_estimation_rows(outcomes: pd.DataFrame) -> pd.DataFrame:
    lagged = outcomes[
        ["maturity_bucket", "week_start", "lagged_dealer_share"]
    ].copy()
    lagged["week_start"] = pd.to_datetime(lagged["week_start"]).dt.strftime(
        "%Y-%m-%d"
    )
    rows = _estimation_rows(outcomes).merge(
        lagged,
        on=["maturity_bucket", "week_start"],
        how="left",
        validate="one_to_one",
    )
    rows = rows.dropna(subset=["lagged_dealer_share"])
    return rows.sort_values(
        ["week_start", "maturity_bucket"], kind="mergesort"
    ).reset_index(drop=True)


def _sample_evidence(outcomes: pd.DataFrame, results: dict[str, object]) -> dict[str, Any]:
    rows = _estimation_rows(outcomes)
    payload = rows.to_csv(index=False, lineterminator="\n").encode("utf-8")
    strict_rows = _strict_lag_estimation_rows(outcomes)
    strict_payload = strict_rows.to_csv(index=False, lineterminator="\n").encode(
        "utf-8"
    )
    info = dict(results["_sample_info"])
    metadata = dict(results["_metadata"])
    info.update(
        {
            "estimation_rows_sha256": _digest_bytes(payload),
            "estimation_row_columns": list(rows.columns),
            "frequency": "weekly",
            "panel_unit": "maturity_bucket_by_week",
            "dependent_variable": (
                "change in aggregate primary-dealer Treasury position"
            ),
            "dependent_variable_unit": "USD millions",
            "treatment": (
                "offering amounts announced for auctions occurring in the "
                "maturity-bucket week"
            ),
            "treatment_unit": "USD billions",
            "coefficient_unit": (
                "USD millions dealer-position change per USD billion announced supply"
            ),
            "fixed_effects": ["maturity_bucket", "week"],
            "headline_covariance": "Driscoll-Kraay",
            "headline_specification": "supply_only_two_way_fixed_effects",
            "driscoll_kraay_kernel": metadata["driscoll_kraay_kernel"],
            "driscoll_kraay_bandwidth": metadata["driscoll_kraay_bandwidth"],
            "strict_lag_robustness": {
                **dict(results["_strict_lag_sample_info"]),
                "estimation_rows_sha256": _digest_bytes(strict_payload),
                "estimation_row_columns": list(strict_rows.columns),
                "driscoll_kraay_kernel": metadata["driscoll_kraay_kernel"],
                "driscoll_kraay_bandwidth": metadata[
                    "strict_lag_driscoll_kraay_bandwidth"
                ],
            },
        }
    )
    return info


def _run_chain(root: Path) -> dict[str, Any]:
    raw_treasury = root / "data/raw/treasury"
    reconstructed = root / RECONSTRUCTED_INPUT.relative_to(ROOT)
    reconstruction_manifest = root / RECONSTRUCTION_MANIFEST.relative_to(ROOT)
    if not reconstructed.is_file() or not reconstruction_manifest.is_file():
        raise FileNotFoundError("Versioned reconstructed input and manifest are required.")

    retained_input_hash = sha256(reconstructed)
    retained_manifest_hash = sha256(reconstruction_manifest)
    _, _, generated_reconstruction = reconstruct(
        primary_path=root / PRIMARY_SOURCE.relative_to(ROOT),
        metadata_path=root / PRIMARY_METADATA.relative_to(ROOT),
        corroborating_path=root / CORROBORATING_SOURCE.relative_to(ROOT),
        output_path=reconstructed,
        manifest_path=reconstruction_manifest,
    )
    raw_sources_equivalent = bool(
        generated_reconstruction["frozen_source_equivalence"]["rows"]
        == generated_reconstruction["replacement_normalized"]["rows"]
        and generated_reconstruction["frozen_source_equivalence"]["columns"] > 0
    )
    raw_to_normalized_exact = sha256(reconstructed) == retained_input_hash
    reconstruction_manifest_exact = (
        sha256(reconstruction_manifest) == retained_manifest_hash
    )
    if not raw_sources_equivalent:
        raise RuntimeError("Replacement sources did not pass frozen-domain equivalence.")
    if not raw_to_normalized_exact or not reconstruction_manifest_exact:
        raise RuntimeError(
            "Raw-source reconstruction did not exactly reproduce the retained "
            "normalized input and manifest."
        )

    shutil.copy2(reconstructed, raw_treasury / "treasury_auctions.csv")
    shutil.copy2(
        reconstruction_manifest,
        raw_treasury / "treasury_auctions_manifest.json",
    )

    for relative_path in CHAIN_ARTIFACTS:
        path = root / relative_path
        if path.exists():
            path.unlink()

    processed_dir = root / "data/processed"
    tables_dir = root / "outputs/tables"
    outputs_dir = root / "outputs"
    processed_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    auctions_path = raw_treasury / "treasury_auctions.csv"
    investor_path = raw_treasury / "investor_class_allotments.csv"
    dealer_path = root / "data/raw/nyfed/primary_dealer_stats.csv"
    week_definition = _study_week_definition()
    panel_path = processed_dir / "auction_week_panel.csv"
    # H.8 was an optional overlay and its historical raw pointer is unavailable.
    # It is not used by the retained OPEN-07 LP, FE, refunding, maturity, persistence,
    # promotion, or RateWall surfaces. SOMA is available and remains in the panel.
    h8_path = root / "data/raw/fed/h8_bank_securities.csv"
    h8_manifest = root / "data/raw/fed/h8_bank_securities_manifest.json"
    if h8_path.exists():
        h8_path.unlink()
    if h8_manifest.exists():
        h8_manifest.unlink()
    build_panel(
        raw_dir=root / "data/raw",
        output_path=panel_path,
        start_date="2010-01-01",
        week_definition=week_definition,
    )

    auctions = _harmonize_auctions(auctions_path)
    investor = _harmonize_investor_class(investor_path)

    run_all_analysis(panel_path)
    panel_from_disk = pd.read_csv(panel_path, parse_dates=["week_start", "week_end"])
    lp = run_local_projections_by_regime(panel_from_disk)
    generate_lp_table(lp, tables_dir)
    generate_lp_placebo_table(
        run_local_projection_placebos(panel_from_disk),
        tables_dir,
    )

    dealer_raw = pd.read_csv(
        dealer_path,
        parse_dates=["as_of_date", "week_start", "week_end"],
    )
    samples: dict[str, Any] = {}
    for label, definition, output_name in (
        ("headline_monday", week_definition, "panel_fe_results.csv"),
        ("robustness_thursday", "thursday", "panel_fe_results_thursday.csv"),
    ):
        maturity_for_fe = build_maturity_panel(
            auctions,
            investor,
            week_definition=definition,
        )
        dealer_for_fe = finalize_primary_dealer_dataframe(
            dealer_raw,
            week_definition=definition,
        )
        outcomes = build_bucket_outcomes(
            maturity_for_fe,
            dealer_for_fe,
            headline_strict=True,
            week_definition=definition,
        )
        results = run_bucket_fe_regression(outcomes)
        generate_panel_fe_table(results, tables_dir, file_name=output_name)
        samples[label] = _sample_evidence(outcomes, results)

    generate_persistence_table(panel_from_disk, tables_dir)
    write_open07_maturity_bucket_summary(
        auctions_path=auctions_path,
        investor_class_path=investor_path,
        output_path=tables_dir / "maturity_bucket_summary.csv",
    )
    write_promotion_artifacts(
        tables_dir,
        tables_dir / "bidbridge_promotion_summary.csv",
        outputs_dir / "bidbridge_promotion_memo.md",
    )
    write_ratewall_dealer_warehousing_support_contract(
        panel_path=panel_path,
        persistence_summary_path=tables_dir / "persistence_summary.csv",
        output_path=tables_dir / "bidbridge_ratewall_dealer_warehousing_support.csv",
    )

    return {
        "artifacts": [_artifact_evidence(root, path) for path in CHAIN_ARTIFACTS],
        "estimation_samples": samples,
        "source_reconstruction_checks": {
            "raw_sources_equivalent_over_frozen_domain": raw_sources_equivalent,
            "raw_to_normalized_exactly_reproduced": raw_to_normalized_exact,
            "reconstruction_manifest_exactly_reproduced": (
                reconstruction_manifest_exact
            ),
            "normalized_input_sha256": sha256(reconstructed),
            "reconstruction_manifest_sha256": sha256(reconstruction_manifest),
        },
    }


def _versioned_path(relative_path: str) -> str:
    path = Path(relative_path)
    if path.parts[:2] == ("data", "processed"):
        return (Path("data/processed/open07_v5") / path.name).as_posix()
    if path.parts[:2] == ("outputs", "tables"):
        return (Path("outputs/open07_v5/tables") / path.name).as_posix()
    if path.parts[0] == "outputs":
        return (Path("outputs/open07_v5") / path.name).as_posix()
    raise ValueError(f"No versioned destination for {relative_path}")


def _copy_project_to_scratch(root: Path, scratch: Path) -> None:
    ignored = shutil.ignore_patterns(
        ".git",
        "do",
        ".worktrees",
        ".codex",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        "*.pyc",
    )
    shutil.copytree(root, scratch, ignore=ignored)


def _producer_commit(root: Path, explicit_commit: str | None) -> tuple[str, str]:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        head = result.stdout.strip()
        if explicit_commit and explicit_commit != head:
            raise RuntimeError(f"Explicit commit {explicit_commit} != local HEAD {head}.")
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        if dirty:
            raise RuntimeError("Producer tree must be clean:\n" + dirty)
        return head, "local_git_head_and_clean_tree"
    if not explicit_commit:
        raise RuntimeError("Gitless runner requires --producer-commit.")
    return explicit_commit, "explicit_commit_in_remrun_reconciled_checkout"


def reproduce(
    *,
    root: Path,
    evidence_path: Path,
    producer_commit: str | None,
    promote: bool,
) -> dict[str, Any]:
    commit, commit_verification = _producer_commit(root, producer_commit)
    with tempfile.TemporaryDirectory(prefix="bidbridge-open07-v5-") as temporary:
        scratch = Path(temporary) / "bidbridge"
        _copy_project_to_scratch(root, scratch)
        child_report = scratch / SCRATCH_REPORT
        env = os.environ.copy()
        env["PYTHONPATH"] = str(scratch)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        command = [
            sys.executable,
            "-B",
            "-m",
            "bidbridge.open07_reproduce",
            "--scratch-run",
            "--evidence",
            SCRATCH_REPORT,
        ]
        completed = subprocess.run(
            command,
            cwd=scratch,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "OPEN-07 v5 scratch reproduction failed.\n"
                + completed.stdout
                + completed.stderr
            )
        scratch_payload = json.loads(child_report.read_text(encoding="utf-8"))

        generated = {item["path"]: item for item in scratch_payload["artifacts"]}
        legacy_comparison = []
        versioned_artifacts = []
        for relative_path in CHAIN_ARTIFACTS:
            generated_path = scratch / relative_path
            legacy_path = root / relative_path
            legacy_comparison.append(
                {
                    "path": relative_path,
                    "legacy_present": legacy_path.is_file(),
                    "legacy_sha256": sha256(legacy_path) if legacy_path.is_file() else None,
                    "generated_sha256": generated[relative_path]["sha256"],
                    "exact_match": (
                        legacy_path.is_file()
                        and sha256(legacy_path) == generated[relative_path]["sha256"]
                    ),
                }
            )
            versioned_relative = _versioned_path(relative_path)
            versioned_path = root / versioned_relative
            if promote:
                versioned_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(generated_path, versioned_path)
            if not versioned_path.is_file():
                raise FileNotFoundError(
                    f"Versioned retained artifact is missing: {versioned_relative}"
                )
            actual_hash = sha256(versioned_path)
            if actual_hash != generated[relative_path]["sha256"]:
                raise RuntimeError(
                    f"Versioned artifact mismatch for {versioned_relative}: "
                    f"{actual_hash} != {generated[relative_path]['sha256']}"
                )
            versioned_artifacts.append(
                {
                    "path": versioned_relative,
                    "bytes": versioned_path.stat().st_size,
                    "sha256": actual_hash,
                    "generated_from": relative_path,
                }
            )

    payload: dict[str, Any] = {
        "reproduction_version": REPRODUCTION_VERSION,
        "producer_commit": commit,
        "producer_commit_verification": commit_verification,
        "command": (
            "python -B -m bidbridge.open07_reproduce "
            f"--producer-commit {commit}"
            + (" --promote" if promote else "")
        ),
        "promotion_mode": promote,
        "source_reconstruction": {
            "input": _artifact_evidence(
                root,
                RECONSTRUCTED_INPUT.relative_to(ROOT).as_posix(),
            ),
            "manifest": _artifact_evidence(
                root,
                RECONSTRUCTION_MANIFEST.relative_to(ROOT).as_posix(),
            ),
        },
        "optional_source_exclusions": [
            {
                "source": "H.8 bank securities overlay",
                "reason": (
                    "historical raw pointer unavailable; field is not consumed by any "
                    "enumerated OPEN-07 LP, FE, refunding, maturity, persistence, "
                    "promotion, or RateWall artifact"
                ),
            }
        ],
        "producer_surface": [
            _artifact_evidence(root, path) for path in _producer_surface_paths(root)
        ],
        "estimation_samples": scratch_payload["estimation_samples"],
        "versioned_artifacts": versioned_artifacts,
        "legacy_comparison": legacy_comparison,
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            **{
                name: importlib.metadata.version(name)
                for name in ("pandas", "numpy", "statsmodels", "linearmodels")
            },
        },
        "claim_boundary": {
            "admissible": (
                "short-run aggregate primary-dealer warehousing and inventory response"
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
        "acceptance": {
            **scratch_payload["source_reconstruction_checks"],
            "normalized_to_outputs_exactly_reproduced": (
                set(generated) == set(CHAIN_ARTIFACTS)
                and all(item["sha256"] for item in generated.values())
            ),
            "all_versioned_artifacts_exactly_matched": (
                len(versioned_artifacts) == len(CHAIN_ARTIFACTS)
                and all(
                    item["sha256"] == generated[item["generated_from"]]["sha256"]
                    for item in versioned_artifacts
                )
            ),
            "original_source_byte_identity_claimed": False,
        },
    }
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--producer-commit")
    parser.add_argument("--promote", action="store_true")
    parser.add_argument("--scratch-run", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--evidence", type=Path, default=DEFAULT_EVIDENCE)
    args = parser.parse_args(argv)
    evidence = args.evidence if args.evidence.is_absolute() else ROOT / args.evidence
    if args.scratch_run:
        payload = _run_chain(ROOT)
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
    else:
        payload = reproduce(
            root=ROOT,
            evidence_path=evidence,
            producer_commit=args.producer_commit,
            promote=args.promote,
        )
        print(f"reproduction_evidence={evidence}")
        print(f"reproduction_evidence_sha256={sha256(evidence)}")
        print(f"versioned_artifacts={len(payload['versioned_artifacts'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
