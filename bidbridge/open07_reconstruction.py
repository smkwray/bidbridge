"""Build the versioned OPEN-07 replacement Treasury-auction input.

This is a bounded reconstruction, not recovery of the missing original file.
It requires two retained FiscalData extracts to agree exactly over the frozen
domain before it writes a newly named normalized input and manifest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from bidbridge.data.sources.treasury_auctions import API_BASE, FIELDS

RECONSTRUCTION_VERSION = "bidbridge_open07_treasury_reconstruction_v2"
START_DATE = "2010-01-01"
FROZEN_CUTOFF = "2026-03-22"
SORT_KEYS = ("auction_date", "cusip")
PRIMARY_SOURCE_PROJECT = "https://github.com/smkwray/qrawatch"
PRIMARY_SOURCE_COMMIT = "a59de59e0f813fdd0e4d717795f16cc6e04f5022"
CORROBORATING_SOURCE_PROJECT = "https://github.com/smkwray/buycurve"
CORROBORATING_SOURCE_COMMIT = "01b591bbea060025eab50bbb7b8190a8b8445b01"

ROOT = Path(__file__).resolve().parents[1]
V2_DIR = ROOT / "data/raw/treasury/open07_reconstruction_v2"
PRIMARY_SOURCE = V2_DIR / "source/qrawatch_auctions_query.csv"
PRIMARY_METADATA = V2_DIR / "source/qrawatch_auctions_query_meta.json"
CORROBORATING_SOURCE = V2_DIR / "source/buycurve_auctions_query.csv"
RECONSTRUCTED_INPUT = V2_DIR / "treasury_auctions_reconstructed.csv"
RECONSTRUCTION_MANIFEST = V2_DIR / "treasury_auctions_reconstructed_manifest.json"

NORMALIZED_COLUMNS = (
    "cusip",
    "auction_date",
    "issue_date",
    "maturity_date",
    "announcement_date",
    "security_type",
    "security_term",
    "instrument_group",
    "offering_amount",
    "announced_amount",
    "awarded_amount",
    "bid_to_cover",
    "high_yield",
    "high_investment_rate",
    "tail_bp",
    "primary_dealer_accepted",
    "direct_bidder_accepted",
    "indirect_bidder_accepted",
    "reopening",
    "is_tips",
    "is_frn",
    "is_cmb",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def _read_source(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Required reconstruction source is missing: {path}")
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = set(FIELDS.split(","))
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Reconstruction source {path} lacks required fields: {missing}")
    return frame


def _bounded_domain(frame: pd.DataFrame) -> pd.DataFrame:
    auction_date = frame["auction_date"].astype(str)
    bounded = frame.loc[
        auction_date.between(START_DATE, FROZEN_CUTOFF, inclusive="both")
    ].copy()
    bounded = bounded.sort_values(list(SORT_KEYS), kind="mergesort").reset_index(drop=True)
    duplicates = bounded.duplicated(list(SORT_KEYS), keep=False)
    if duplicates.any():
        keys = bounded.loc[duplicates, list(SORT_KEYS)].head(10).to_dict(orient="records")
        raise ValueError(f"Duplicate reconstruction keys are not allowed: {keys}")
    return bounded


def _canonical_source_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False, lineterminator="\n").encode("utf-8")


def _require_source_equivalence(
    primary: pd.DataFrame,
    corroborating: pd.DataFrame,
) -> dict[str, Any]:
    if list(primary.columns) != list(corroborating.columns):
        raise ValueError("Replacement-source schemas differ over the frozen domain.")
    if not primary.equals(corroborating):
        mismatch = primary.compare(corroborating, keep_shape=False, keep_equal=False)
        raise ValueError(
            "Replacement sources differ over the frozen domain; first mismatches:\n"
            + mismatch.head(10).to_string()
        )
    payload = _canonical_source_bytes(primary)
    return {
        "rows": len(primary),
        "columns": len(primary.columns),
        "canonical_csv_sha256": _sha256_bytes(payload),
        "first_auction_date": str(primary["auction_date"].min()),
        "last_auction_date": str(primary["auction_date"].max()),
    }


def _value(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "null":
        return None
    return text


def _number(value: Any) -> float | None:
    parsed = _value(value)
    return float(parsed) if parsed is not None else None


def normalize_auction_source(source: pd.DataFrame) -> pd.DataFrame:
    """Apply the committed BidBridge field mapping to a bounded source frame."""
    rows: list[dict[str, Any]] = []
    for record in source.to_dict(orient="records"):
        bid_to_cover = _number(record.get("bid_to_cover_ratio"))
        if bid_to_cover is None:
            continue

        security_type = _value(record.get("security_type")) or ""
        is_tips = _value(record.get("inflation_index_security")) == "Yes"
        is_frn = _value(record.get("floating_rate")) == "Yes"
        is_cmb = _value(record.get("cash_management_bill_cmb")) == "Yes"
        if is_tips:
            instrument_group = "tips"
        elif is_frn:
            instrument_group = "frns"
        elif security_type == "Bill":
            instrument_group = "cmb" if is_cmb else "bills"
        elif security_type == "Bond":
            instrument_group = "bonds"
        else:
            instrument_group = "nominal_coupons"

        if security_type == "Bill":
            high_rate = _number(record.get("high_discnt_rate"))
            average_rate = _number(record.get("avg_med_discnt_rate"))
        else:
            high_rate = _number(record.get("high_yield"))
            average_rate = _number(record.get("avg_med_yield"))
        tail_bp = (
            round((high_rate - average_rate) * 100, 4)
            if high_rate is not None and average_rate is not None
            else None
        )
        offering = _number(record.get("offering_amt"))

        rows.append(
            {
                "cusip": _value(record.get("cusip")),
                "auction_date": _value(record.get("auction_date")),
                "issue_date": _value(record.get("issue_date")),
                "maturity_date": _value(record.get("maturity_date")),
                "announcement_date": _value(record.get("announcemt_date")),
                "security_type": security_type,
                "security_term": _value(record.get("security_term")),
                "instrument_group": instrument_group,
                "offering_amount": offering,
                "announced_amount": offering,
                "awarded_amount": _number(record.get("total_accepted")),
                "bid_to_cover": bid_to_cover,
                "high_yield": high_rate,
                "high_investment_rate": _number(record.get("high_investment_rate")),
                "tail_bp": tail_bp,
                "primary_dealer_accepted": _number(
                    record.get("primary_dealer_accepted")
                ),
                "direct_bidder_accepted": _number(record.get("direct_bidder_accepted")),
                "indirect_bidder_accepted": _number(
                    record.get("indirect_bidder_accepted")
                ),
                "reopening": _value(record.get("reopening")) == "Yes",
                "is_tips": is_tips,
                "is_frn": is_frn,
                "is_cmb": is_cmb,
            }
        )

    normalized = pd.DataFrame(rows, columns=NORMALIZED_COLUMNS)
    normalized = normalized.sort_values(list(SORT_KEYS), kind="mergesort").reset_index(
        drop=True
    )
    if normalized.duplicated(list(SORT_KEYS), keep=False).any():
        raise ValueError("Normalized reconstruction contains duplicate auction_date/cusip keys.")
    return normalized


def _file_evidence(path: Path, *, project: str, commit: str | None) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": _display_path(path),
        "bytes": stat.st_size,
        "sha256": sha256(path),
        "last_write_utc": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
        "source_project": project,
        "source_commit": commit,
    }


def reconstruct(
    *,
    primary_path: Path = PRIMARY_SOURCE,
    metadata_path: Path = PRIMARY_METADATA,
    corroborating_path: Path = CORROBORATING_SOURCE,
    output_path: Path = RECONSTRUCTED_INPUT,
    manifest_path: Path = RECONSTRUCTION_MANIFEST,
) -> tuple[Path, Path, dict[str, Any]]:
    primary_full = _read_source(primary_path)
    corroborating_full = _read_source(corroborating_path)
    primary = _bounded_domain(primary_full)
    corroborating = _bounded_domain(corroborating_full)
    equivalence = _require_source_equivalence(primary, corroborating)
    normalized = normalize_auction_source(primary)

    if not metadata_path.is_file():
        raise FileNotFoundError(f"QRAWatch query metadata is missing: {metadata_path}")
    query_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if query_metadata.get("endpoint") != "v1/accounting/od/auctions_query":
        raise ValueError("Unexpected QRAWatch FiscalData endpoint.")

    output_bytes = normalized.to_csv(index=False, lineterminator="\n").encode("utf-8")
    code_path = Path(__file__).resolve()
    manifest: dict[str, Any] = {
        "reconstruction_version": RECONSTRUCTION_VERSION,
        "input_lineage_status": "reconstructed_replacement",
        "producer_input_identity_changed": True,
        "original_normalized_input_recovered": False,
        "original_normalized_input_sha256": None,
        "original_manifest_sha256": None,
        "original_source_byte_identity_proven": False,
        "network_refetch_used": False,
        "start_date": START_DATE,
        "equivalence_scope_end": FROZEN_CUTOFF,
        "sort_keys": list(SORT_KEYS),
        "duplicate_policy": "fail_on_duplicate_auction_date_cusip",
        "literal_null_handling": (
            "actual null/NaN values, empty strings, and the case-insensitive "
            "literal null map to missing"
        ),
        "completed_auction_rule": "bid_to_cover_ratio is nonmissing",
        "numeric_conversion": "Python float from FiscalData decimal text",
        "serialization": {
            "format": "CSV",
            "encoding": "UTF-8",
            "line_ending": "LF",
            "quoting": "pandas CSV minimal quoting",
            "float_serialization": "pandas default round-trip representation",
        },
        "source_fields": FIELDS.split(","),
        "normalized_columns": list(NORMALIZED_COLUMNS),
        "frozen_source_equivalence": equivalence,
        "primary_source": _file_evidence(
            primary_path,
            project=PRIMARY_SOURCE_PROJECT,
            commit=PRIMARY_SOURCE_COMMIT,
        ),
        "primary_query_metadata": _file_evidence(
            metadata_path,
            project=PRIMARY_SOURCE_PROJECT,
            commit=PRIMARY_SOURCE_COMMIT,
        ),
        "corroborating_source": _file_evidence(
            corroborating_path,
            project=CORROBORATING_SOURCE_PROJECT,
            commit=CORROBORATING_SOURCE_COMMIT,
        ),
        "query": {
            "base_url": API_BASE,
            "endpoint": query_metadata.get("endpoint"),
            "parameters": query_metadata.get("params"),
            "pages_fetched": query_metadata.get("pages_fetched"),
            "rows_fetched": query_metadata.get("rows_fetched"),
        },
        "reconstruction_code": {
            "path": code_path.relative_to(ROOT).as_posix(),
            "sha256": sha256(code_path),
        },
        "replacement_normalized": {
            "path": _display_path(output_path),
            "bytes": len(output_bytes),
            "sha256": _sha256_bytes(output_bytes),
            "rows": len(normalized),
            "first_auction_date": str(normalized["auction_date"].min()),
            "last_auction_date": str(normalized["auction_date"].max()),
        },
    }
    manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    destinations = (output_path, manifest_path)
    if output_path.resolve() == manifest_path.resolve():
        raise ValueError("Reconstruction output and manifest must be distinct paths")
    previous = {path: path.read_bytes() if path.exists() else None for path in destinations}
    staged: list[Path] = []
    replaced: list[Path] = []
    try:
        for path, content in zip(destinations, (output_bytes, manifest_bytes), strict=True):
            path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as handle:
                staged.append(Path(handle.name))
                handle.write(content)
            if staged[-1].read_bytes() != content:
                raise ValueError("Reconstruction staging readback failed")
        for temporary, destination in zip(staged, destinations, strict=True):
            temporary.replace(destination)
            replaced.append(destination)
    except BaseException:
        for path in reversed(replaced):
            if previous[path] is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(previous[path])
        raise
    finally:
        for temporary in staged:
            temporary.unlink(missing_ok=True)
    return output_path, manifest_path, manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--primary", type=Path, default=PRIMARY_SOURCE)
    parser.add_argument("--metadata", type=Path, default=PRIMARY_METADATA)
    parser.add_argument("--corroborating", type=Path, default=CORROBORATING_SOURCE)
    parser.add_argument("--output", type=Path, default=RECONSTRUCTED_INPUT)
    parser.add_argument("--manifest", type=Path, default=RECONSTRUCTION_MANIFEST)
    args = parser.parse_args(argv)
    output, manifest, payload = reconstruct(
        primary_path=args.primary,
        metadata_path=args.metadata,
        corroborating_path=args.corroborating,
        output_path=args.output,
        manifest_path=args.manifest,
    )
    print(f"reconstructed_input={output}")
    print(f"reconstructed_input_sha256={payload['replacement_normalized']['sha256']}")
    print(f"reconstruction_manifest={manifest}")
    print(f"reconstruction_manifest_sha256={sha256(manifest)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
