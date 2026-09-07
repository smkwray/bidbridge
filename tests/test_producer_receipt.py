from __future__ import annotations

import hashlib

import pytest

from bidbridge.producer_receipt import _file_evidence


def test_file_evidence_hashes_resolved_pointer_content(tmp_path) -> None:
    actual = tmp_path / "actual.csv"
    pointer = tmp_path / "pointer.csv"
    actual.write_bytes(b"a,b\n1,2\n")
    pointer.write_text(str(actual), encoding="utf-8")

    evidence = _file_evidence(tmp_path, "pointer.csv", resolve_pointer=True)

    assert evidence["storage"] == "one_line_pointer"
    assert evidence["sha256"] == hashlib.sha256(actual.read_bytes()).hexdigest()
    assert evidence["pointer_sha256"] == hashlib.sha256(pointer.read_bytes()).hexdigest()


def test_file_evidence_fails_closed_for_unavailable_pointer(tmp_path) -> None:
    pointer = tmp_path / "pointer.csv"
    pointer.write_text(str(tmp_path / "missing.csv"), encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="unavailable"):
        _file_evidence(tmp_path, "pointer.csv", resolve_pointer=True)


def test_superseded_receipt_cli_cannot_issue_timing_claim():
    import subprocess
    import sys
    from pathlib import Path

    from bidbridge.producer_receipt import write_receipt

    result = subprocess.run([sys.executable, "-B", "-m", "bidbridge.producer_receipt"],
                            capture_output=True, text=True, check=False)
    assert result.returncode != 0
    assert "Superseded by OPEN-07 v5" in result.stderr
    source = Path(__file__).resolve().parents[1] / "bidbridge/producer_receipt.py"
    assert "timing-consistent" not in source.read_text()
    with pytest.raises(ValueError, match="Superseded"):
        write_receipt(Path("unused.json"), root=source.parent)
