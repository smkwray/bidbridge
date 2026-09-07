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
