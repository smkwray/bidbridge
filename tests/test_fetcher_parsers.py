from __future__ import annotations

import pandas as pd
import pytest

from bidbridge.data.sources.nyfed_pd import finalize_primary_dealer_dataframe
from bidbridge.data.sources.treasury_investor_class import (
    InvestorClassDiscoveryError,
    _discover_allotment_links,
    _parse_allotment_df,
    _read_allotment_workbook,
)


class _MockResponse:
    def __init__(self, text: str = "", content: bytes = b""):
        self.text = text
        self.content = content

    def raise_for_status(self) -> None:
        return None


def test_discover_allotment_links_handles_changed_path_structure(monkeypatch):
    html = """
    <a href="/downloads/current-coupons-release.xlsx">Current coupons</a>
    <a href="https://home.treasury.gov/files/current-bills-2026.xls">Current bills</a>
    <a href="/archive/historical-coupons-2000.xls">Historical coupons</a>
    <a href="/legacy/historical-bills-2001.xlsx">Historical bills</a>
    """

    monkeypatch.setattr(
        "bidbridge.data.sources.treasury_investor_class.requests.get",
        lambda *args, **kwargs: _MockResponse(text=html),
    )
    links = _discover_allotment_links()
    assert set(links) == {"coupons", "bills", "hist_coupons", "hist_bills"}
    assert links["coupons"].endswith("/downloads/current-coupons-release.xlsx")
    assert links["bills"].endswith("/files/current-bills-2026.xls")
    assert links["hist_coupons"].endswith("/archive/historical-coupons-2000.xls")
    assert links["hist_bills"].endswith("/legacy/historical-bills-2001.xlsx")


def test_discover_allotment_links_raises_on_empty_page(monkeypatch):
    monkeypatch.setattr(
        "bidbridge.data.sources.treasury_investor_class.requests.get",
        lambda *args, **kwargs: _MockResponse(text="<html><body>No files here</body></html>"),
    )
    with pytest.raises(InvestorClassDiscoveryError) as excinfo:
        _discover_allotment_links()
    assert excinfo.value.landing_url.endswith("investor-class-auction-allotments")
    assert excinfo.value.total_links == 0
    assert excinfo.value.spreadsheet_links == []


def test_read_allotment_workbook_uses_xls_engine(monkeypatch):
    raw = pd.DataFrame([
        ["Treasury investor class"],
        ["Issue Date", "Cusip", "Dealer Share", "Total Issue Amount"],
        ["2025-01-09", "123456AA", 35.0, 100.0],
    ])
    parsed = pd.DataFrame([
        ["2025-01-09", "123456AA", 35.0, 100.0],
    ], columns=["Issue Date", "Cusip", "Dealer Share", "Total Issue Amount"])
    calls: list[tuple[str | None, int | None]] = []

    def fake_read_excel(workbook, engine=None, header=None):
        calls.append((engine, header))
        if header is None:
            return raw
        return parsed

    monkeypatch.setattr(
        "bidbridge.data.sources.treasury_investor_class.requests.get",
        lambda *args, **kwargs: _MockResponse(content=b"fake-xls-bytes"),
    )
    monkeypatch.setattr(
        "bidbridge.data.sources.treasury_investor_class.pd.read_excel",
        fake_read_excel,
    )

    df = _read_allotment_workbook("https://home.treasury.gov/files/current-coupons.xls")
    assert calls == [("xlrd", None), ("xlrd", 1)]
    assert list(df.columns) == ["issue_date", "cusip", "dealer_share", "total_issue_amount"]


def test_read_allotment_workbook_uses_xlsx_engine(monkeypatch):
    raw = pd.DataFrame([
        ["Treasury investor class"],
        ["Issue Date", "Cusip", "Dealer Share", "Total Issue Amount"],
        ["2025-01-09", "123456AA", 35.0, 100.0],
    ])
    parsed = pd.DataFrame([
        ["2025-01-09", "123456AA", 35.0, 100.0],
    ], columns=["Issue Date", "Cusip", "Dealer Share", "Total Issue Amount"])
    calls: list[tuple[str | None, int | None]] = []

    def fake_read_excel(workbook, engine=None, header=None):
        calls.append((engine, header))
        if header is None:
            return raw
        return parsed

    monkeypatch.setattr(
        "bidbridge.data.sources.treasury_investor_class.requests.get",
        lambda *args, **kwargs: _MockResponse(content=b"fake-xlsx-bytes"),
    )
    monkeypatch.setattr(
        "bidbridge.data.sources.treasury_investor_class.pd.read_excel",
        fake_read_excel,
    )

    df = _read_allotment_workbook("https://home.treasury.gov/files/current-coupons.xlsx")
    assert calls == [(None, None), (None, 1)]
    assert list(df.columns) == ["issue_date", "cusip", "dealer_share", "total_issue_amount"]


def test_parse_allotment_df_converts_amounts_to_shares():
    df = pd.DataFrame({
        "issue_date": ["2025-01-09"],
        "security_type": ["10-Year Bond"],
        "total_issue_amount": [100.0],
        "dealer_share": [35.0],
        "investment_funds_share": [25.0],
        "foreign_share": [20.0],
        "depository_share": [10.0],
        "other_share": [5.0],
        "fed_reserve_share": [5.0],
    })
    parsed = _parse_allotment_df(df)
    assert parsed.loc[0, "dealer_share"] == 0.35
    assert parsed.loc[0, "security_type"] == "Bond"
    assert round(parsed.loc[0, "other_share"], 2) == 0.10


def test_parse_allotment_df_tolerates_missing_security_type_column():
    df = pd.DataFrame({
        "issue_date": ["2025-01-09"],
        "cusip": ["123456AA"],
        "total_issue_amount": [100.0],
        "dealer_share": [35.0],
        "investment_funds_share": [25.0],
        "foreign_share": [20.0],
        "depository_share": [10.0],
        "other_share": [10.0],
    })
    parsed = _parse_allotment_df(df)
    assert "security_type" in parsed.columns
    assert parsed.loc[0, "security_type"] == ""


def test_finalize_primary_dealer_dataframe_forward_fills_and_reanchors():
    df = pd.DataFrame({
        "as_of_date": pd.to_datetime(["2025-01-08", "2025-01-15"]),
        "pd_repo_treasury": [10.0, None],
        "pd_reverse_repo_treasury": [4.0, None],
        "pd_financing_usage": [6.0, None],
    })
    finalized = finalize_primary_dealer_dataframe(df, week_definition="thursday")
    assert finalized.loc[1, "pd_repo_treasury"] == 10.0
    assert finalized.loc[1, "pd_repo_treasury_raw"] != finalized.loc[1, "pd_repo_treasury"]
    assert finalized.loc[0, "week_start"].strftime("%Y-%m-%d") == "2025-01-02"
