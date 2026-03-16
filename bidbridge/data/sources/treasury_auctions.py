"""Treasury auctions source via FiscalData API.

Endpoint:
  https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query

Returns completed auction results with bid-to-cover, yields, amounts, and bidder breakdowns.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from ..sources.base import DownloadManifest, write_manifest

logger = logging.getLogger(__name__)

API_BASE = (
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
    "/v1/accounting/od/auctions_query"
)

FIELDS = ",".join([
    "cusip",
    "auction_date",
    "issue_date",
    "maturity_date",
    "announcemt_date",
    "security_type",
    "security_term",
    "offering_amt",
    "total_accepted",
    "total_tendered",
    "bid_to_cover_ratio",
    "high_yield",
    "avg_med_yield",
    "high_discnt_rate",
    "avg_med_discnt_rate",
    "high_investment_rate",
    "high_price",
    "price_per100",
    "primary_dealer_accepted",
    "primary_dealer_tendered",
    "direct_bidder_accepted",
    "direct_bidder_tendered",
    "indirect_bidder_accepted",
    "indirect_bidder_tendered",
    "soma_accepted",
    "comp_accepted",
    "noncomp_accepted",
    "reopening",
    "inflation_index_security",
    "floating_rate",
    "cash_management_bill_cmb",
    "int_rate",
    "allocation_pctage",
])

PAGE_SIZE = 10_000


def _null_safe(val: str | None) -> str | None:
    """FiscalData returns the literal string 'null' for missing values."""
    if val is None or val == "null":
        return None
    return val


def _fetch_page(page: int, start_date: str | None = None) -> dict:
    params: dict[str, str] = {
        "fields": FIELDS,
        "sort": "-auction_date",
        "page[size]": str(PAGE_SIZE),
        "page[number]": str(page),
        "format": "json",
    }
    if start_date:
        params["filter"] = f"auction_date:gte:{start_date}"

    resp = requests.get(API_BASE, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def fetch_treasury_auctions(
    output_dir: Path,
    start_date: str = "2010-01-01",
) -> Path:
    """Fetch all Treasury auction results from FiscalData API.

    Parameters
    ----------
    output_dir : Path
        Directory where the CSV and manifest will be written.
    start_date : str
        Earliest auction_date to fetch (YYYY-MM-DD).

    Returns
    -------
    Path
        Path to the written CSV file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    all_records: list[dict] = []
    page = 1
    total_pages = None

    while total_pages is None or page <= total_pages:
        logger.info("Fetching treasury auctions page %d / %s", page, total_pages or "?")
        data = _fetch_page(page, start_date)
        meta = data.get("meta", {})
        total_pages = meta.get("total-pages", 1)
        records = data.get("data", [])
        if not records:
            break
        all_records.extend(records)
        page += 1

    logger.info("Fetched %d auction records total", len(all_records))

    rows = []
    for rec in all_records:
        # Use bid_to_cover_ratio as the completeness check — it's populated
        # for all completed auctions (Bills, Notes, Bonds alike).
        btc = _null_safe(rec.get("bid_to_cover_ratio"))
        if btc is None:
            continue

        sec_type = rec.get("security_type", "")
        is_tips = rec.get("inflation_index_security", "No") == "Yes"
        is_frn = rec.get("floating_rate", "No") == "Yes"
        is_cmb = rec.get("cash_management_bill_cmb", "No") == "Yes"

        if is_tips:
            instrument_group = "tips"
        elif is_frn:
            instrument_group = "frns"
        elif sec_type == "Bill":
            instrument_group = "cmb" if is_cmb else "bills"
        elif sec_type == "Bond":
            instrument_group = "bonds"
        else:
            instrument_group = "nominal_coupons"

        total_accepted = _null_safe(rec.get("total_accepted"))
        offering = _null_safe(rec.get("offering_amt"))

        # Bills use discount rate fields; Notes/Bonds use yield fields.
        # For tail_bp we must compare rates on the same basis:
        #   Bills  → high_discnt_rate vs avg_med_discnt_rate (discount basis)
        #   Others → high_yield vs avg_med_yield (yield basis)
        # high_investment_rate is kept as a separate output column.
        if sec_type == "Bill":
            high_rate = _null_safe(rec.get("high_discnt_rate"))
            avg_rate = _null_safe(rec.get("avg_med_discnt_rate"))
        else:
            high_rate = _null_safe(rec.get("high_yield"))
            avg_rate = _null_safe(rec.get("avg_med_yield"))

        high_investment_rate_val = _null_safe(rec.get("high_investment_rate"))

        tail_bp = None
        if high_rate and avg_rate:
            try:
                tail_bp = round((float(high_rate) - float(avg_rate)) * 100, 4)
            except (ValueError, TypeError):
                pass

        rows.append({
            "cusip": rec.get("cusip"),
            "auction_date": rec.get("auction_date"),
            "issue_date": rec.get("issue_date"),
            "maturity_date": rec.get("maturity_date"),
            "announcement_date": rec.get("announcemt_date"),
            "security_type": sec_type,
            "security_term": rec.get("security_term"),
            "instrument_group": instrument_group,
            "offering_amount": float(offering) if offering else None,
            "announced_amount": float(offering) if offering else None,
            "awarded_amount": float(total_accepted) if total_accepted else None,
            "bid_to_cover": float(btc),
            "high_yield": float(high_rate) if high_rate else None,
            "high_investment_rate": float(high_investment_rate_val) if high_investment_rate_val else None,
            "tail_bp": tail_bp,
            "primary_dealer_accepted": (
                float(v) if (v := _null_safe(rec.get("primary_dealer_accepted"))) else None
            ),
            "direct_bidder_accepted": (
                float(v) if (v := _null_safe(rec.get("direct_bidder_accepted"))) else None
            ),
            "indirect_bidder_accepted": (
                float(v) if (v := _null_safe(rec.get("indirect_bidder_accepted"))) else None
            ),
            "reopening": rec.get("reopening", "No") == "Yes",
            "is_tips": is_tips,
            "is_frn": is_frn,
            "is_cmb": is_cmb,
        })

    _EXPECTED_COLUMNS = [
        "cusip", "auction_date", "issue_date", "maturity_date",
        "announcement_date", "security_type", "security_term",
        "instrument_group", "offering_amount", "announced_amount",
        "awarded_amount", "bid_to_cover", "high_yield",
        "high_investment_rate", "tail_bp",
        "primary_dealer_accepted", "direct_bidder_accepted",
        "indirect_bidder_accepted", "reopening", "is_tips", "is_frn", "is_cmb",
    ]

    df = pd.DataFrame(rows)

    if df.empty:
        df = pd.DataFrame(columns=_EXPECTED_COLUMNS)
    else:
        for col in ["auction_date", "issue_date", "maturity_date", "announcement_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        df = df.sort_values("auction_date").reset_index(drop=True)

    csv_path = output_dir / "treasury_auctions.csv"
    df.to_csv(csv_path, index=False)

    write_manifest(
        output_dir / "treasury_auctions_manifest.json",
        DownloadManifest(
            source_id="treasury_auctions",
            source_url=API_BASE,
            retrieved_at_utc=datetime.now(timezone.utc).isoformat(),
            local_filename="treasury_auctions.csv",
            parser_version="v1",
            content_type="text/csv",
            notes=f"{len(df)} completed auctions from {start_date}",
        ),
    )

    logger.info("Wrote %d rows to %s", len(df), csv_path)
    return csv_path


def fetch_upcoming_auctions(output_dir: Path) -> Path:
    """Fetch upcoming/announced auctions that haven't settled yet."""
    output_dir.mkdir(parents=True, exist_ok=True)

    params = {
        "fields": FIELDS,
        "sort": "-auction_date",
        "page[size]": "500",
        "format": "json",
        "filter": "high_yield:eq:null",
    }
    resp = requests.get(API_BASE, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    records = data.get("data", [])

    rows = []
    for rec in records:
        rows.append({
            "cusip": rec.get("cusip"),
            "auction_date": rec.get("auction_date"),
            "issue_date": rec.get("issue_date"),
            "security_type": rec.get("security_type"),
            "security_term": rec.get("security_term"),
            "offering_amount": (
                float(v) if (v := _null_safe(rec.get("offering_amt"))) else None
            ),
        })

    df = pd.DataFrame(rows)
    csv_path = output_dir / "upcoming_auctions.csv"
    df.to_csv(csv_path, index=False)
    logger.info("Wrote %d upcoming auctions to %s", len(df), csv_path)
    return csv_path
