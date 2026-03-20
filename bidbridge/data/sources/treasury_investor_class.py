"""Treasury investor class auction allotments source.

Landing page: https://home.treasury.gov/data/investor-class-auction-allotments

Files are .xls (Excel 97-2003 format) with date-stamped filenames that change
on each release. We scrape the landing page to discover current download URLs.
"""

from __future__ import annotations

import logging
import re
from html import unescape
from io import BytesIO
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

from ..sources.base import DownloadManifest, write_manifest

logger = logging.getLogger(__name__)

LANDING_URL = "https://home.treasury.gov/data/investor-class-auction-allotments"
FILE_BASE = "https://home.treasury.gov"
_HISTORICAL_MARKERS = (
    "histor",
    "archive",
    "legacy",
    "old",
    "pre-2009",
    "pre 2009",
)


class InvestorClassDiscoveryError(RuntimeError):
    """Raised when the Treasury landing page no longer exposes expected links."""

    def __init__(
        self,
        message: str,
        *,
        landing_url: str,
        total_links: int,
        spreadsheet_links: list[str],
    ) -> None:
        super().__init__(message)
        self.landing_url = landing_url
        self.total_links = total_links
        self.spreadsheet_links = spreadsheet_links


def _extract_anchor_hrefs(html: str) -> list[tuple[str, str]]:
    """Return raw href/text pairs from anchor tags in a landing page."""

    anchors: list[tuple[str, str]] = []
    pattern = re.compile(r'<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.I | re.S)
    for match in pattern.finditer(html):
        href = unescape(match.group(1)).strip()
        text = re.sub(r"<[^>]+>", " ", match.group(2))
        text = re.sub(r"\s+", " ", unescape(text)).strip()
        anchors.append((href, text))
    return anchors


def _classify_allotment_link(href: str, text: str) -> str | None:
    """Classify a Treasury allotment workbook into one of the public buckets."""

    parsed = urlparse(href)
    name = Path(parsed.path).name.lower()
    blob = " ".join([href, text, name]).lower()
    suffix = Path(parsed.path).suffix.lower()
    if suffix not in {".xls", ".xlsx"}:
        return None

    is_historical = any(marker in blob for marker in _HISTORICAL_MARKERS)
    if re.search(r"\bcoupons?\b", blob):
        return "hist_coupons" if is_historical else "coupons"
    if re.search(r"\bbills?\b", blob):
        return "hist_bills" if is_historical else "bills"
    return None


def _discover_allotment_links() -> dict[str, str]:
    """Scrape the landing page to find current and historical download links."""

    resp = requests.get(LANDING_URL, timeout=30)
    resp.raise_for_status()
    html = resp.text

    links: dict[str, str] = {}
    candidate_spreadsheets: list[str] = []
    for href, text in _extract_anchor_hrefs(html):
        abs_href = urljoin(FILE_BASE, href)
        suffix = Path(urlparse(abs_href).path).suffix.lower()
        if suffix in {".xls", ".xlsx"}:
            candidate_spreadsheets.append(abs_href)
        classified = _classify_allotment_link(abs_href, text)
        if classified is None:
            continue
        links[classified] = abs_href

    if not links:
        all_links = [urljoin(FILE_BASE, href) for href, _ in _extract_anchor_hrefs(html)]
        message = (
            "Could not discover Treasury investor-class allotment workbooks from "
            f"{LANDING_URL}. The page layout may have changed. "
            f"Anchors found={len(all_links)}; spreadsheet links found={len(candidate_spreadsheets)}."
        )
        raise InvestorClassDiscoveryError(
            message,
            landing_url=LANDING_URL,
            total_links=len(all_links),
            spreadsheet_links=candidate_spreadsheets[:10],
        )

    logger.info("Discovered investor class links: %s", sorted(links))
    return links


def _discover_xls_links() -> dict[str, str]:
    """Backward-compatible wrapper for the old helper name."""

    return _discover_allotment_links()


def _normalize_column_name(col: str) -> str:
    """Standardize column names from the messy Excel headers.

    The Treasury .xls files have multi-line column headers with newlines
    embedded in cell values, e.g. 'Issue \\n date', 'Dealers \\n and \\n brokers'.
    """
    col = str(col).strip().lower()
    # Collapse all whitespace (including embedded newlines) to single space
    col = re.sub(r"\s+", " ", col)

    # Match against known patterns
    patterns = [
        (r"issue.*date", "issue_date"),
        (r"security.*type", "security_type"),
        (r"coupon.*rate|spread", "coupon_rate"),
        (r"cusip", "cusip"),
        (r"maturity.*date", "maturity_date"),
        (r"total.*issue", "total_issue_amount"),
        (r"federal.*reserve|soma", "fed_reserve_share"),
        (r"depository.*institution", "depository_share"),
        (r"individual", "individuals_share"),
        (r"dealer.*broker", "dealer_share"),
        (r"pension.*retire|ins.*co", "pension_insurance_share"),
        (r"investment.*fund", "investment_funds_share"),
        (r"foreign.*international", "foreign_share"),
        (r"other", "other_share"),
        (r"auction.*high.*rate|high.*rate", "auction_high_rate"),
    ]

    for pattern, name in patterns:
        if re.search(pattern, col):
            return name

    # Fallback: clean up and use as-is
    col = re.sub(r"[^a-z0-9 ]", "", col)
    col = re.sub(r"\s+", "_", col).strip("_")
    return col


def _read_allotment_workbook(url: str) -> pd.DataFrame:
    """Download and read a single Treasury allotment workbook.

    These files have 2 title rows, then column headers on row 3 (0-indexed row 2),
    followed by data rows.
    """

    logger.info("Downloading: %s", url)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    suffix = Path(urlparse(url).path).suffix.lower()
    engine = "xlrd" if suffix == ".xls" else None
    workbook = BytesIO(resp.content)
    # Read raw rows to find the actual column header row.
    df_raw = pd.read_excel(workbook, engine=engine, header=None)
    header_row = None
    for idx in range(min(10, len(df_raw))):
        row_text = " ".join(str(v).lower() for v in df_raw.iloc[idx] if pd.notna(v))
        if "issue" in row_text and "cusip" in row_text:
            header_row = idx
            break

    if header_row is None:
        header_row = 2  # fallback

    workbook.seek(0)
    df = pd.read_excel(workbook, engine=engine, header=header_row)

    # Drop rows that are all NaN (spacer rows in the Excel)
    df = df.dropna(how="all")

    # Normalize columns
    df.columns = [_normalize_column_name(c) for c in df.columns]
    return df


def _parse_allotment_df(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize an allotment dataframe to our schema.

    Treasury files report allotment amounts in billions of dollars, not shares.
    We convert to fractions by dividing each category by total_issue_amount.
    """
    df = df.copy()

    # Convert issue_date
    if "issue_date" in df.columns:
        df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")

    # Category columns that contain dollar amounts
    category_cols = [
        "dealer_share", "investment_funds_share", "foreign_share",
        "depository_share", "other_share", "fed_reserve_share",
        "individuals_share", "pension_insurance_share",
    ]

    for col in category_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert dollar amounts to fractions using total_issue_amount
    if "total_issue_amount" in df.columns:
        total = pd.to_numeric(df["total_issue_amount"], errors="coerce")
        total_safe = total.replace({0: pd.NA})
        for col in category_cols:
            if col in df.columns:
                df[col] = (df[col] / total_safe).clip(lower=0, upper=1)

    # Ensure we have the standard share columns
    for expected in ["dealer_share", "investment_funds_share", "foreign_share",
                     "depository_share", "other_share"]:
        if expected not in df.columns:
            df[expected] = None

    # Merge minor categories into other_share
    minor_cats = ["individuals_share", "pension_insurance_share", "fed_reserve_share"]
    minor_sum = sum(
        df[c].fillna(0) for c in minor_cats if c in df.columns
    )
    df["other_share"] = df["other_share"].fillna(0) + minor_sum

    # Standardize security_type — Treasury files use e.g. "2-Year Note", "10-Year Bond"
    # Bills file has "security_term" instead of "security_type"
    def _map_sec_type(raw: str) -> str:
        raw = str(raw).strip().lower()
        if raw == "nan" or not raw:
            return ""
        if "bill" in raw or "cmb" in raw:
            return "Bill"
        if "bond" in raw:
            return "Bond"
        if "tip" in raw:
            return "TIPS"
        if "frn" in raw or "floating" in raw:
            return "FRN"
        if "note" in raw:
            return "Note"
        return raw.title()

    if "security_type" in df.columns:
        df["security_type"] = df["security_type"].apply(_map_sec_type)
    elif "security_term" in df.columns:
        df["security_type"] = df["security_term"].apply(_map_sec_type)
    else:
        df["security_type"] = ""

    # Fill remaining blanks — if all entries in a file are Bills
    # (Bills-only file has no security_type column)
    blank_mask = df["security_type"].isin(["", "Nan"]) | df["security_type"].isna()
    if blank_mask.any() and "security_term" in df.columns:
        df.loc[blank_mask, "security_type"] = df.loc[blank_mask, "security_term"].apply(
            _map_sec_type
        )

    return df


def fetch_investor_class_allotments(
    output_dir: Path,
    start_date: str = "2010-01-01",
    include_historical: bool = False,
) -> Path:
    """Fetch investor class auction allotment data from Treasury.gov.

    Parameters
    ----------
    output_dir : Path
        Directory where the CSV and manifest will be written.
    start_date : str
        Earliest issue_date to include (YYYY-MM-DD).
    include_historical : bool
        If True, also fetch the pre-2009 historical files.

    Returns
    -------
    Path
        Path to the written CSV file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    links = _discover_allotment_links()
    if not links:
        raise InvestorClassDiscoveryError(
            "Could not discover any Treasury investor-class allotment workbooks.",
            landing_url=LANDING_URL,
            total_links=0,
            spreadsheet_links=[],
        )

    frames = []
    for key in ["coupons", "bills"]:
        if key in links:
            df = _read_allotment_workbook(links[key])
            df = _parse_allotment_df(df)
            frames.append(df)

    if include_historical:
        for key in ["hist_coupons", "hist_bills"]:
            if key in links:
                df = _read_allotment_workbook(links[key])
                df = _parse_allotment_df(df)
                frames.append(df)

    if not frames:
        raise RuntimeError("No allotment data files could be downloaded.")

    combined = pd.concat(frames, ignore_index=True)

    # Filter by start date
    if "issue_date" in combined.columns:
        combined["issue_date"] = pd.to_datetime(combined["issue_date"], errors="coerce")
        combined = combined[combined["issue_date"] >= start_date].copy()

    # Deduplicate by (issue_date, cusip)
    if "cusip" in combined.columns and "issue_date" in combined.columns:
        combined = combined.drop_duplicates(subset=["issue_date", "cusip"], keep="last")

    # Keep only the columns our schema needs
    keep_cols = [
        "issue_date", "security_type", "cusip", "total_issue_amount",
        "dealer_share", "investment_funds_share", "foreign_share",
        "depository_share", "other_share",
    ]
    available = [c for c in keep_cols if c in combined.columns]
    combined = combined[available].sort_values("issue_date").reset_index(drop=True)

    csv_path = output_dir / "investor_class_allotments.csv"
    combined.to_csv(csv_path, index=False)

    write_manifest(
        output_dir / "investor_class_allotments_manifest.json",
        DownloadManifest(
            source_id="treasury_investor_class",
            source_url=LANDING_URL,
            retrieved_at_utc=datetime.now(timezone.utc).isoformat(),
            local_filename="investor_class_allotments.csv",
            parser_version="v1",
            content_type="text/csv",
            notes=f"{len(combined)} allotment records from {start_date}",
        ),
    )

    logger.info("Wrote %d rows to %s", len(combined), csv_path)
    return csv_path
