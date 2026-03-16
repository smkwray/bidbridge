"""Treasury investor class auction allotments source.

Landing page: https://home.treasury.gov/data/investor-class-auction-allotments

Files are .xls (Excel 97-2003 format) with date-stamped filenames that change
on each release. We scrape the landing page to discover current download URLs.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from ..sources.base import DownloadManifest, write_manifest

logger = logging.getLogger(__name__)

LANDING_URL = "https://home.treasury.gov/data/investor-class-auction-allotments"
FILE_BASE = "https://home.treasury.gov"


def _discover_xls_links() -> dict[str, str]:
    """Scrape the landing page to find current .xls download links.

    Returns a dict with keys like 'coupons', 'bills', 'hist_coupons', 'hist_bills'.
    """
    resp = requests.get(LANDING_URL, timeout=30)
    resp.raise_for_status()
    html = resp.text

    # Find all .xls and .xlsx links
    pattern = r'href="(/system/files/276/[^"]+\.xlsx?)"'
    matches = re.findall(pattern, html)

    links: dict[str, str] = {}
    for path in matches:
        lower = path.lower()
        if "coupon" in lower and ("2000" in lower or "2009" in lower and "jan" in lower.split("coupon")[0]):
            links["hist_coupons"] = FILE_BASE + path
        elif "bill" in lower and ("2001" in lower or "2009" in lower and "aug" in lower.split("bill")[0]):
            links["hist_bills"] = FILE_BASE + path
        elif "coupon" in lower:
            links["coupons"] = FILE_BASE + path
        elif "bill" in lower:
            links["bills"] = FILE_BASE + path

    logger.info("Discovered investor class links: %s", list(links.keys()))
    return links


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


def _read_xls_file(url: str) -> pd.DataFrame:
    """Download and read a single .xls file from Treasury.

    These files have 2 title rows, then column headers on row 3 (0-indexed row 2),
    followed by data rows.
    """
    import tempfile

    logger.info("Downloading: %s", url)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    with tempfile.NamedTemporaryFile(suffix=".xls", delete=False) as tmp:
        tmp.write(resp.content)
        tmp_path = tmp.name

    try:
        # Read raw to find header row (look for a row containing "Issue" and "Cusip")
        df_raw = pd.read_excel(tmp_path, engine="xlrd", header=None)
        header_row = None
        for idx in range(min(10, len(df_raw))):
            row_text = " ".join(str(v).lower() for v in df_raw.iloc[idx] if pd.notna(v))
            if "issue" in row_text and "cusip" in row_text:
                header_row = idx
                break

        if header_row is None:
            header_row = 2  # fallback

        df = pd.read_excel(tmp_path, engine="xlrd", header=header_row)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

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

    links = _discover_xls_links()
    if not links:
        # Count all links on the page to aid debugging
        all_links = re.findall(r'href="([^"]+)"', requests.get(LANDING_URL, timeout=30).text)
        raise RuntimeError(
            "Could not discover any investor class .xls/.xlsx links from "
            f"{LANDING_URL}. The page layout may have changed. "
            f"Total links found on page: {len(all_links)}."
        )

    frames = []
    for key in ["coupons", "bills"]:
        if key in links:
            df = _read_xls_file(links[key])
            df = _parse_allotment_df(df)
            frames.append(df)

    if include_historical:
        for key in ["hist_coupons", "hist_bills"]:
            if key in links:
                df = _read_xls_file(links[key])
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
