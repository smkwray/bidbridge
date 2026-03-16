# Data sources

All five priority sources are implemented. TRACE Treasury is an optional stub.

## 1. Treasury Auctions (FiscalData)

| Field | Value |
|---|---|
| Module | `bidbridge/data/sources/treasury_auctions.py` |
| Endpoint | `https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query` |
| Frequency | Per auction (paginated JSON, sorted by `auction_date`) |
| Grain | One row per completed auction |
| Values | Dollars (raw API units) |
| Key fields | `cusip`, `auction_date`, `issue_date`, `security_type`, `instrument_group`, `offering_amount`, `awarded_amount`, `bid_to_cover`, `high_yield`, `tail_bp`, `primary_dealer_accepted`, `direct_bidder_accepted`, `indirect_bidder_accepted` |

**Known issues:**
- The API returns the literal string `"null"` for missing values (not JSON null). A `_null_safe()` helper handles this.
- Bills use `high_investment_rate` / `high_discnt_rate` instead of `high_yield`. The fetcher selects the correct field by `security_type`.
- `tail_bp` is computed as `(high_rate - avg_rate) * 100`; it is `None` when either rate is missing.
- Completeness check: rows without `bid_to_cover_ratio` are dropped (these are announced but unsettled auctions).

The module also exposes `fetch_upcoming_auctions()` for announced-but-not-settled auctions (filter: `high_yield:eq:null`).

---

## 2. NY Fed Primary Dealer Statistics

| Field | Value |
|---|---|
| Module | `bidbridge/data/sources/nyfed_pd.py` |
| Endpoint | `https://markets.newyorkfed.org/api/pd/` |
| Frequency | Weekly (Wednesday as-of date, released Thursday) |
| Grain | Aggregate across all primary dealers |
| Values | Millions of USD |
| Key fields | `pd_treasury_inventory`, `pd_bills_position`, `pd_coupon_position`, `pd_tips_position`, `pd_frn_position`, `pd_repo_treasury`, `pd_reverse_repo_treasury`, `pd_financing_usage` |

**Known issues:**
- The API organizes data by series breaks (e.g., `SBN2024`, `SBN2022`, ..., `SBP2013`). The fetcher iterates all breaks and deduplicates by `(asofdate, keyid)`.
- Suppressed values appear as `"*"` when too few dealers report. This increasingly affects repo/reverse-repo data from 2022 onward (50--87% of weeks by 2025). The pipeline applies forward-fill on these columns.
- `pd_financing_usage` = `pd_repo_treasury - pd_reverse_repo_treasury`, recomputed after forward-fill.

---

## 3. Treasury Investor Class Auction Allotments

| Field | Value |
|---|---|
| Module | `bidbridge/data/sources/treasury_investor_class.py` |
| Endpoint | `https://home.treasury.gov/data/investor-class-auction-allotments` (HTML scrape for .xls links) |
| Frequency | Monthly or semi-monthly releases |
| Grain | One row per auction issue |
| Values | Shares (fractions of `total_issue_amount`, range 0--1) |
| Key fields | `issue_date`, `cusip`, `security_type`, `dealer_share`, `investment_funds_share`, `foreign_share`, `depository_share`, `other_share` |

**Known issues:**
- Download URLs contain date-stamped filenames that change on each release. The fetcher scrapes the landing page to discover current `.xls` links.
- Files are Excel 97-2003 format (`.xls`), read with `xlrd`. Column headers span multiple rows with embedded newlines; `_normalize_column_name()` handles this.
- Raw values are dollar amounts in billions. The fetcher converts to shares by dividing each category by `total_issue_amount`.
- Minor categories (individuals, pension/insurance, Fed Reserve) are merged into `other_share`.
- Bills and coupons are in separate files with slightly different column layouts.
- Deduplication is by `(issue_date, cusip)`, keeping the last occurrence.

---

## 4. NY Fed SOMA Holdings

| Field | Value |
|---|---|
| Module | `bidbridge/data/sources/soma.py` |
| Endpoint | `https://markets.newyorkfed.org/api/soma/summary.json` |
| Frequency | Weekly |
| Grain | Aggregate by security type |
| Values | Dollars (raw API units — large numbers) |
| Key fields | `soma_bills`, `soma_notes_bonds`, `soma_tips`, `soma_frn`, `soma_treasury_total`, `soma_mbs`, `soma_agencies`, `soma_total` |

**Known issues:**
- `soma_treasury_total` is computed by summing bills + notes/bonds + TIPS + FRN (excluding MBS and agencies).
- Suppressed values appear as `"*"` (same convention as dealer stats).

---

## 5. Federal Reserve H.8 (via FRED)

| Field | Value |
|---|---|
| Module | `bidbridge/data/sources/h8.py` |
| Endpoint | `https://fred.stlouisfed.org/graph/fredgraph.csv` (public, no API key required) |
| Frequency | Weekly (Wednesday observation date) |
| Grain | Aggregate commercial bank balance sheet |
| Values | Millions of USD (converted from billions at fetch time) |
| Key fields | `bank_treasury_securities` (NSA), plus SA/NSA variants for total, MBS component, and non-MBS component |

**Known issues:**
- FRED CSV endpoint accepts multi-series requests. Six series are fetched in a single call: `TASACBW027SBOG`, `TASACBW027NBOG`, `TMBACBW027SBOG`, `TNMACBW027SBOG`, `TMBACBW027NBOG`, `TNMACBW027NBOG`.
- H.8 uses a Thursday--Wednesday reporting week. The pipeline shifts `week_start` to the Monday of that week before merging with the panel.
- Missing values appear as `"."` in the FRED CSV; pandas `na_values` handles this.

---

## 6. FINRA TRACE Treasury Aggregates (optional)

| Field | Value |
|---|---|
| Module | `bidbridge/data/sources/trace_treasury.py` |
| Endpoint | `https://www.finra.org/filing-reporting/trace/data/trace-treasury-aggregates/about` |
| Status | **Stub only** — raises `NotImplementedError` |

Not required for the core bridge analysis. Would provide secondary-market depth context if implemented.

---

## Retrieval metadata

Every fetcher writes a sidecar `*_manifest.json` alongside its CSV containing:
- `source_id`, `source_url`, `retrieved_at_utc`, `local_filename`, `content_type`, `parser_version`, `notes`
