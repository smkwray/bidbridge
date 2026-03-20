# Architecture

## Data flow

```text
fetch  -->  harmonize  -->  panel + maturity panels  -->  audit + analysis  -->  site data + manifest
```

1. **Fetch** (`bidbridge fetch`): Each source module in `bidbridge/data/sources/` calls its public API, writes a CSV, and writes a sidecar JSON manifest with the source page, fetch timestamp, and retrieval metadata into `data/raw/<source>/`.
2. **Harmonize** (`bidbridge/data/pipeline.py`): Raw CSVs are loaded and filtered to the columns the panel needs. Auction records get refunding-week tagging, investor-class data is joined by CUSIP, and dealer stats are trimmed to inventory and financing columns.
3. **Panel construction** (`bidbridge/features/auction_week.py`, `bidbridge/features/maturity_panel.py`): Auctions are grouped into Monday-start weeks by default, with a Thursday-start option for FE robustness. Within each week the code computes award totals, weighted bid-to-cover, weighted tail, and award-weighted investor-class shares. Dealer stats, SOMA, and H.8 are merged on `week_start`.
4. **Audit** (`bidbridge/analysis/data_audit.py`): `run-all` writes CSV and JSON audit artifacts covering unmatched joins, FE eligibility, missing granular coupon-band coverage, and financing forward-fill prevalence.
5. **Analysis** (`bidbridge/analysis/outputs.py`, `bidbridge/analysis/local_projections.py`, `bidbridge/analysis/panel_fe.py`, `bidbridge/analysis/persistence.py`, `bidbridge/features/stress_flags.py`): Figures and tables are written for descriptive outputs, LPs, LP placebo / falsification checks, headline FE plus Thursday-start robustness, persistence, and stress.
6. **Site data** (`bidbridge/analysis/site_data.py`): A site JSON payload is generated for the static site, including LP results, stress summaries, the upcoming pressure monitor, and the rendered data-source registry.
7. **Manifest** (`bidbridge/run_manifest.py`): `run-all` writes a single run manifest with git SHA, config snapshot, raw inputs, processed outputs, analysis outputs, audit outputs, and extension outputs.

## Module map

```text
bidbridge/
  data/
    sources/
      base.py                    DownloadManifest dataclass, write_manifest()
      treasury_auctions.py       FiscalData auctions_query API
      nyfed_pd.py                NY Fed primary dealer stats API
      treasury_investor_class.py Treasury.gov investor-class allotments scraper
      soma.py                    NY Fed SOMA summary API
      h8.py                      FRED CSV endpoint for H.8 series
      trace_treasury.py          Stub — not yet implemented
    pipeline.py                  fetch_all(), build_panel(), harmonize helpers
    registry.py                  SourceRecord model, config-driven registry
  features/
    auction_week.py              week anchoring helpers, weighted_average()
    bridge_metrics.py            add_bridge_metrics(), safe_divide()
    maturity_panel.py            maturity-bucket panel and FE alignment helpers
  analysis/
    outputs.py                   run_all_analysis(), individual figure/table generators
    local_projections.py         LP baseline and placebo outputs
    panel_fe.py                  FE headline export, Thursday-start robustness, DK inference
    persistence.py               persistence figures and summary table
    data_audit.py                post-build audit summaries
    pressure_monitor.py          upcoming-auction pressure scores
    site_data.py                 JSON payload for the static site
```

## Key design decisions

### CUSIP-based merge

Auctions and investor-class allotments are joined on `cusip` when both sides have it. This avoids false matches that can arise from joining on `(issue_date, security_type)` alone because reopenings share dates with original issues. A fallback to `(issue_date, security_type)` exists for demo data that lacks CUSIPs.

### Z-score bridge episodes

A bridge episode is defined as a week where:
1. Supply is above-median (`heavy_supply`),
2. Dealer inventory increased (`inventory_change > 0`), and
3. The increase is unusually large relative to a trailing 13-week window (`inventory_change_zscore > 1`).

This three-part filter avoids flagging routine inventory swings during low-supply weeks.

### Forward-fill financing

The NY Fed suppresses repo/reverse-repo values with `"*"` when too few dealers report. The pipeline applies last-observation-carried-forward on `pd_repo_treasury` and `pd_reverse_repo_treasury`, then recomputes `pd_financing_usage`. This preserves level information while clearly marking the limitation in the manifest notes.

### Investor-class workbook discovery

Treasury investor-class allotments are still sourced from a changing landing page, but the fetcher now parses all anchor tags, classifies bill versus coupon workbooks from filename and link text, and supports both `.xls` and `.xlsx`. When discovery fails, it raises a structured error with anchor counts instead of silently returning no files.

### Week anchoring

All weekly data is aligned to ISO Monday-start weeks by default. NY Fed reports (Wednesday as-of) and H.8 observations (Wednesday end-of-week) are shifted back to the week anchor before merging. A Thursday-start alternative is exposed for the maturity-bucket FE robustness path because the Wednesday snapshot timing can attenuate week-level position changes.

### FE export

The maturity-bucket FE branch exports Driscoll-Kraay as the headline inference surface. Clustered-by-bucket results are retained as robustness outputs. Headline FE outputs are withheld when granular coupon bands are unavailable so the public table does not silently rely on proportional fallback.

### Values units

Dealer stats and SOMA values are in millions of USD. H.8 values are converted from billions to millions to match. Auction amounts are in raw dollars from the API. When computing `dealer_bridge_ratio`, awarded amounts are divided by 1e6 to align with the dealer-stats scale.

## CLI command dispatch

All commands are routed through `bidbridge/cli.py` via argparse subcommands. The `main()` function is the `[project.scripts]` entry point registered in `pyproject.toml`. `bidbridge run-all` is the canonical full reproduction path because it orchestrates fetch, panel build, audit, analysis, FE, LP placebos, persistence, stress, pressure-monitor generation, site-data writing, and the run manifest in one idempotent pass.
