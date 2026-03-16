# Architecture

## Data flow

```
fetch  -->  harmonize  -->  panel  -->  metrics  -->  analysis
```

1. **Fetch** (`bidbridge fetch`): Each source module in `bidbridge/data/sources/` calls its public API, writes a CSV and a sidecar JSON manifest into `data/raw/<source>/`.
2. **Harmonize** (`bidbridge/data/pipeline.py`): Raw CSVs are loaded and filtered to the columns the panel needs. Auction records get refunding-week tagging, investor-class data is joined by CUSIP, and dealer stats are trimmed to inventory and financing columns.
3. **Panel** (`bidbridge/features/auction_week.py`): Auctions are grouped into Monday-start weeks. Within each week the code computes award totals, weighted bid-to-cover, weighted tail, and award-weighted investor-class shares. Dealer stats, SOMA, and H.8 are merged on `week_start`.
4. **Metrics** (`bidbridge/features/bridge_metrics.py`): The panel is enriched with `inventory_change`, `dealer_bridge_ratio`, `financing_intensity`, `inventory_change_zscore`, and the `bridge_episode` flag.
5. **Analysis** (`bidbridge/analysis/outputs.py`): Figures (timeseries, scatter, event studies, SOMA comparison) and tables (summary stats, annual summary, bridge-episode summary, demo regression) are written to `outputs/`.

## Module map

```text
bidbridge/
  data/
    sources/
      base.py                    DownloadManifest dataclass, write_manifest()
      treasury_auctions.py       FiscalData auctions_query API
      nyfed_pd.py                NY Fed primary dealer stats API
      treasury_investor_class.py Treasury.gov .xls scraper
      soma.py                    NY Fed SOMA summary API
      h8.py                      FRED CSV endpoint for H.8 series
      trace_treasury.py          Stub — not yet implemented
    pipeline.py                  fetch_all(), build_panel(), harmonize helpers
    registry.py                  SourceRecord model, config-driven registry
  features/
    auction_week.py              build_weekly_panel(), monday_start(), weighted_average()
    bridge_metrics.py            add_bridge_metrics(), safe_divide()
  analysis/
    outputs.py                   run_all_analysis(), individual figure/table generators
    regressions.py               run_demo_bridge_regression() — placeholder OLS
    event_studies.py             Event-study helper functions
```

## Key design decisions

### CUSIP-based merge

Auctions and investor-class allotments are joined on `cusip` when both sides have it. This avoids false matches that can arise from joining on `(issue_date, security_type)` alone (reopenings share dates with original issues). A fallback to `(issue_date, security_type)` exists for demo data that lacks CUSIPs.

### Z-score bridge episodes

A bridge episode is defined as a week where:
1. Supply is above-median (`heavy_supply`),
2. Dealer inventory increased (`inventory_change > 0`), and
3. The increase is unusually large relative to a trailing 13-week window (`inventory_change_zscore > 1`).

This three-part filter avoids flagging routine inventory swings during low-supply weeks.

### Forward-fill financing

The NY Fed suppresses repo/reverse-repo values with `"*"` when too few dealers report (increasingly common from 2022 onward, affecting 50--87% of weeks by 2025). The pipeline applies last-observation-carried-forward on `pd_repo_treasury` and `pd_reverse_repo_treasury`, then recomputes `pd_financing_usage`. This preserves level information while clearly marking the limitation in the manifest notes.

### Monday-start weeks

All weekly data is aligned to ISO Monday-start weeks. NY Fed reports (Wednesday as-of) and H.8 observations (Wednesday end-of-week) are shifted back to the Monday of their reporting week before merging.

### Values units

Dealer stats and SOMA values are in millions of USD. H.8 values are converted from billions to millions to match. Auction amounts are in raw dollars from the API. When computing `dealer_bridge_ratio`, awarded amounts are divided by 1e6 to align with the dealer-stats scale.

## CLI command dispatch

All commands are routed through `bidbridge/cli.py` via argparse subcommands. The `main()` function is the `[project.scripts]` entry point registered in `pyproject.toml`.
