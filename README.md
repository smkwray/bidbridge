# BidBridge

BidBridge measures the weekly association between announced US Treasury auction offering amounts and changes in aggregate primary-dealer Treasury positions by remaining-maturity bucket. It downloads public Treasury, New York Fed, and Federal Reserve series, builds weekly and maturity-bucket panels, and writes two-way fixed-effects regressions, local projections, persistence and stress summaries, figures, and static site data.

## Data

All primary and context sources are public and require no API keys:

- US Treasury FiscalData (`auctions_query` API): auction offering amounts, dates, security types, bid-to-cover ratios, yields or rates, and upcoming auction notices.
- NY Fed Primary Dealer Statistics API: weekly aggregate dealer Treasury positions by remaining-maturity band (in USD millions), transactions, and financing.
- US Treasury Investor Class Auction Allotments (`.xls` releases): initial auction allotments by investor class.
- NY Fed System Open Market Account (SOMA) summary API: weekly Federal Reserve Treasury holdings, lagged one week in the weekly panel.
- The Federal Reserve's H.8 release on commercial bank balance sheets, via FRED (Federal Reserve Economic Data, St. Louis Fed): weekly seasonally adjusted and non-seasonally adjusted commercial-bank Treasury and agency securities, lagged one week in the weekly panel.

Auction records join to investor-class allotments on CUSIP (the standard security identifier) and issue date. `configs/sources.yml` also lists Treasury aggregates from the Financial Industry Regulatory Authority's Trade Reporting and Compliance Engine (FINRA TRACE) as an optional overlay that the default pipeline does not fetch.

## Install

Python 3.11 or later is required. From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

## Usage

Check repository layout and configured sources:

```bash
bidbridge doctor
bidbridge list-sources
```

Run the full pipeline (fetch data, build panels, audit inputs, estimate models, and write tables, figures, and site JSON):

```bash
bidbridge run-all
```

Pass `--start-date YYYY-MM-DD` (default `2010-01-01`) to change the fetch start date. Individual stages can be run separately:

```bash
bidbridge fetch
bidbridge build-panel
bidbridge analyze
bidbridge panel-fe
bidbridge lp
bidbridge persistence
bidbridge stress
```

`bidbridge update --max-age 1.0` refreshes only sources older than the given number of days and rebuilds the panel. `bidbridge panel-fe` estimates both Monday-start and Thursday-start maturity-bucket fixed-effects models from the detailed New York Fed coupon maturity bands that `bidbridge fetch` downloads. Run `bidbridge --help` for all commands, including synthetic demo targets (`demo-data`, `demo-panel`). Sample settings live in `configs/study.yml`.

## Output

Downloaded data and generated outputs under `data/` and `outputs/` are ignored by Git. The static site under `site/` is tracked, including the JSON exports in `site/data/` that `bidbridge run-all` rewrites.

| Path | Description |
| --- | --- |
| `data/processed/auction_week_panel.csv` | Weekly aggregate Treasury auction supply and primary-dealer balance-sheet panel (auctions in USD, dealer positions in USD millions) |
| `data/processed/maturity_bucket_panel.csv` | Weekly maturity-bucket panel used for two-way fixed-effects estimation |
| `outputs/tables/` | Fixed-effects, local-projection, placebo, persistence, stress, audit, and upcoming-supply CSVs |
| `outputs/figures/` | Static PNG/SVG charts |
| `site/data/` | JSON exports for the static site under `site/` |

## Published estimate and limits

The archived sample runs from April 8, 2013 to March 9, 2026 (3,958 maturity-bucket-week observations across 6 buckets, Monday-start weeks, bucket and week fixed effects, Bartlett Driscoll-Kraay standard errors with bandwidth 6). In it, $1 billion of announced offering amounts is associated with a $9.45 million contemporaneous increase in aggregate primary-dealer Treasury positions (95% confidence interval: $2.51 million to $16.39 million); with Thursday-start weeks the estimate is $14.55 million ($6.76 million to $22.35 million).

A fresh `bidbridge run-all` downloads current upstream releases, so its sample dates and estimates can differ from the archived ones unless inputs are pinned.

- This is an aggregate short-run warehousing association. It does not identify final holders, customer trades, inventory financing costs, deposit flows, or a causal supply effect.
- Investor-class allotment shares measure initial auction allocation, not final ownership.
- NY Fed dealer positions are Wednesday snapshots grouped by remaining maturity, while auctions are grouped by original maturity and settle one business day after the auction (T+1); under Monday-start weeks, Thursday and Friday auctions can fall after the Wednesday snapshot.
- Binary high-supply local projections define high-supply weeks using an expanding 75th-percentile threshold that includes the current week, and two of five non-overlapping lead placebo tests reject zero at 5%. Local projections and quantitative-tightening splits are descriptive only.

## Tests

Run the offline test suite and linter:

```bash
python -B -m pytest -q -m 'not network'
ruff check .
```

Run live upstream source tests separately:

```bash
python -B -m pytest -q -m network
```

## License

MIT. See [LICENSE](LICENSE).
