# BidBridge

**[Live Site](https://smkwray.github.io/bidbridge/)**

Do primary dealers act as short-run balance-sheet bridges when Treasury supply exceeds end-investor demand?

BidBridge builds an 846-week auction-week panel (2010-2026) from five public federal data sources, identifies supply shocks from pre-auction announcements, and traces their impact on dealer inventories using local projections with HAC inference.

---

## Key Findings

### Immediate dealer absorption of supply shocks

When Treasury announced supply exceeds its expanding historical 75th percentile, primary dealers accumulate **+$7.3 billion** in net Treasury inventory in the same week (p < 0.001, Newey-West HAC). The impulse response is front-loaded: most of the absorption happens at horizon 0, with partial reversal over subsequent weeks as dealers distribute holdings to end-investors through the secondary market.

### Quantitative tightening amplifies the bridge effect 2.4x

During Fed balance-sheet runoff periods (QT1: Oct 2017--Sep 2019, QT2: Jun 2022--present), the dealer inventory response to a supply shock is **+$12.2 billion** (p < 0.001) vs **+$5.0 billion** outside QT (p = 0.02). When the Fed is no longer absorbing new issuance through reinvestment, dealers must warehouse supply that would otherwise flow onto the Fed's balance sheet. Identified via shock x QT interaction on the full contiguous panel with the total effect computed via the delta method.

### Supply and dealer share are inversely correlated

Weekly supply volume and dealer allotment share have a correlation of **r = -0.82**. This inverts the naive hypothesis: bigger auction weeks bring in more institutional and foreign investors, so dealers take a smaller *percentage* of allotments -- even as they absorb more in *absolute* dollar terms. Dealer share has declined secularly from 74% (2013) to 36% (2026), reflecting the structural growth of investment fund and foreign participation in Treasury auctions.

### Maturity gradient in dealer absorption

Dealer absorption follows a monotonic maturity gradient: **62% of Bills**, 38% of short coupons (2--3Y), 30% of belly coupons (5--7Y), 27% of long bonds (10--30Y), and just 24% of TIPS. This is consistent with dealers acting as short-duration warehouses with high turnover -- they absorb the most where secondary market liquidity is deepest and holding periods are shortest. Confirmed by maturity-bucket panel FE with bucket and week fixed effects on a balanced 6 x 675 grid.

### Refunding weeks drive concentrated inventory accumulation

Quarterly refunding weeks (identified by the presence of both a 10-year Note and 30-year Bond auction) show an average inventory change of **+$10.4 billion** vs **-$1.8 billion** in ordinary weeks (p < 0.001, Welch's t-test). Refunding weeks carry 39% higher supply volume but weaker demand metrics: bid-to-cover of 2.99 vs 3.21 (p < 0.001). Auction tails show no significant difference (p = 0.30), suggesting price concessions are absorbed through volume rather than pricing.

### Bridge episodes cluster in stress regimes

85 bridge episodes are identified using a backward-looking classification: heavy supply (rolling 52-week median), positive inventory accumulation, and a z-score > 1 on a trailing 13-week window. These cluster during **risk-off periods** (bridge rate 17.0% vs 11.2% baseline) defined by elevated auction tails, and during **QT periods** (12.0%). The extended OLS regression (R² = 0.176, n = 673) shows that each additional $1 billion of Fed purchases (SOMA) reduces dealer inventory by $82 million (p < 0.001), while each year of secular trend reduces average inventory change by $3.4 billion -- dealers are becoming more efficient distributors over time.

## Quick Start

```bash
# Create external venv (never inside the repo)
uv venv ~/venvs/bidbridge --python 3.11

# Install
~/venvs/bidbridge/bin/pip install -e ".[dev]"

# Full pipeline: fetch data, build panel, generate analysis
bidbridge -v run-all

# Or step by step:
bidbridge fetch              # Download 5 public data sources
bidbridge build-panel        # Build 846-week panel + maturity panels
bidbridge analyze            # Generate figures, tables, regressions
bidbridge lp                 # Local projection impulse responses
bidbridge panel-fe           # Maturity-bucket panel fixed effects
bidbridge persistence        # Inventory persistence analysis
bidbridge stress             # Stress regime analysis

# Incremental refresh (skip sources fetched within 1 day)
bidbridge update --max-age 1
```

## Data Sources

All data is publicly available from U.S. federal agencies. No API keys required.

| Source | Provider | Frequency | Records | What It Provides |
|--------|----------|-----------|---------|-----------------|
| **Treasury Auctions** | [FiscalData API](https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/) | Per auction | 5,524 | Offering amounts, bid-to-cover, yields, bidder breakdowns by CUSIP |
| **Primary Dealer Stats** | [NY Fed Markets API](https://www.newyorkfed.org/markets/counterparties/primary-dealers-statistics) | Weekly | 675 | Dealer Treasury positions by maturity bucket, repo financing |
| **Investor Class** | [Treasury.gov](https://home.treasury.gov/data/investor-class-auction-allotments) | Semi-monthly | 5,504 | Allotment shares: dealers, investment funds, foreign, depository |
| **SOMA Holdings** | [NY Fed Markets API](https://www.newyorkfed.org/markets/soma-holdings) | Weekly | 845 | Fed Treasury holdings -- the QE/QT balance-sheet backdrop |
| **H.8 Bank Securities** | [FRED](https://fred.stlouisfed.org/) | Weekly | 844 | Commercial bank Treasury/agency holdings overlay |

The pipeline merges these into a panel using CUSIP + issue-date joins, with SOMA and H.8 lagged one week to prevent look-ahead bias.

## Methods

### Local Projections (main specification)

Jorda-style LPs estimate the cumulative dealer inventory response to ex ante supply shocks:

```
cum_inv_change(t,t+h) = alpha + beta * shock(t) + theta * shock(t) * soft_demand(t-1) + controls(t-1) + u
```

- **Shock**: Announced supply > expanding-window p75 (pre-auction only, no outcome contamination)
- **Controls**: All lagged one week (supply, dealer share, trend, SOMA change)
- **Inference**: Newey-West HAC via statsmodels, bandwidth = h+1
- **Regime effects**: Shock x QT interaction, total QT effect via delta method
- **QT periods**: Announcement-dated (Oct 2017-Sep 2019, Jun 2022-present)

### Panel Fixed Effects

Maturity-bucket panel with bucket and week FE on a balanced 6-bucket x 675-week grid:

```
delta_position(b,t) = alpha(b) + tau(t) + beta * supply(b,t) + theta * supply * soft_demand(b,t-1) + epsilon
```

Buckets: Bills, Short (2-3Y), Belly (5-7Y), Long (10-30Y), TIPS, FRN. Driscoll-Kraay SEs as robustness.

### Bridge Episode Detection

Backward-looking classification using only past data:
1. Heavy supply: awarded amount > 52-week rolling median
2. Positive dealer inventory accumulation
3. Unusually large: z-score > 1 on trailing 13-week window

## CLI Reference

| Command | Description |
|---------|-------------|
| `bidbridge fetch` | Fetch all data sources (incremental with `--max-age`) |
| `bidbridge build-panel` | Build aggregate + maturity panels from raw data |
| `bidbridge analyze` | Generate all figures, tables, and regressions |
| `bidbridge lp` | Local projection impulse responses |
| `bidbridge panel-fe` | Maturity-bucket panel fixed-effects regressions |
| `bidbridge persistence` | Inventory persistence and half-life analysis |
| `bidbridge stress` | Stress regime analysis (QT, TGA, risk-off) |
| `bidbridge update` | Incremental refresh + rebuild |
| `bidbridge run-all` | Full pipeline: fetch + build + analyze |
| `bidbridge doctor` | Verify repo structure |
| `bidbridge list-sources` | Print source registry |
| `bidbridge demo-data` | Generate synthetic demo data |

## Project Structure

```
bidbridge/                    Python package
  cli.py                      15 CLI commands
  data/
    pipeline.py               fetch_all(), build_panel(), incremental refresh
    sources/                  5 fetcher modules (treasury_auctions, nyfed_pd, etc.)
  features/
    auction_week.py           Weekly panel builder with CUSIP-based merge
    bridge_metrics.py         Bridge episode detection (rolling z-score)
    maturity_panel.py         Maturity-bucket panel + wide pivot
    stress_flags.py           QT, TGA, risk-off regime flags
  analysis/
    local_projections.py      Jorda LP with ex ante shocks, HAC, regime interaction
    panel_fe.py               Maturity-bucket FE with linearmodels
    regressions.py            OLS + HC1, interaction, subsample splits
    persistence.py            Detrended half-life, ACF/PACF, impulse response
    outputs.py                Deterministic figure/table generation

site/                         Static research site (Plotly.js, no build step)
configs/                      sources.yml, study.yml
data/                         raw / processed (gitignored)
outputs/                      figures/ tables/ (gitignored)
tests/                        29 tests (unit + integration + edge cases)
notebooks/                    Exploration notebook
scripts/                      Maturity analysis, verification
```

## Tests

```bash
PYTHONDONTWRITEBYTECODE=1 ~/venvs/bidbridge/bin/python -B -m pytest tests/ -x
```

29 tests covering data fetchers, merge correctness (reopenings), NaN handling, bridge metric edge cases, stress flag calendar logic, and panel building.

## Audit History

The codebase has been through **4 independent audit passes** addressing:
- CUSIP reopening merge correctness
- Forward-looking SOMA/H.8 timing (lagged 1 week)
- NaN-as-zero masking in inventory and weighted averages
- Endogenous LP treatment (replaced with ex ante supply shock)
- Look-ahead thresholds (replaced with rolling/expanding windows)
- Announcement-dated QT regimes (replacing realized SOMA declines)
- Balanced panel FE (zero-filled non-auction weeks)

## License

MIT
