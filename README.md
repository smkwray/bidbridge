# BidBridge

**[Live Site](https://smkwray.github.io/bidbridge/)**

Do primary dealers act as short-run balance-sheet bridges when Treasury supply exceeds end-investor demand?

BidBridge builds an 846-week auction-week panel (2010-2026) from five public federal data sources and studies associations between announced Treasury supply and aggregate primary-dealer positions.

---

## Key Findings

### Same-week high-supply indicator

At h=0, the coefficient on a binary indicator for weeks when announced aggregate supply exceeds its expanding-window 75th percentile is **+$7.3 billion** of aggregate dealer-position change (p < 0.001, Newey-West HAC). The threshold is computed through the current week, so this is not a continuous offering-amount slope or a strictly prior-history-only shock. These aggregate data do not identify terminal holders or the transactions behind later position normalization.

### QT and non-QT point estimates

The h=0 high-supply-indicator point estimate is **+$10.9 billion** during QT and **+$5.2 billion** outside QT. The bounded association evidence does not treat this contrast as evidence of amplification or an established regime difference. It is not a structural causal estimate of Federal Reserve policy or dealer capacity.

### Supply and dealer share are inversely correlated

Weekly supply volume and dealer allotment share have a correlation of **r = -0.81**. This inverts the naive hypothesis: bigger auction weeks bring in more institutional and foreign investors, so dealers take a smaller *percentage* of allotments -- even as they absorb more in *absolute* dollar terms. Dealer share has declined secularly from 70% (2013) to 37% (2026), reflecting the structural growth of investment fund and foreign participation in Treasury auctions.

### Maturity gradient in dealer absorption

Dealer auction allotments follow a maturity gradient, with higher aggregate shares in bills than in longer coupons. The headline inventory evidence comes from a maturity-bucket panel FE with bucket and week fixed effects using directly observed NY Fed position data by remaining-maturity band. This pattern supports short-run dealer warehousing; it does not identify terminal ownership, customer identity, or holding periods.

### Refunding weeks drive concentrated inventory accumulation

Quarterly refunding weeks (identified by the presence of both a 10-year Note and 30-year Bond auction) show an average inventory change of **+$11.2 billion** vs **-$0.3 billion** in ordinary weeks (p < 0.001, Welch's t-test). Refunding weeks carry higher supply volume but weaker demand metrics: bid-to-cover of 3.02 vs 3.17 (p = 0.006). Auction tails show marginal significance (p = 0.07), suggesting price concessions are absorbed primarily through volume rather than pricing.

### Bridge episodes cluster in stress regimes

85 bridge episodes are identified using a backward-looking classification: heavy supply (rolling 52-week median), positive inventory accumulation, and a z-score > 1 on a trailing 13-week window. These cluster during **risk-off periods** (bridge rate 17.0% vs 11.2% baseline) defined by elevated auction tails, and during **QT periods** (12.2% vs 12.9% baseline). Extended OLS estimates provide descriptive associations with SOMA and time trends; they do not identify a structural policy mechanism or distribution efficiency.

## Quick Start

```bash
# Create external venv (never inside the repo)
uv venv ~/venvs/bidbridge --python 3.11

# Install
~/venvs/bidbridge/bin/pip install -e ".[dev]"

# Full reproduction path: fetch, build, audit, analysis, FE, LP placebos, pressure monitor, site data, manifest
bidbridge run-all

# Or step by step:
bidbridge fetch              # Download 5 public data sources
bidbridge build-panel        # Build 846-week panel + maturity panels
bidbridge analyze            # Generate descriptive figures, tables, regressions
bidbridge lp                 # Local projection impulse responses
bidbridge panel-fe           # Supply-only headline FE + strict-lag and Thursday robustness
bidbridge promotion          # Compact warehousing-association summary and bounded memo
bidbridge ratewall-support   # Bounded dealer-warehousing support contract for scenario consumers
bidbridge persistence        # Inventory persistence analysis
bidbridge stress             # Stress regime analysis

# Incremental refresh (skip sources fetched within 1 day)
bidbridge update --max-age 1

# Frozen reconstructed-input evidence chain (no network refetch)
python -B -m bidbridge.open07_reconstruction
python -B -m bidbridge.open07_reproduce --producer-commit <commit> --promote
python -B -m bidbridge.open07_reproduce --producer-commit <commit>
python -B -m bidbridge.open07_reconstructed_receipt --producer-commit <commit>
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

### Local Projections (supporting specification)

Jorda-style LPs estimate the cumulative dealer-position association with the binary high-supply indicator:

```
cum_inv_change(t,t+h) = alpha + beta * shock(t) + theta * shock(t) * soft_demand(t-1) + controls(t-1) + u
```

- **Shock**: Announced supply > expanding-window p75 (pre-auction only, no outcome contamination)
- **Controls**: All lagged one week (supply, dealer share, trend, SOMA change)
- **Inference**: Newey-West HAC via statsmodels, bandwidth = h+1
- **Regime surfaces**: high-supply-indicator x QT interaction and total QT point estimate; no promoted amplification claim without inference on the interaction
- **QT periods**: Announcement-dated (Oct 2017-Sep 2019, Jun 2022-Dec 2025)

### Panel Fixed Effects

Maturity-bucket panel with bucket and week FE using directly observed NY Fed position data by remaining-maturity band:

```
delta_position(b,t) = alpha(b) + tau(t) + beta * supply(b,t) + theta * supply * soft_demand(b,t-1) + epsilon
```

Buckets: Bills, Short (2-3Y), Belly (5-7Y), Long (10-30Y), TIPS, FRN. Driscoll-Kraay is the headline FE inference surface, with clustered-by-bucket results retained as robustness outputs. Headline FE outputs are withheld if granular coupon bands are unavailable.

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
| `bidbridge analyze` | Generate descriptive figures, tables, and regressions |
| `bidbridge lp` | Local projection impulse responses |
| `bidbridge panel-fe` | Supply-only maturity-bucket FE headline plus strict-lag and Thursday-start robustness (Driscoll-Kraay) |
| `bidbridge promotion` | Write the bounded dealer-warehousing association summary and memo |
| `bidbridge ratewall-support` | Write the bounded dealer-warehousing support contract |
| `bidbridge persistence` | Inventory persistence and half-life analysis |
| `bidbridge stress` | Stress regime analysis (QT, TGA, risk-off) |
| `bidbridge update` | Incremental refresh + rebuild |
| `bidbridge run-all` | Full reproduction path: fetch + build + audit + analysis + FE + LP placebos + pressure monitor + site data + manifest |
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

Deterministic tests cover week anchoring, FE gating, LP placebo outputs, audit artifacts, pressure-monitor outputs, parser logic, merge correctness, and bridge metric edge cases. Live-source fetcher tests remain separately marked with `@pytest.mark.network`.

## Audit History

The codebase has been through **5 independent audit passes** addressing:
- CUSIP reopening merge correctness
- Forward-looking SOMA/H.8 timing (lagged 1 week)
- NaN-as-zero masking in inventory and weighted averages
- Endogenous LP treatment (replaced with a pre-auction-announcement high-supply indicator)
- Look-ahead thresholds (replaced with rolling/expanding windows)
- Announcement-dated QT regimes (replacing realized SOMA declines)
- Mechanical endogeneity in panel FE (replaced proportional coupon split with directly observed NY Fed maturity bands)
- QT2 end date correction (Jun 2022--Dec 2025, was open-ended)
- Week-alignment documentation (Monday-start vs Wednesday as-of attenuation bias)

## License

MIT
