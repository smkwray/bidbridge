# BidBridge

BidBridge measures the association between announced Treasury supply and
aggregate primary-dealer positions. It combines auction offering amounts with
New York Fed dealer positions by remaining-maturity bucket. The
[research site](https://smkwray.github.io/bidbridge/) presents the estimates and
their interpretation limits.

## Result and limits

The headline specification estimates a contemporaneous increase of $9.4536
million in aggregate dealer positions per $1 billion of announced offering
amounts, with a 95% confidence interval of [$2.5134 million, $16.3937 million].
It uses 3,958 maturity-bucket-week observations, Monday-aligned weeks, bucket and
week fixed effects, and Bartlett Driscoll–Kraay inference with bandwidth 6.
The strict-lag confidence interval includes zero.

This is an aggregate warehousing association. It does not identify final holders,
customer trades, inventory costs, deposit effects, or a structural causal supply
response. Dealer allotment shares describe initial auction allocation, not who
ultimately absorbs marginal supply.

Binary high-supply local projections are descriptive diagnostics: their expanding
75th-percentile threshold includes the current week, and two of five
non-overlapping lead tests reject zero. Quantitative-tightening splits remain
descriptive point estimates, without an established amplification effect.

## Install and validate

Python 3.11 or later is required. From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
python -B -m pytest -q -m 'not network'
ruff check .
bidbridge doctor
```

Live acquisition tests run separately with `python -B -m pytest -q -m network`.
A passing offline suite does not verify current upstream availability.

## Sources

| Source | Use |
|---|---|
| Treasury FiscalData auctions | Offering amounts, dates, security types, and bidder results |
| NY Fed primary dealer statistics | Weekly positions, financing, and transactions |
| Treasury investor-class allotments | Initial auction buyer shares |
| NY Fed System Open Market Account (SOMA) | Federal Reserve holdings context |
| Federal Reserve H.8 | Bank Treasury and agency securities context |

The pipeline joins auctions by security identifier and issue date and lags SOMA
and H.8 context by one week. Auction maturity and dealer remaining-maturity
bands retain their distinct definitions. See the
[source guide](docs/source_guide.md) and [empirical design](docs/empirical_design.md).

## Build

The full workflow acquires data and replaces local derived outputs:

```bash
bidbridge run-all
```

Individual stages support inspection and selective rebuilding:

```bash
bidbridge fetch
bidbridge build-panel
bidbridge analyze
bidbridge panel-fe
bidbridge lp
```

`panel-fe` produces the supply-only headline and strict-lag and week-alignment
checks. It withholds headline outputs when granular coupon bands are unavailable.
`promotion` writes a bounded association summary; `ratewall-support` writes an
assumption-limited scenario-consumption contract. These commands do not confer
causal authority on the estimates. Run `bidbridge --help` for all commands.

The retained numerical result is tied to its own input and output receipt.
A fresh download or a successful software test does not reproduce that result
without its specified inputs, sample, and estimator settings.

## Files and outputs

- `bidbridge/`: acquisition, panel construction, analysis, and CLI.
- `configs/`: source definitions and study settings.
- `tests/`: synthetic tests and explicitly marked network tests.
- `data/` and `outputs/`: local source data and generated analysis; ignored by Git.
- `site/`: curated static research-site assets.

License: MIT.
