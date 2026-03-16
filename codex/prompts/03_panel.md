# M2 — Panel builder and harmonization

You are building the first analysis-ready auction-week dataset.

## Read first

- `docs/panel_spec.md`
- `docs/empirical_design.md`
- `docs/architecture.md`

## Goal

Build standardized tables and a reproducible auction-week panel.

## Required work

1. Standardize date fields:
   - announcement date
   - auction date
   - issue date
   - maturity date
   - week start / week end
2. Standardize security groups:
   - bills
   - nominal coupons
   - FRNs
   - TIPS
3. Create weighted auction metrics using awarded amount weights.
4. Merge in investor-class shares.
5. Merge in dealer inventory and financing data.
6. Add optional SOMA and H.8 overlays if available.
7. Export a stable panel file and a schema summary.

## Tests

- no duplicate week keys,
- weighted metrics behave as expected,
- share columns sum plausibly,
- missingness is logged, not ignored.

## Output

A first real `auction_week_panel.csv` or parquet with the columns documented in `docs/panel_spec.md`.
