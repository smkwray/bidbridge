# AGENTS.md

This repo is designed to be completed by a coding agent.

## Mission

Build a public, reproducible research repo named `bidbridge` that studies whether primary dealers temporarily warehouse Treasury supply when end-investor demand is weak.

## Non-negotiables

- Use public data only for the main pipeline.
- Prefer structured downloads, APIs, CSV, JSON, or XML over PDF scraping.
- Preserve provenance: every raw download needs a source page, fetch timestamp, and retrieval metadata.
- Keep pipelines idempotent.
- Put reusable logic in the package, not only in notebooks.
- Add tests for parsing, joins, and feature engineering.
- Use clear schema names and plain-English docs.
- Document any unverifiable assumption as a TODO, not as a silent default.
- Avoid premature macro sprawl. The first version is an auction-capacity / dealer-intermediation repo.

## Priority order

1. Foundation and source registry
2. Ingestion for priority sources
3. Harmonization and auction-week panel construction
4. Bridge metrics and descriptive analysis
5. Public-repo hardening

## Done criteria for each milestone

A milestone is only done when:

- code runs from the CLI or documented scripts,
- raw and processed outputs land in the expected directories,
- tests pass,
- docs are updated,
- any open data caveats are logged.

## Data principles

- Track source grain separately from model grain.
- Avoid merging on text labels if stable IDs or dates are available.
- Normalize date fields explicitly: announcement date, auction date, issue date, maturity date, week start, week end.
- Keep bill, coupon, FRN, TIPS, and buyback logic separable.
- Do not hide weight choices; weighted averages must name their weights.

## First target

The first serious milestone should produce a weekly panel with:

- total announced and awarded issuance,
- bill vs coupon composition,
- weighted bid-to-cover and auction tails,
- investor-class shares,
- dealer Treasury inventory and financing usage,
- optional SOMA and H.8 overlays,
- bridge metrics and event flags.

## Public-repo tone

Optimize for transparency, reproducibility, and institutional clarity.
