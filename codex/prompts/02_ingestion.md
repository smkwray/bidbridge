# M1 — Ingestion for priority sources

You are implementing the first real public-data ingestion layer.

## Read first

- `AGENTS.md`
- `docs/data_sources.md`
- `docs/architecture.md`
- `configs/sources.yml`

## Goal

Implement fetchers and parsers for the priority public sources.

## Sources to target

- NY Fed Primary Dealer Statistics
- Treasury Securities Auctions Data / Auction Query
- Treasury Upcoming Auctions
- Treasury Investor Class Auction Allotments
- NY Fed SOMA Holdings
- Federal Reserve H.8

## Requirements

- Use structured formats before PDF scraping.
- Store raw snapshots under `data/raw/<source_id>/`.
- Write a manifest sidecar for every download.
- Implement deterministic interim parsers into `data/interim/`.
- Add tests with mocked local fixtures where possible.
- Keep source-specific logic isolated in `bidbridge/data/sources/`.

## Deliverables

- `bidbridge fetch --source <id>`
- `bidbridge fetch --all-priority`
- `bidbridge build-interim --source <id>` or equivalent documented scripts
- parser tests and fixture files

## Important

If a source has multiple valid official endpoints, prefer the least brittle one and document the choice.
