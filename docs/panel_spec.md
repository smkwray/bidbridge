# Auction-week panel specification

This is the core table the first paper should revolve around.

## Grain

Default grain: one row per auction week.

Optional extensions:

- week x instrument group,
- week x maturity bucket,
- week x bill/coupon split.

## Required columns

### Supply block

- week_start
- week_end
- auction_count
- announced_amount_total
- awarded_amount_total
- bill_amount
- coupon_amount
- bill_share
- coupon_share
- refunding_week

### Auction quality block

- weighted_bid_to_cover
- weighted_tail_bp
- weighted_high_yield_or_rate if feasible

### Investor absorption block

- dealer_share_allotment
- investment_funds_share_allotment
- foreign_share_allotment
- depository_share_allotment
- nondealer_share

### Dealer bridge block

- pd_treasury_inventory
- pd_inventory_change
- pd_financing_usage
- financing_intensity
- dealer_bridge_ratio
- inventory_persistence_proxy

### Balance-sheet backdrop block

- soma_holdings_total or soma_change
- bank_treasury_securities_proxy
- trace_total_volume_proxy

### Flags

- heavy_supply
- weak_end_investor_absorption
- bridge_episode
- qt_period
- tga_rebuild
- risk_off_window

## Aggregation rules

- Auction metrics should be weighted by awarded amount unless documented otherwise.
- Shares should be carried in decimals on `[0,1]`.
- Event flags should be boolean columns, not encoded strings.
- First differences should be explicit and named with `_change`.

## Minimal success test

A finished panel builder should create a CSV or parquet file with the required columns for a
reproducible sample window and no duplicate week keys.
