# Auction-week panel specification

The auction-week panel is the core weekly table (`data/processed/auction_week_panel.csv`).

## Grain

One row per auction week. A separate week x maturity-bucket panel
(`data/processed/maturity_bucket_panel.csv`) feeds the fixed-effects models.

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

- soma_treasury_total (with soma_bills, soma_notes_bonds, and soma_tips where available)
- bank_treasury_securities

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
