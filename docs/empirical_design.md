# Empirical design

Keep the first version descriptive and institutional.

## Main question

When weekly supply is heavy, do dealer inventories rise more when nondealer allotments are weak?

Baseline local-projection and descriptive specifications use ex ante supply shocks and lagged controls rather than contemporaneous auction outcomes.

```text
inventory_change_t = a + b1 * awarded_amount_t + b2 * weak_end_investor_t + b3 * (awarded_amount_t * weak_end_investor_t) + controls + e_t
```

## Persistence and falsification

Inventory normalization is assessed with:

- event-study plots around heavy-supply weeks,
- persistence proxies over 1 to 4 weeks,
- LP placebo / falsification checks using lead or shifted shocks,
- splits by bills vs coupons.

## Capacity stress

Bridge episodes are cross-tabbed with simple capacity stress proxies:

- QT periods,
- TGA rebuild windows,
- weak bank absorption proxies,
- risk-off windows.

## Fixed-effects robustness

The maturity-bucket panel FE branch is a robustness layer, not the only estimate. The public FE surface uses Driscoll-Kraay as the headline inference method and keeps clustered-by-bucket results as secondary robustness outputs. When granular coupon bands are unavailable, the headline FE table is withheld rather than relying on proportional coupon allocation.

Thursday-start aggregation is the explicit robustness check for the week-alignment issue between auction timing and Wednesday NY Fed position snapshots.

## Pressure monitor

The repo also emits a compact upcoming-auction pressure monitor. It is a descriptive output built from the same public-data pipeline and is intended as a transparency tool, not a separate forecasting model.

## What the first paper does not need

- dealer-level identification,
- a giant causal pipeline,
- structural modeling,
- full integration with broader sector-flow frameworks.

## Figure list

1. Weekly supply vs dealer inventory change
2. Dealer share of awards during heavy-supply weeks
3. Mean inventory path around bridge episodes
4. Maturity split comparison: bills vs coupons
5. Upcoming-auction pressure monitor

## Table list

1. Source coverage and sample window
2. Summary statistics by week type
3. Baseline bridge regression
4. Persistence split by instrument group
5. FE headline export plus Thursday-start robustness
