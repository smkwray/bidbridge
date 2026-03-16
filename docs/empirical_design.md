# Empirical design

Keep the first version descriptive and institutional.

## Question 1

When weekly supply is heavy, do dealer inventories rise more when nondealer allotments are weak?

Example baseline regression:

`inventory_change_t = a + b1 * awarded_amount_t + b2 * weak_end_investor_t + b3 * (awarded_amount_t * weak_end_investor_t) + controls + e_t`

## Question 2

Does inventory normalize after bridge episodes?

First pass:

- event-study plots around heavy-supply weeks,
- persistence proxies over 1 to 4 weeks,
- split by bills vs coupons.

## Question 3

Are bridge episodes more fragile under capacity stress?

Add simple interactions with:

- QT periods,
- TGA rebuild windows,
- weak bank absorption proxies,
- risk-off windows.

## What the first paper does not need

- dealer-level identification,
- a giant causal pipeline,
- structural modeling,
- full integration with broader sector-flow frameworks.

## Figure list for MVP

1. Weekly supply vs dealer inventory change
2. Dealer share of awards during heavy-supply weeks
3. Mean inventory path around bridge episodes
4. Maturity split comparison: bills vs coupons

## Table list for MVP

1. Source coverage and sample window
2. Summary statistics by week type
3. Baseline bridge regression
4. Persistence split by instrument group
