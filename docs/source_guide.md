# Plan 4 — Primary Dealer Bridge / Auction Capacity

## One-sentence version

Study whether primary dealers **temporarily warehouse Treasury supply when end-investor demand is weak**, and whether auction outcomes, financing conditions, and dealer balance sheets show that bridge role.

## Main question

When banks, money funds, or foreigners do not immediately absorb Treasury supply, do primary dealers step in as the balance-sheet bridge?

And if they do, what shows up first:

- larger dealer positions,
- heavier financing usage,
- weaker auction metrics,
- more dealer inventory persistence,
- or unusual post-auction adjustment?

## Why this fits your thesis specifically

Your thesis is about the monetary and balance-sheet effects of debt management. This project gives you the missing **market-plumbing bridge** between Treasury issuance and final sector holdings.

It answers a very practical question that sits between your existing chapters:

- the Treasury issues debt,
- somebody has to absorb it,
- but in the short run the “somebody” may be the dealer system before the final holder appears.

That makes this a strong bridge between:

- your sector-holdings work,
- your WAM/issuance work,
- and any bank-capacity extension.

## Why this is not just another CoFlow / DASS / DFLMX job

This is mainly a **market microstructure / intermediation** project.

It should begin with:

- dealer inventory behavior,
- auction results,
- financing data,
- issuance timing.

That is much more institutional than the macro-screening or causal-pipeline work already in EconArk.

## Best free public data

| Source | Frequency | What it gives you | Why it matters |
|---|---:|---|---|
| NY Fed primary dealer statistics / FR 2004 aggregates | weekly (plus some daily/issue-specific forms) | dealer positions, transactions, financing, and related market activity | core dataset |
| Treasury auction results | daily | high yield, bid-to-cover, tails, award distribution | auction outcomes |
| Treasury upcoming auctions / refunding schedules | weekly / quarterly | known supply calendar | event timing |
| Treasury investor class auction allotments | monthly | buyer-category context | who else may be absorbing supply |
| Treasury issuance data / auction query | daily / monthly | size and maturity composition of supply | supply pressure |
| TRACE Treasury aggregate data (optional) | weekly / monthly | secondary-market trading context | market depth/liquidity overlay |
| SOMA holdings / Fed purchase operations | weekly / daily | Fed participation / balance-sheet backdrop | removes Fed effects |
| H.8 / bank data (optional) | weekly | bank absorption overlay | links to bank-capacity story |

## Core variables to build

### Supply side

- announced auction size
- actual issuance size
- bill share / coupon share
- refunding month indicators
- buyback windows (optional stretch)

### Dealer bridge variables

- primary dealer Treasury positions
- financing / repo usage
- inventory persistence after auctions
- dealer share of absorption in heavy-supply weeks
- optional: specific-issue or when-issued measures if feasible

### Outcome variables

- auction bid-to-cover
- tail / stop-out behavior
- immediate post-auction dealer position change
- time to inventory normalization
- interaction with issuance maturity

## Suggested empirical design

## 1. Build an auction-week panel

The natural unit is not just the month or quarter. It is the **auction week**.

For each auction week, merge:

- supply information,
- dealer inventory,
- financing usage,
- auction results,
- broad buyer context.

## 2. Test the basic bridge story

A simple first pass:

- when issuance is heavy and nondealer absorption looks weak,
- do dealer positions jump?
- do those positions mean-revert afterward?

That already gives you the central result.

## 3. Split by instrument and maturity

Important splits:

- bills vs coupons
- short coupons vs long coupons
- refunding weeks vs ordinary weeks
- buyback weeks vs non-buyback weeks (stretch)

## 4. Add “capacity stress” conditions

Examples:

- periods of weak bank absorption,
- periods of large TGA rebuild,
- QT periods,
- risk-off windows.

The question becomes:
**when private balance-sheet capacity is tight, does the dealer bridge get larger or more fragile?**

## 5. Keep the econometrics modest

This project does not need a giant causal stack at first. It can be strong with:

- event-study charts,
- auction-week regressions,
- persistence tests,
- descriptive bridge metrics.

## Minimum viable paper

A good first paper shows:

1. primary dealers warehouse more Treasury supply in certain supply/stress regimes,
2. the bridge is larger for some maturities than others,
3. dealer balance-sheet usage helps explain why issuance does not map one-for-one into immediate final-holder changes.

## Stretch version

- dealer bridge around buyback operations
- link to bank-capacity project
- link to foreign-demand project
- limited connection to TGA rebuild episodes

## How to use your existing code and models

- **TDC project:** use only as conceptual context
- **SLR Watch:** optional market-capacity cross-reference
- **CoFlow / DASS / DFLMX:** mostly unnecessary for the first version
- **SFC simulation:** not needed

## Main risks and fallback

### Risk 1: aggregate dealer data only
That is okay. The point is not dealer-level identification, but the aggregate bridge role.

### Risk 2: auction microstructure can sprawl
Keep the first version very focused on one question:
**do dealers bridge weak end-investor absorption?**

### Risk 3: too disconnected from thesis
Make the opening section explicit: this is the missing short-run intermediation channel between issuance and final sector ownership.

## Best home

**Best home: standalone GitHub repo.**

Why:

- distinct data pipeline,
- clean public-data story,
- broader audience than the thesis alone,
- can evolve into an updateable “auction capacity monitor.”

This is one of the strongest candidates for a public-facing repo.
