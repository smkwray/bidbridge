# BidBridge plan

This document translates the seed guide into an execution plan for a public repo.

## Thesis fit

BidBridge sits between Treasury issuance and final sector holdings. The repo focuses on the
short-run intermediation layer: primary dealers as temporary balance-sheet warehouses when
end-investor demand is weak.

## Main question

When Treasury supply is heavy and nondealer demand is soft, do primary dealers absorb the gap?

## Observable signatures

A convincing first paper should look for the following sequence:

1. heavier issuance or clustered refunding supply,
2. weaker auction metrics or higher dealer awards,
3. larger dealer positions and financing usage,
4. persistence followed by normalization of dealer inventory.

## Unit of analysis

The default unit is the auction week, not just the month or quarter.

## Core workstreams

### 1. Data ingestion

Implement public-data fetchers for:

- primary dealer statistics,
- Treasury auctions,
- upcoming auctions,
- investor class allotments,
- SOMA holdings,
- H.8,
- optional TRACE Treasury aggregates.

### 2. Harmonization

Standardize:

- announcement date,
- auction date,
- issue date,
- maturity date,
- instrument group,
- auction-week key,
- weighted auction metrics,
- investor-class shares,
- dealer inventory and financing measures.

### 3. Bridge metrics

Minimum metrics:

- inventory change around supply shocks,
- dealer share of awards,
- financing intensity,
- inventory persistence,
- bridge episode flags under weak nondealer absorption.

### 4. Descriptive analysis

Start with:

- event-study charts,
- maturity splits,
- refunding vs ordinary weeks,
- bridge-episode summaries,
- simple regressions.

### 5. Public-repo hardening

Before launch, make sure the repo has:

- source registry and provenance notes,
- deterministic CLI entry points,
- tests,
- clear docs,
- generated example outputs.

## Build philosophy

Keep econometrics modest early. The first release should be transparent, reproducible, and
institutionally grounded before it becomes more causal or macro-ambitious.

## MVP definition

The MVP is done when the repo can:

1. fetch and cache priority public datasets,
2. build a weekly auction-capacity panel,
3. generate bridge metrics,
4. export at least two figures and one summary table,
5. explain every step in repo docs.
