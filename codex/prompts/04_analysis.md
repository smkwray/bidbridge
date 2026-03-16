# M3 — Bridge metrics and first analyses

You are adding the first actual research outputs.

## Read first

- `docs/empirical_design.md`
- `docs/panel_spec.md`

## Goal

Produce first-pass descriptive evidence for the dealer bridge story.

## Implement

1. Bridge metrics:
   - inventory change
   - dealer bridge ratio
   - financing intensity
   - persistence proxies
   - heavy supply and weak absorption flags
2. Figures:
   - supply vs inventory change
   - bridge episodes
   - bills vs coupons split
   - inventory normalization after selected weeks
3. Tables:
   - summary stats
   - baseline regression coefficients
   - split-sample comparison
4. CLI or script entry points to regenerate all outputs.

## Guardrails

- Keep models modest.
- Label proxies clearly.
- Do not overclaim causality.

## Done

The repo can regenerate figures and tables from the processed panel in one documented flow.
