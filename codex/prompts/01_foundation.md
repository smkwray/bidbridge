# M0 — Foundation and repo hardening

You are working inside the `bidbridge` repo.

## Read first

- `AGENTS.md`
- `README.md`
- `docs/plan.md`
- `docs/panel_spec.md`
- `configs/sources.yml`
- `configs/study.yml`

## Goal

Strengthen the scaffold so it is a clean base for real data ingestion.

## Implement

1. Improve CLI ergonomics:
   - add source inspection,
   - add config inspection,
   - add stable exit codes for doctor checks.
2. Create a better typed source registry object.
3. Add a simple manifest helper for raw downloads.
4. Add tests for:
   - config loading,
   - source registry parsing,
   - path creation,
   - manifest serialization.
5. Tighten docstrings and module-level comments.

## Constraints

- Keep the repo lightweight.
- Do not implement brittle network scraping here.
- Preserve the demo pipeline.

## Definition of done

- tests pass,
- `python -m bidbridge doctor` is useful,
- source metadata is accessible through code and CLI,
- docs reflect any schema changes.
