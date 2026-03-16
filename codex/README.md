# Codex guide

This directory is the handoff layer for a coding agent.

## Recommended flow

```bash
python scripts/codex_task_runner.py list
python scripts/codex_task_runner.py show M0
```

Then feed the prompt file for the current milestone to Codex.

## Ground rules

- Read `AGENTS.md` first.
- Use `docs/plan.md`, `docs/data_sources.md`, and `docs/panel_spec.md` as the source of truth.
- Keep changes incremental.
- Do not skip tests and provenance.
- Finish M0 before trying to build real fetchers.
