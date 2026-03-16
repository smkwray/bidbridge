# Contributing

Thanks for contributing to BidBridge.

## Setup

BidBridge uses an external virtual environment to keep the repo directory clean (no `.venv` inside the repo).

```bash
# Create the venv (one time)
python3.11 -m venv ~/venvs/bidbridge

# Install the package in editable mode with dev extras
~/venvs/bidbridge/bin/pip install -e .[dev]

# Verify the install
~/venvs/bidbridge/bin/python -m bidbridge doctor
```

Do **not** create a `.venv` directory inside the repo. The external venv path `~/venvs/bidbridge` is the project convention.

## Running tests

```bash
PYTHONDONTWRITEBYTECODE=1 ~/venvs/bidbridge/bin/python -B -m pytest tests/ -x
```

All 15 tests should pass. The `-x` flag stops on the first failure for faster feedback.

## Workflow

- Start with a small issue or milestone.
- Read `AGENTS.md` and the relevant prompt in `codex/prompts/`.
- Keep commits narrowly scoped.
- Add or update tests whenever behavior changes.
- Update docs when schemas, assumptions, or source logic changes.

## Style

- Python 3.11+
- Plain pandas first
- Small pure functions where possible
- Typed interfaces for config-like objects
- Public-data provenance in code comments and docs

## Before opening a PR

```bash
~/venvs/bidbridge/bin/python -m bidbridge doctor
PYTHONDONTWRITEBYTECODE=1 ~/venvs/bidbridge/bin/python -B -m pytest tests/ -x
```

## Notebooks

Exploratory notebooks are welcome, but production logic must live in the package.
