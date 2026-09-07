#!/usr/bin/env bash
set -euo pipefail

python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]

python -m bidbridge doctor
python scripts/make_demo_data.py
python scripts/build_demo_panel.py
pytest
