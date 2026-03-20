from __future__ import annotations

import pandas as pd

from bidbridge.analysis.local_projections import (
    generate_lp_placebo_table,
    run_local_projection_placebos,
)
from bidbridge.demo import build_demo_outputs


def test_local_projection_placebos_generate_table():
    outputs = build_demo_outputs()
    panel = pd.read_csv(outputs["panel"], parse_dates=["week_start"])

    placebo = run_local_projection_placebos(panel, max_horizon=4)
    assert not placebo.empty
    assert {"placebo_type", "horizon", "beta", "p_value"}.issubset(placebo.columns)
    assert set(placebo["placebo_type"]) == {"lead_placebo_h1", "shifted_placebo_h4"}


def test_generate_lp_placebo_table_writes_file(tmp_path):
    placebo = pd.DataFrame({
        "horizon": [0, 1],
        "beta": [1.0, 2.0],
        "se": [0.1, 0.2],
        "t_stat": [10.0, 10.0],
        "p_value": [0.01, 0.02],
        "ci_lower": [0.8, 1.6],
        "ci_upper": [1.2, 2.4],
        "n_obs": [20, 19],
        "r_squared": [0.1, 0.1],
        "regime": ["lead_placebo_h1", "shifted_placebo_h4"],
        "placebo_type": ["lead_placebo_h1", "shifted_placebo_h4"],
    })
    out = generate_lp_placebo_table(placebo, tmp_path)
    assert out.exists()
