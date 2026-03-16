import pandas as pd

from bidbridge.demo import build_demo_outputs


def test_demo_pipeline_outputs_exist():
    outputs = build_demo_outputs()
    for path in outputs.values():
        assert path.exists()

    panel = pd.read_csv(outputs["panel"])
    assert "dealer_bridge_ratio" in panel.columns
    assert panel.shape[0] >= 10
