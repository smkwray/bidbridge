import pandas as pd

from bidbridge.features.bridge_metrics import add_bridge_metrics


def test_add_bridge_metrics_basic():
    frame = pd.DataFrame(
        {
            "week_start": pd.to_datetime(["2025-01-06", "2025-01-13", "2025-01-20"]),
            "awarded_amount_total": [50.0, 90.0, 60.0],
            "nondealer_share": [0.8, 0.55, 0.75],
            "pd_treasury_inventory": [200.0, 225.0, 220.0],
            "pd_financing_usage": [120.0, 135.0, 130.0],
        }
    )

    out = add_bridge_metrics(frame)

    assert "inventory_change" in out.columns
    assert "dealer_bridge_ratio" in out.columns
    assert out.loc[1, "inventory_change"] == 25.0
    assert out["bridge_episode"].isin([True, False]).all()
