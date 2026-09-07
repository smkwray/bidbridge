from __future__ import annotations

import pandas as pd
import pytest

from bidbridge.analysis.ratewall_contracts import (
    build_ratewall_dealer_warehousing_support_contract,
)


def test_dealer_warehousing_support_is_transient_bounded_proxy() -> None:
    panel = pd.DataFrame(
        [
            {
                "week_start": "2025-01-06",
                "week_end": "2025-01-12",
                "awarded_amount_total": 100.0,
                "dealer_share_allotment": 0.4,
                "pd_treasury_inventory": 1000.0,
                "pd_financing_usage": 500.0,
                "inventory_change": 25.0,
                "dealer_bridge_ratio": 0.25,
                "bridge_episode": True,
            }
        ]
    )
    persistence = pd.DataFrame([{"halflife_weeks": 4.5}])

    rows = build_ratewall_dealer_warehousing_support_contract(
        panel,
        persistence_summary=persistence,
    )

    row = rows.iloc[0]
    assert row["evidence_tier"] == "bounded_proxy"
    assert row["measurement_stage"] == "transient_bridge"
    assert row["observed_field_status"] == "source_backed_measurement"
    assert row["route_interpretation_status"] == "bounded_proxy"
    assert "final_holder_split" in row["blocked_use"]
    assert "source_backed_private_bucket_split" in row["blocked_use"]
    assert row["persistence_half_life_weeks"] == "4.5"


def test_ratewall_writer_fails_closed_for_missing_declared_persistence(tmp_path) -> None:
    from bidbridge.analysis.ratewall_contracts import (
        write_ratewall_dealer_warehousing_support_contract,
    )

    panel = tmp_path / "panel.csv"
    pd.DataFrame([{"week_start": "2025-01-06"}]).to_csv(panel, index=False)

    with pytest.raises(FileNotFoundError, match="Required persistence summary"):
        write_ratewall_dealer_warehousing_support_contract(
            panel_path=panel,
            persistence_summary_path=tmp_path / "missing.csv",
            output_path=tmp_path / "support.csv",
        )
