from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def make_supply_inventory_plot(panel: pd.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    figure = plt.figure(figsize=(10, 5))
    plt.plot(panel["week_start"], panel["awarded_amount_total"], label="Weekly awarded amount")
    plt.plot(panel["week_start"], panel["inventory_change"], label="Dealer inventory change")
    plt.title("Demo supply and dealer inventory change")
    plt.xlabel("Week start")
    plt.ylabel("Billions (synthetic demo units)")
    plt.legend()
    plt.tight_layout()
    figure.savefig(output_path, dpi=150)
    plt.close(figure)
    return output_path
