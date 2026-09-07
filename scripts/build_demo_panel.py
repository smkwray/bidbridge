from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bidbridge.demo import build_demo_outputs


if __name__ == "__main__":
    outputs = build_demo_outputs()
    for key, value in outputs.items():
        print(f"{key}: {value}")
