from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bidbridge.demo import write_demo_data


if __name__ == "__main__":
    outputs = write_demo_data()
    for key, value in outputs.items():
        print(f"{key}: {value}")
