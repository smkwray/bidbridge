from __future__ import annotations

from pathlib import Path
import argparse
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bidbridge.paths import CODEX_DIR


def load_tasks() -> list[dict]:
    path = CODEX_DIR / "tasks.json"
    return json.loads(path.read_text(encoding="utf-8"))


def list_tasks() -> int:
    for task in load_tasks():
        print(f"{task['id']}\t{task['title']}\tstatus={task['status']}")
    return 0


def show_task(task_id: str) -> int:
    for task in load_tasks():
        if task["id"] == task_id:
            print(json.dumps(task, indent=2))
            prompt = CODEX_DIR / task["prompt_file"].replace("codex/", "", 1)
            if prompt.exists():
                print("\n--- prompt path ---")
                print(prompt)
            return 0
    print(f"Task not found: {task_id}")
    return 1


def next_task() -> int:
    for task in load_tasks():
        if task.get("status") == "ready":
            print(json.dumps(task, indent=2))
            return 0
    print("No ready tasks.")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list")
    show = sub.add_parser("show")
    show.add_argument("task_id")
    sub.add_parser("next")

    args = parser.parse_args()

    if args.command == "list":
        return list_tasks()
    if args.command == "show":
        return show_task(args.task_id)
    if args.command == "next":
        return next_task()

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
