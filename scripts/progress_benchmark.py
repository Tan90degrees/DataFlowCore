"""Measure actual durable writes for a chatty 10,000-chunk operator."""

import argparse
import json
import tempfile
import time
from pathlib import Path

from dataflowcore import runtime


def run():
    writes = 0
    original = runtime.atomic_json

    def count(*args):
        nonlocal writes
        writes += 1
        return original(*args)

    runtime.atomic_json = count
    try:
        with tempfile.TemporaryDirectory() as root:
            progress = runtime.Progress(Path(root) / "progress.json", [{"id": "step"}])
            progress.update("step", state="RUNNING")
            started = time.perf_counter()
            for i in range(10000):
                progress.update("step", completed=i + 1, total=10000)
            progress.update("step", state="SUCCEEDED")
            elapsed = time.perf_counter() - started
            snapshot = json.loads((Path(root) / "progress.json").read_text())
            assert snapshot["steps"]["step"]["completed"] == 10000
            return {"reports": 10000, "durable_writes": writes, "elapsed_seconds": elapsed}
    finally:
        runtime.atomic_json = original


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = run()
    Path(args.output).write_text(json.dumps(result, indent=2))
    print(json.dumps(result))
