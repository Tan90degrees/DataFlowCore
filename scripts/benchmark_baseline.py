"""Run the same reference workload with the previous committed framework engine."""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ref", default="e1fe96e86b322a13913aa57b0ab49bfe9f16fae2")
    parser.add_argument("--output", required=True)
    parser.add_argument("--progress", action="store_true")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="dataflow-baseline-") as root:
        root = Path(root)
        files = subprocess.check_output(
            ["git", "ls-tree", "-r", "--name-only", args.ref, "src"], text=True
        ).splitlines()
        for file in files:
            target = root / file
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(subprocess.check_output(["git", "show", f"{args.ref}:{file}"]))
        shutil.copytree(
            "src/dataflowcore/examples",
            root / "src/dataflowcore/examples",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        # Only adapt the business operator to the old Context. Scheduling, progress,
        # HTTP API, DB and watchdog remain the exact committed baseline implementations.
        runtime = root / "src/dataflowcore/runtime.py"
        with runtime.open("a") as out:
            out.write(
                "\ndef _baseline_map(self, fn, items, *, max_workers=4, max_pending=None):\n"
                "    with ThreadPoolExecutor(max_workers=max_workers) as pool:\n"
                "        yield from pool.map(fn, items, buffersize=max_pending or max_workers*2)\n"
                "Context.map = _baseline_map\n"
            )
        env = os.environ | {
            "PYTHONPATH": str(root / "src"),
            "DATAFLOW_BENCHMARK_REVISION": args.ref,
        }
        script = (
            "scripts/progress_benchmark.py" if args.progress else "scripts/business_benchmark.py"
        )
        subprocess.run(
            [sys.executable, script, "--output", args.output],
            env=env,
            check=True,
        )


if __name__ == "__main__":
    main()
