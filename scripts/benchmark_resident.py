"""Sequential, reproducible business comparison against the pre-resident revision."""

import argparse
import io
import json
import os
import statistics
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-ref", default="2f37f717552f3cb8c68108f8b391282d577158e8")
    parser.add_argument("--output-dir", default="docs/benchmarks/resident")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--files", type=int, default=24)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    archive = subprocess.check_output(["git", "archive", args.baseline_ref], cwd=repo)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=repo, text=True)
    runs = {"before": [], "resident": []}
    with tempfile.TemporaryDirectory(prefix="dataflow-before-resident-") as temp:
        baseline = Path(temp)
        with tarfile.open(fileobj=io.BytesIO(archive)) as stream:
            stream.extractall(baseline, filter="data")
        # Interleave trials to reduce ordering bias. Every run gets a new database,
        # new executors, identical files and the same real HTTP embedding fixture.
        for repetition in range(1, args.repetitions + 1):
            for name, package, ref in (
                ("before", baseline, args.baseline_ref),
                ("resident", repo, revision + ("-dirty" if dirty else "")),
            ):
                target = output / f"{name}-{repetition}.json"
                command = [
                    sys.executable,
                    str(repo / "scripts/business_benchmark.py"),
                    "--files",
                    str(args.files),
                    "--workers",
                    "2",
                    "--slots",
                    "4",
                    "--interval",
                    ".1",
                    "--chars",
                    "2048",
                    "--delay",
                    ".01",
                    "--output",
                    str(target),
                ]
                environment = os.environ | {
                    "PYTHONPATH": str(package / "src"),
                    "DATAFLOW_BENCHMARK_REVISION": ref,
                }
                subprocess.run(
                    command, env=environment, cwd=repo, stdout=subprocess.DEVNULL, check=True
                )
                runs[name].append(json.loads(target.read_text()))
                print(f"completed {name} {repetition}", flush=True)
    result = {
        name: {
            key: statistics.median(trial[key] for trial in trials)
            for key in (
                "elapsed_seconds",
                "files_per_second",
                "runner_processes_used",
                "runner_reused_tasks",
                "successful_files",
                "attempts",
            )
        }
        for name, trials in runs.items()
    }
    result["throughput_ratio"] = (
        result["resident"]["files_per_second"] / result["before"]["files_per_second"]
    )
    result["baseline_ref"] = args.baseline_ref
    result["repetitions"] = args.repetitions
    (output / "summary.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
