"""Sequential measurements, with raw data and environment retained for comparison."""

import argparse
import json
import statistics
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="docs/benchmarks")
    parser.add_argument(
        "--skip-baseline", action="store_true", help="reuse recorded immutable baseline"
    )
    args = parser.parse_args()
    directory = Path(args.output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    jobs = []
    for kind in ("baseline", "optimized"):
        for repetition in range(1, 4):
            script = "benchmark_baseline" if kind == "baseline" else "business_benchmark"
            jobs.append((f"{kind}-{repetition}", script, []))
    jobs.extend(
        [
            ("two-executors", "business_benchmark", ["--workers", "2"]),
            (
                "batch-120",
                "business_benchmark",
                ["--files", "120", "--workers", "2", "--slots", "8", "--chars", "2048"],
            ),
            (
                "large-file",
                "business_benchmark",
                ["--files", "1", "--slots", "1", "--chars", "1048576", "--delay", "0"],
            ),
            ("progress-baseline", "benchmark_baseline", ["--progress"]),
            ("progress-optimized", "progress_benchmark", []),
        ]
    )
    for threads in (1, 4):
        for repetition in range(1, 4):
            jobs.append(
                (
                    f"cpu-{threads}-{repetition}",
                    "business_benchmark",
                    [
                        "--files",
                        "1",
                        "--slots",
                        "1",
                        "--chars",
                        "32768",
                        "--interval",
                        ".1",
                        "--embedding-workers",
                        str(threads),
                        "--cpu-rounds",
                        "15000",
                    ],
                )
            )
    for name, script, extra in jobs:
        output = directory / f"{name}.json"
        if args.skip_baseline and (name.startswith("baseline-") or name == "progress-baseline"):
            if not output.is_file():
                raise FileNotFoundError(output)
            continue
        command = [sys.executable, f"scripts/{script}.py", "--output", str(output), *extra]
        with (directory / f"{name}.log").open("w") as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
        print(f"completed {name}", flush=True)
    result = {}
    for kind in ("baseline", "optimized"):
        runs = [json.loads((directory / f"{kind}-{i}.json").read_text()) for i in range(1, 4)]
        result[kind] = {
            k: statistics.median(r[k] for r in runs)
            for k in ("elapsed_seconds", "files_per_second", "queue_p95_ms", "poll_requests")
        }
    result["throughput_ratio"] = (
        result["optimized"]["files_per_second"] / result["baseline"]["files_per_second"]
    )
    cpu = {}
    for threads in (1, 4):
        runs = [
            json.loads((directory / f"cpu-{threads}-{i}.json").read_text()) for i in range(1, 4)
        ]
        cpu[threads] = statistics.median(r["step_p95_seconds"]["embed"] for r in runs)
    result["cpu_embedding_step_median_seconds"] = cpu
    result["cpu_thread_speedup"] = cpu[1] / cpu[4]
    (directory / "summary.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
