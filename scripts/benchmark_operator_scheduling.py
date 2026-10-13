"""Interleaved real HTTP business runs comparing task and operator admission."""

import argparse
import json
import statistics
import tempfile
from pathlib import Path

from business_benchmark import run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="docs/benchmarks/operators")
    parser.add_argument("--files", type=int, default=8)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--dsn")
    args = parser.parse_args()
    if args.files < 1 or args.repetitions < 1:
        parser.error("files and repetitions must be positive")
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    trials = {"task": [], "operator": []}
    for repetition in range(1, args.repetitions + 1):
        order = ("task", "operator") if repetition % 2 else ("operator", "task")
        for mode in order:
            with tempfile.TemporaryDirectory(prefix=f"dataflow-{mode}-") as root:
                result = run(
                    root,
                    files=args.files,
                    workers=1,
                    slots=4,
                    interval=0.1,
                    chars=8192,
                    delay=0.03,
                    dsn=args.dsn,
                    scheduling=mode,
                    worker_cpu=2,
                    worker_memory_mb=1024,
                    task_cpu=2,
                )
            (output / f"{mode}-{repetition}.json").write_text(json.dumps(result, indent=2))
            trials[mode].append(result)
            print(
                f"{mode} trial {repetition}: {result['elapsed_seconds']:.3f}s; "
                f"{result['successful_files']} successful",
                flush=True,
            )
    summary = {
        mode: {
            key: statistics.median(t[key] for t in values)
            for key in (
                "elapsed_seconds",
                "files_per_second",
                "runner_processes_used",
                "attempts",
                "successful_files",
                "validated_index_rows",
                "fixture_peak_http_requests",
            )
        }
        for mode, values in trials.items()
    }
    summary.update(
        repetitions=args.repetitions,
        throughput_ratio=summary["operator"]["files_per_second"]
        / summary["task"]["files_per_second"],
        workload="8192-character documents; six-node ingestion DAG; HTTP fixture delay 30ms",
        worker_budget={"cpu": 2, "memory_mb": 1024, "slots": 4},
        caveat=(
            "Declared admission budget, not a cgroup limit. "
            "Fixture IO results do not predict production CPU speedup."
        ),
    )
    (output / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
