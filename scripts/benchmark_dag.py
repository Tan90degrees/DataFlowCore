"""Measure large-DAG bookkeeping on the real resident runtime, with validated results."""

import argparse
import hashlib
import json
import os
import platform
import statistics
import tempfile
import time
from collections import Counter
from pathlib import Path


def trial(shape, steps, reports):
    import dataflowcore.runtime as engine
    from dataflowcore import __version__
    from dataflowcore.contracts import TaskSpec
    from dataflowcore.resources import ResourceBudget
    from dataflowcore.runner_runtime import RunnerRuntime

    source = Path(engine.__file__).parent
    digest = hashlib.sha256()
    for name in ("runtime.py", "resources.py", "runner_runtime.py"):
        digest.update(name.encode() + (source / name).read_bytes())
    writes, sql = Counter(), Counter()
    original_atomic, original_fsync = engine.atomic_json, os.fsync

    def atomic(path, value, **kwargs):
        writes["progress_snapshots"] += Path(path).name == "progress.json"
        return original_atomic(path, value, **kwargs)

    def fsync(fd):
        writes["fsync_calls"] += 1
        return original_fsync(fd)

    def trace(statement):
        if statement.startswith("INSERT OR IGNORE INTO requests"):
            sql["request_insert_statements"] += 1
        if statement.startswith("BEGIN IMMEDIATE"):
            sql["write_transactions"] += 1

    with tempfile.TemporaryDirectory(prefix="dfc-dag-benchmark-") as directory:
        root = Path(directory)
        (root / "input.txt").write_text("immutable benchmark input")
        (root / "workspace").mkdir()
        nodes = [
            {
                "id": f"n{index:04d}",
                "callable": "dag_fixture:visit",
                "workload": "cpu",
                "resources": {"cpu": 0.001, "memory_mb": 1},
                "depends_on": [f"n{index - 1:04d}"] if shape == "chain" and index else [],
                "parameters": {"reports": reports},
            }
            for index in range(steps)
        ]
        spec = TaskSpec.parse({"input_path": str(root / "input.txt"), "steps": nodes})
        budget = ResourceBudget(root / "budget.sqlite", 4, 1024)
        runtime = RunnerRuntime(dag_workers=4, budget=budget)
        runtime.budget.connection.set_trace_callback(trace)
        engine.atomic_json, os.fsync = atomic, fsync
        started, cpu_started = time.perf_counter(), time.process_time()
        try:
            result = engine.run_dag(
                {"task_id": "task", "attempt_id": "attempt", "spec": spec.storage()},
                root / "workspace",
                root,
                runtime=runtime,
            )
            elapsed, cpu = time.perf_counter() - started, time.process_time() - cpu_started
        finally:
            engine.atomic_json, os.fsync = original_atomic, original_fsync
            runtime.close()
        progress = json.loads((root / "workspace" / "progress.json").read_text())
        assert progress["state"] == "SUCCEEDED" and progress["completed_steps"] == steps
        assert len(result["steps"]) == steps and not result["files"]
        for node in nodes:
            assert result["steps"][node["id"]] == {
                "step": node["id"],
                "parents": node["depends_on"],
            }
            assert progress["steps"][node["id"]]["state"] == "SUCCEEDED"
        return {
            "shape": shape,
            "steps": steps,
            "reports_per_step": reports,
            "elapsed_seconds": elapsed,
            "cpu_seconds": cpu,
            "steps_per_second": steps / elapsed,
            **writes,
            **sql,
            "core_version": __version__,
            "source_sha256": digest.hexdigest(),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "executor_budget": {"cpu": 4, "memory_mb": 1024, "cpu_pool": 4},
            "gil_enabled": result["runtime"]["gil_enabled"],
            "validated_steps": steps,
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--steps", type=int, default=1000)
    parser.add_argument("--reports", type=int, default=20)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if not 1 <= args.steps <= 1000 or args.reports < 0 or args.repetitions < 1:
        parser.error("steps must be 1..1000, reports nonnegative, repetitions positive")
    samples = {shape: [] for shape in ("chain", "wide")}
    for _ in range(args.repetitions):
        for shape in samples:
            result = trial(shape, args.steps, args.reports)
            samples[shape].append(result)
            print(json.dumps(result), flush=True)
    output = {
        "workload": "Validated tiny operators: chain and independent roots; resident runtime",
        "caveat": "Measures orchestration overhead, not production model or file throughput",
        "trials": samples,
        "median": {
            shape: {
                key: statistics.median(row[key] for row in rows)
                for key in (
                    "elapsed_seconds",
                    "cpu_seconds",
                    "steps_per_second",
                    "progress_snapshots",
                    "fsync_calls",
                    "request_insert_statements",
                    "write_transactions",
                )
            }
            for shape, rows in samples.items()
        },
    }
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    main()
