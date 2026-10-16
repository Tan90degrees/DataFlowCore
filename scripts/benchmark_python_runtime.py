"""Real HTTP CPU files plus an aggressive-interval idle sample on the selected Python."""

import argparse
import hashlib
import json
import os
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from benchmark_internal import idle

from dataflowcore.client import APIError, Client
from dataflowcore.runtime import check_runtime


def until(check):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if value := check():
                return value
        except (OSError, APIError):
            pass
        time.sleep(0.02)
    raise TimeoutError("runtime benchmark condition timed out")


def cpu_trial(files, rounds, slots):
    with tempfile.TemporaryDirectory(prefix="dfc-python-cpu-") as directory:
        root = Path(directory)
        (root / "input.txt").write_text("immutable CPU input")
        repo = Path(__file__).resolve().parents[1]
        env = os.environ | {
            "PYTHONPATH": os.pathsep.join((str(repo / "src"), str(repo / "scripts")))
        }
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        processes, logs = [], []

        def launch(*args):
            log = (root / f"process-{len(processes)}.log").open("wb")
            logs.append(log)
            processes.append(
                subprocess.Popen(
                    [sys.executable, "-m", "dataflowcore.cli", *args],
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
            )

        client = Client(f"http://127.0.0.1:{port}", timeout=3)
        try:
            launch(
                "control",
                "--dsn",
                "sqlite:///" + str(root / "metadata.db"),
                "--port",
                str(port),
                "--data-root",
                str(root),
            )
            until(lambda: client.request("GET", "/readyz"))
            launch(
                "worker",
                "--url",
                client.url,
                "--data-root",
                str(root),
                "--work-root",
                str(root / "work"),
                "--slots",
                str(slots),
                "--cpu",
                "4",
                "--interval",
                "0.1",
                "--stop-grace",
                "0.2",
            )

            def ready():
                workers = client.request("GET", "/v1/workers")["workers"]
                return workers and workers[0].get("runtime", {}).get("runner_processes") == slots

            until(ready)

            def submit(iterations):
                return client.submit(
                    {
                        "input_path": str(root / "input.txt"),
                        "max_attempts": 1,
                        "steps": [
                            {
                                "id": "cpu",
                                "callable": "runtime_fixture:calculate",
                                "workload": "cpu",
                                "resources": {"cpu": 1, "memory_mb": 32},
                                "parameters": {"rounds": iterations},
                            }
                        ],
                    }
                )["id"]

            def completed(ids):
                rows = client.statuses(ids)["tasks"]
                if any(task["state"] == "FAILED" for task in rows):
                    raise AssertionError([client.get(task["id"]) for task in rows])
                return rows if all(task["state"] == "SUCCEEDED" for task in rows) else None

            warm = [submit(10000) for _ in range(slots)]
            until(lambda: completed(warm))
            started = time.monotonic()
            ids = [submit(rounds) for _ in range(files)]
            until(lambda: completed(ids))
            elapsed = time.monotonic() - started
            tasks = [client.get(tid) for tid in ids]
            values = [task["result"]["steps"]["cpu"] for task in tasks]
            assert all(task["attempt_count"] == 1 for task in tasks)
            assert len({value["checksum"] for value in values}) == 1
            assert all(
                task["result"]["runtime"]["gil_enabled"] is check_runtime()["gil_enabled"]
                for task in tasks
            )
            pids = {value["pid"] for value in values}
            assert len(pids) == slots
            assert all(
                len({value["cache_id"] for value in values if value["pid"] == pid}) == 1
                for pid in pids
            )
            assert any(value["cache_calls"] > 1 for value in values)
            return {
                "files": files,
                "rounds": rounds,
                "slots": slots,
                "elapsed_seconds": elapsed,
                "files_per_second": files / elapsed,
                "cpu_seconds": sum(value["cpu_seconds"] for value in values),
                "mean_busy_cores": sum(value["cpu_seconds"] for value in values) / elapsed,
                "runner_processes_used": len(pids),
                "checksum": values[0]["checksum"],
                "single_attempt_successes": len(tasks),
                "validated_cache_reuse": True,
            }
        finally:
            for process in reversed(processes):
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
            for log in logs:
                log.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", type=int, default=16)
    parser.add_argument("--rounds", type=int, default=4000000)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--idle-seconds", type=float, default=8)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if not 4 <= args.files <= 100 or min(args.rounds, args.repetitions, args.idle_seconds) <= 0:
        parser.error("files must be 4..100; rounds, repetitions and idle seconds must be positive")
    trials = {"single_slot": [], "four_slots": []}
    for repetition in range(args.repetitions):
        order = (1, 4) if repetition % 2 == 0 else (4, 1)
        for slots in order:
            result = cpu_trial(args.files, args.rounds, slots)
            trials["single_slot" if slots == 1 else "four_slots"].append(result)
            print(json.dumps(result), flush=True)
    with tempfile.TemporaryDirectory(prefix="dfc-python-idle-") as directory:
        idle_result = idle(directory, args.idle_seconds)
    package = Path(sys.modules["dataflowcore"].__file__).parent
    digest = hashlib.sha256(
        b"".join(path.name.encode() + path.read_bytes() for path in sorted(package.glob("*.py")))
    ).hexdigest()
    summary = {
        name: {
            key: statistics.median(trial[key] for trial in values)
            for key in ("elapsed_seconds", "files_per_second", "mean_busy_cores")
        }
        for name, values in trials.items()
    }
    report = {
        "runtime": check_runtime(),
        "source_digest": digest,
        "trials": trials,
        "summary": summary,
        "idle": idle_result,
        "speedup_four_over_one": summary["four_slots"]["files_per_second"]
        / summary["single_slot"]["files_per_second"],
        "caveat": "Pure Python CPU files; shared hardware timing is not a production model SLA",
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "runtime": report["runtime"],
                "summary": summary,
                "speedup_four_over_one": report["speedup_four_over_one"],
                "idle_requests": idle_result["worker_http_requests"],
                "idle_log_bytes": idle_result["log_bytes"],
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
