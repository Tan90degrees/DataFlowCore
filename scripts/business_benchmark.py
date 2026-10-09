"""Reproducible full-process business benchmark; never uses an in-process fake worker."""

import argparse
import hashlib
import json
import os
import platform
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

from dataflowcore import __version__
from dataflowcore.client import APIError, Client
from dataflowcore.examples.embedding_fixture import FixtureServer
from dataflowcore.examples.ingestion import pipeline
from dataflowcore.runtime import file_hash, require_free_threading


def percentile(values, p):
    return sorted(values)[max(0, min(len(values) - 1, int((len(values) - 1) * p)))]


def wait(check, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if value := check():
                return value
        except OSError, APIError:
            pass
        time.sleep(0.05)
    raise TimeoutError("benchmark condition timed out")


def run(
    root,
    *,
    files=24,
    workers=1,
    slots=4,
    interval=0.5,
    chars=8192,
    delay=0.01,
    dsn=None,
    embedding_workers=4,
    cpu_rounds=0,
):
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    model = FixtureServer(delay=delay)
    thread = threading.Thread(target=model.serve_forever, daemon=True)
    thread.start()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = os.environ | {"DATAFLOW_ADMIN_TOKEN": "a" * 32, "DATAFLOW_WORKER_TOKEN": "w" * 32}
    processes, logs = [], []

    def launch(*args):
        log = (root / f"process-{len(processes)}.log").open("wb")
        logs.append(log)
        proc = subprocess.Popen(
            [sys.executable, "-m", "dataflowcore.cli", *args],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        processes.append(proc)

    try:
        launch(
            "control",
            "--port",
            str(port),
            "--data-root",
            str(root),
            "--dsn",
            dsn or "sqlite:///" + str(root / "control.db"),
        )
        client = Client(f"http://127.0.0.1:{port}", "a" * 32)
        wait(lambda: client.request("GET", "/readyz"))
        existing_sessions = {
            w["session_id"] for w in client.request("GET", "/v1/workers")["workers"]
        }
        for w in range(workers):
            launch(
                "worker",
                "--url",
                client.url,
                "--data-root",
                str(root),
                "--work-root",
                str(root / f"worker-{w}"),
                "--slots",
                str(slots),
                "--cpu",
                str(max(slots * 2, embedding_workers if cpu_rounds else 1)),
                "--memory-mb",
                str(slots * 512),
                "--interval",
                str(interval),
                "--stop-grace",
                "0.2",
            )
        wait(
            lambda: (
                len(
                    [
                        w
                        for w in client.request("GET", "/v1/workers")["workers"]
                        if w["session_id"] not in existing_sessions and w["online"]
                    ]
                )
                >= workers
            )
        )
        flow = pipeline(
            cpu=embedding_workers if cpu_rounds else 1,
            memory_mb=128,
            parameters={
                "embedding_url": None if cpu_rounds else f"http://127.0.0.1:{model.server_port}",
                "chunk_size": 512,
                "embedding_workers": embedding_workers,
                "cpu_rounds": cpu_rounds,
            },
        )
        ids, submit_ms, queries_ms = [], [], []
        run_id = uuid.uuid4().hex
        started = time.monotonic()
        for i in range(files):
            source = root / f"document-{i}.md"
            source.write_text((f"业务文件 {i} DataFlow document paragraph.\n" * chars)[:chars])
            before = time.monotonic()
            ids.append(
                client.submit(flow.spec(source), key=f"benchmark:{run_id}:document-{i}")["id"]
            )
            submit_ms.append((time.monotonic() - before) * 1000)
        final, poll_requests, peak_control_rss = {}, 0, 0
        deadline = started + 180
        while len(final) < files and time.monotonic() < deadline:
            pending = [tid for tid in ids if tid not in final]
            # The unchanged baseline lacks the batch API; optimized runs use it.
            if hasattr(client, "statuses"):
                batches = [pending[i : i + 100] for i in range(0, len(pending), 100)]
                for batch in batches:
                    before = time.monotonic()
                    statuses = client.statuses(batch)
                    poll_requests += 1
                    queries_ms.append((time.monotonic() - before) * 1000)
                    for task in statuses["tasks"]:
                        if task["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                            final[task["id"]] = client.get(task["id"])
            else:
                for tid in pending:
                    before = time.monotonic()
                    task = client.get(tid)
                    poll_requests += 1
                    queries_ms.append((time.monotonic() - before) * 1000)
                    if task["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                        final[tid] = task
            status_file = Path(f"/proc/{processes[0].pid}/status")
            if status_file.exists():
                line = next(
                    x for x in status_file.read_text().splitlines() if x.startswith("VmRSS:")
                )
                peak_control_rss = max(peak_control_rss, int(line.split()[1]) * 1024)
            time.sleep(0.05)
        elapsed = time.monotonic() - started
        assert len(final) == files, "benchmark timeout"
        assert all(t["state"] == "SUCCEEDED" for t in final.values()), [
            t["error"] for t in final.values()
        ]
        tasks = list(final.values())
        queue = [(t["attempts"][0]["started_at"] - t["created_at"]) * 1000 for t in tasks]
        runtime = [(t["updated_at"] - t["attempts"][0]["started_at"]) * 1000 for t in tasks]
        assert all(t["result"]["runtime"]["gil_enabled"] is False for t in tasks)
        assert all(t["attempt_count"] == 1 for t in tasks), (
            "unexpected retry in fault-free benchmark"
        )
        sessions = {t["attempts"][0]["worker_session"] for t in tasks}
        expected_rows = sum(t["result"]["steps"]["receipt"]["chunks"] for t in tasks)
        with sqlite3.connect(root / "index.sqlite") as conn:
            assert conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == expected_rows
        for task in tasks:
            for entry in task["result"]["files"]:
                assert (
                    file_hash(Path(task["result"]["output_dir"]) / entry["path"]) == entry["sha256"]
                )
        package = Path(sys.modules["dataflowcore"].__file__).parent
        result = {
            "runtime": require_free_threading(),
            "package_version": __version__,
            "run_id": run_id,
            "measured_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "platform": platform.platform(),
            "logical_cpus": os.cpu_count(),
            "affinity_cpus": len(os.sched_getaffinity(0)),
            "cpu_quota": Path("/sys/fs/cgroup/cpu.max").read_text().strip()
            if Path("/sys/fs/cgroup/cpu.max").exists()
            else None,
            "source_digest": hashlib.sha256(
                b"".join(
                    (Path(sys.modules["dataflowcore"].__file__).parent / name).read_bytes()
                    for name in (
                        "worker.py",
                        "runtime.py",
                        "store.py",
                        "client.py",
                        "contracts.py",
                        "api.py",
                        "__init__.py",
                    )
                )
            ).hexdigest(),
            "business_source_digest": hashlib.sha256(
                (package / "examples/ingestion.py").read_bytes()
                + (package / "examples/embedding_fixture.py").read_bytes()
            ).hexdigest(),
            "baseline_revision": os.getenv("DATAFLOW_BENCHMARK_REVISION"),
            "config": {
                "files": files,
                "workers": workers,
                "slots": slots,
                "interval": interval,
                "characters_per_file": chars,
                "fixture_http_delay_seconds": delay,
                "store": "postgres"
                if dsn and dsn.startswith(("postgresql://", "postgres://"))
                else "sqlite",
                "embedding_workers": embedding_workers,
                "cpu_rounds": cpu_rounds,
                "polling": "batch" if hasattr(client, "statuses") else "individual",
            },
            "elapsed_seconds": elapsed,
            "files_per_second": files / elapsed,
            "chunks_per_second": sum(t["result"]["steps"]["receipt"]["chunks"] for t in tasks)
            / elapsed,
            "submit_p95_ms": percentile(submit_ms, 0.95),
            "query_p95_ms": percentile(queries_ms, 0.95),
            "queue_p50_ms": percentile(queue, 0.5),
            "queue_p95_ms": percentile(queue, 0.95),
            "execution_p95_ms": percentile(runtime, 0.95),
            "used_executors": len(sessions),
            "successful_files": len(tasks),
            "validated_index_rows": expected_rows,
            "attempts": sum(t["attempt_count"] for t in tasks),
            "poll_requests": poll_requests,
            "control_peak_sampled_rss_bytes": peak_control_rss
            if peak_control_rss >= 10_000_000
            else None,
            "fixture_peak_http_requests": model.peak_active,
            "task_peak_rss_bytes": max(
                t["result"].get("usage", {}).get("peak_rss_bytes", 0) for t in tasks
            )
            or None,
            "task_cpu_seconds": sum(
                t["result"].get("usage", {}).get("cpu_seconds", 0) for t in tasks
            )
            or None,
            "step_p95_seconds": {
                step: percentile(
                    [
                        t["attempts"][-1]["progress"]["steps"][step].get("duration_seconds", 0)
                        for t in tasks
                    ],
                    0.95,
                )
                if "usage" in tasks[0]["result"]
                else None
                for step in ("parse", "chunk", "stats", "embed", "index", "receipt")
            },
        }
        return result
    finally:
        for proc in reversed(processes):
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        for log in logs:
            log.close()
        model.shutdown()
        model.server_close()
        thread.join(timeout=3)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", required=True)
    p.add_argument("--files", type=int, default=24)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--slots", type=int, default=4)
    p.add_argument("--interval", type=float, default=0.5)
    p.add_argument("--chars", type=int, default=8192)
    p.add_argument("--delay", type=float, default=0.01)
    p.add_argument("--dsn")
    p.add_argument("--embedding-workers", type=int, default=4)
    p.add_argument("--cpu-rounds", type=int, default=0)
    args = p.parse_args()
    with tempfile.TemporaryDirectory(prefix="dataflow-business-") as root:
        report = run(
            root,
            files=args.files,
            workers=args.workers,
            slots=args.slots,
            interval=args.interval,
            chars=args.chars,
            delay=args.delay,
            dsn=args.dsn,
            embedding_workers=args.embedding_workers,
            cpu_rounds=args.cpu_rounds,
        )
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
