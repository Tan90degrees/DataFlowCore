"""Compare immutable source revisions using real processes and validated file DAGs.

The parent creates a fresh PG schema for each sample. Reports never contain DSNs.
Idle CPU is null when /proc is unavailable, rather than pretending usage is zero.
"""

import argparse
import contextlib
import hashlib
import importlib.util
import json
import os
import re
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import quote, urlencode


def cpu_seconds(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rpartition(") ")[2].split()
        return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    except OSError, IndexError, ValueError:
        return None


def metrics(client):
    import urllib.request

    request = urllib.request.Request(
        client.url + "/metrics", headers={"Authorization": "Bearer " + "a" * 32}
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        body = response.read().decode()
    return {
        route: int(float(value))
        for route, value in re.findall(
            r'dataflow_http_requests_total\{method="POST",route="(worker_[^\"]+)"'
            r',status="2xx"\} (\S+)',
            body,
        )
    }


def wait(check):
    end = time.monotonic() + 20
    while time.monotonic() < end:
        try:
            if result := check():
                return result
        except OSError:
            pass
        time.sleep(0.05)
    raise TimeoutError("component did not become ready")


def idle(root, seconds):
    from dataflowcore.client import APIError, Client

    root = Path(root)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    logs, processes = [], []

    def launch(*args):
        stream = (root / f"process-{len(processes)}.log").open("wb")
        logs.append(stream)
        proc = subprocess.Popen(
            [sys.executable, "-m", "dataflowcore.cli", *args],
            stdout=stream,
            stderr=subprocess.STDOUT,
        )
        processes.append(proc)

    try:
        launch(
            "control",
            "--dsn",
            "sqlite:///" + str(root / "state.db"),
            "--port",
            str(port),
            "--data-root",
            str(root),
            "--lease",
            "3",
        )
        client = Client(f"http://127.0.0.1:{port}", "a" * 32)

        def ready():
            try:
                return client.request("GET", "/readyz")
            except APIError:
                return False

        wait(ready)
        launch(
            "worker",
            "--url",
            client.url,
            "--data-root",
            str(root),
            "--work-root",
            str(root / "work"),
            "--slots",
            "4",
            "--interval",
            "0.2",
            "--stop-grace",
            "0.2",
        )

        def worker_ready():
            try:
                health = json.loads((root / "work" / "health.json").read_text())
                return health["registered"] and health["live_runners"] == 4
            except OSError, ValueError:
                return False

        wait(worker_ready)
        pids = [p.pid for p in processes] + [
            json.loads(p.read_text())["pid"] for p in (root / "work").glob("*/runner-*/state.json")
        ]
        before_cpu = [cpu_seconds(pid) for pid in pids]
        before_requests = metrics(client)
        before_logs = sum(p.stat().st_size for p in root.glob("process-*.log"))
        started = time.monotonic()
        time.sleep(seconds)
        elapsed = time.monotonic() - started
        log_bytes = sum(p.stat().st_size for p in root.glob("process-*.log")) - before_logs
        after_cpu = [cpu_seconds(pid) for pid in pids]
        after_requests = metrics(client)
        return {
            "seconds": elapsed,
            "worker_http_requests": sum(after_requests.values()) - sum(before_requests.values()),
            "log_bytes": log_bytes,
            "cpu_seconds": sum(a - b for a, b in zip(after_cpu, before_cpu, strict=True))
            if all(v is not None for v in after_cpu + before_cpu)
            else None,
            "idle_workers": client.request("GET", "/v1/workers")["workers"],
            "configuration": {"slots": 4, "interval_seconds": 0.2, "lease_seconds": 3},
        }
    finally:
        for p in reversed(processes):
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=3)
        for stream in logs:
            stream.close()


def business(root, source, files, dsn, interval):
    from dataflowcore.client import Client

    spec = importlib.util.spec_from_file_location(
        "business_sample", source / "scripts" / "business_benchmark.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    observed = {}

    class ObservedClient(Client):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.completed = set()

        def request(self, method, path, *args, **kwargs):
            result = super().request(method, path, *args, **kwargs)
            if (
                method == "GET"
                and path.startswith("/v1/tasks/")
                and result.get("state") == "SUCCEEDED"
            ):
                self.completed.add(result["id"])
                if len(self.completed) == files and not observed:
                    observed["worker_requests_by_route"] = metrics(self)
                    observed["database_pool"] = (
                        super().request("GET", "/v1/control").get("database")
                    )
            return result

    module.Client = ObservedClient
    result = module.run(
        root, files=files, workers=1, slots=4, interval=interval, chars=2048, delay=0.005, dsn=dsn
    )
    return {**result, **observed}


@contextlib.contextmanager
def trial_database(dsn):
    if not dsn:
        yield None
        return
    import psycopg
    from psycopg import sql

    schema = "dfc_perf_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            yield (
                dsn
                + ("&" if "?" in dsn else "?")
                + urlencode({"options": f"-c search_path={schema}"}, quote_via=quote)
            )
        finally:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def sample(args):
    from dataflowcore import __version__
    from dataflowcore.runtime import require_free_threading

    with tempfile.TemporaryDirectory(prefix="dfc-efficiency-") as root:
        value = (
            idle(root, args.idle_seconds)
            if args.sample == "idle"
            else business(
                root, args.source, args.files, os.getenv("DATAFLOW_BENCHMARK_DSN"), args.interval
            )
        )
    files = sorted((args.source / "src" / "dataflowcore").glob("*.py"))
    return {
        "version": __version__,
        "runtime": require_free_threading(),
        "source_digest": hashlib.sha256(
            b"".join(p.name.encode() + p.read_bytes() for p in files)
        ).hexdigest(),
        "sample": args.sample,
        "result": value,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--baseline-source", type=Path)
    parser.add_argument("--sample", choices=("idle", "business"))
    parser.add_argument("--dsn")
    parser.add_argument("--files", type=int, default=24)
    parser.add_argument("--interval", type=float, default=2)
    parser.add_argument("--idle-seconds", type=float, default=8)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    args.source = args.source.resolve()
    if args.sample:
        result = sample(args)
    else:
        sources = {"candidate": args.source}
        if args.baseline_source:
            sources = {"baseline": args.baseline_source.resolve(), **sources}
        reports = {k: [] for k in sources}
        for repetition in range(args.repetitions):
            for name, source in sources.items():
                values = {}
                for kind in ("idle", "business"):
                    with trial_database(args.dsn if kind == "business" else None) as dsn:
                        env = os.environ | {
                            "PYTHONPATH": str(source / "src"),
                            "DATAFLOW_ADMIN_TOKEN": "a" * 32,
                            "DATAFLOW_WORKER_TOKEN": "w" * 32,
                            "DATAFLOW_BENCHMARK_DSN": dsn or "",
                        }
                        with tempfile.TemporaryDirectory() as temporary:
                            output = Path(temporary) / "sample.json"
                            subprocess.run(
                                [
                                    sys.executable,
                                    str(Path(__file__).resolve()),
                                    "--sample",
                                    kind,
                                    "--source",
                                    str(source),
                                    "--output",
                                    str(output),
                                    "--files",
                                    str(args.files),
                                    "--idle-seconds",
                                    str(args.idle_seconds),
                                    "--interval",
                                    str(args.interval),
                                ],
                                env=env,
                                check=True,
                            )
                            values[kind] = json.loads(output.read_text())
                reports[name].append(values)
                print(
                    json.dumps(
                        {
                            "trial": repetition + 1,
                            "source": name,
                            "idle_requests": values["idle"]["result"]["worker_http_requests"],
                            "idle_log_bytes": values["idle"]["result"]["log_bytes"],
                            "files_per_second": values["business"]["result"]["files_per_second"],
                        }
                    ),
                    flush=True,
                )
        summary = {
            name: {
                "idle_requests": statistics.median(
                    v["idle"]["result"]["worker_http_requests"] for v in trials
                ),
                "idle_log_bytes": statistics.median(
                    v["idle"]["result"]["log_bytes"] for v in trials
                ),
                "files_per_second": statistics.median(
                    v["business"]["result"]["files_per_second"] for v in trials
                ),
                "worker_requests_per_file": statistics.median(
                    sum(v["business"]["result"]["worker_requests_by_route"].values()) / args.files
                    for v in trials
                ),
            }
            for name, trials in reports.items()
        }
        result = {
            "configuration": {
                "files": args.files,
                "interval": args.interval,
                "repetitions": args.repetitions,
                "idle_seconds": args.idle_seconds,
                "business_store": "postgres" if args.dsn else "sqlite",
            },
            "summary": summary,
            "trials": reports,
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
