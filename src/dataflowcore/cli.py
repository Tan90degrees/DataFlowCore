"""Control plane, executor and user CLI entrypoints."""

import argparse
import json
import logging
import os
import time
from pathlib import Path

from . import __version__
from .client import Client
from .runtime import child_main, require_free_threading


def parser():
    p = argparse.ArgumentParser(prog="dataflow")
    p.add_argument("--version", action="version", version=__version__)
    commands = p.add_subparsers(dest="command", required=True)
    control = commands.add_parser("control", help="run control plane")
    control.add_argument(
        "--dsn", default=os.getenv("DATAFLOW_DATABASE_URL", "sqlite:///dataflow.db")
    )
    control.add_argument("--host", default="127.0.0.1")
    control.add_argument("--port", type=int, default=8080)
    control.add_argument("--data-root", default=os.getenv("DATAFLOW_DATA_ROOT", "."))
    control.add_argument("--lease", type=float, default=30)
    control.add_argument("--insecure", action="store_true", help="development only")
    control.add_argument("--allow-gil", action="store_true", help="development only")
    worker = commands.add_parser("worker", help="run executor supervisor")
    worker.add_argument("--url", default=os.getenv("DATAFLOW_URL", "http://127.0.0.1:8080"))
    worker.add_argument("--data-root", default=os.getenv("DATAFLOW_DATA_ROOT", "."))
    worker.add_argument("--work-root", default=os.getenv("DATAFLOW_WORK_ROOT", ".dataflow-worker"))
    worker.add_argument("--slots", type=int, default=1)
    worker.add_argument("--cpu", type=int, default=2)
    worker.add_argument("--memory-mb", type=int, default=1024)
    worker.add_argument("--pool", default="default")
    worker.add_argument("--interval", type=float, default=2)
    worker.add_argument("--stop-grace", type=float, default=5)
    worker.add_argument("--allow-gil", action="store_true", help="development only")
    worker.add_argument("--runtime-version", default=__version__)
    for name in ("submit", "get", "cancel", "retry", "events", "list", "workers", "wait"):
        sub = commands.add_parser(name)
        sub.add_argument("--url", default=os.getenv("DATAFLOW_URL", "http://127.0.0.1:8080"))
        sub.add_argument("--key", default=None)
        if name not in ("list", "workers"):
            sub.add_argument("value", help="spec JSON path" if name == "submit" else "task id")
        if name == "wait":
            sub.add_argument("--timeout", type=float, default=3600)
    commands.add_parser("doctor", help="verify actual free-threaded runtime")
    migrate = commands.add_parser("migrate")
    migrate.add_argument(
        "--dsn", default=os.getenv("DATAFLOW_DATABASE_URL", "sqlite:///dataflow.db")
    )
    health = commands.add_parser("worker-health")
    health.add_argument("--work-root", default=os.getenv("DATAFLOW_WORK_ROOT", ".dataflow-worker"))
    health.add_argument("--ready", action="store_true")
    run = commands.add_parser("_run", help=argparse.SUPPRESS)
    run.add_argument("assignment")
    run.add_argument("--workspace", required=True)
    run.add_argument("--data-root", required=True)
    run.add_argument("--parent-pid", type=int, required=True)
    run.add_argument("--allow-gil", action="store_true")
    return p


def main():
    args = parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.command == "control":
        from .api import serve

        require_free_threading(args.allow_gil)
        serve(
            args.dsn,
            args.host,
            args.port,
            args.data_root,
            os.getenv("DATAFLOW_ADMIN_TOKEN", ""),
            os.getenv("DATAFLOW_WORKER_TOKEN", ""),
            args.insecure,
            args.lease,
        )
    elif args.command == "worker":
        from .worker import Worker

        Worker(
            args.url,
            os.getenv("DATAFLOW_WORKER_TOKEN", ""),
            args.data_root,
            args.work_root,
            args.slots,
            args.cpu,
            args.memory_mb,
            args.pool,
            args.interval,
            args.stop_grace,
            args.allow_gil,
            runtime_version=args.runtime_version,
        ).run()
    elif args.command == "_run":
        from .worker import parent_death_signal

        parent_death_signal(args.parent_pid)
        child_main(args.assignment, args.workspace, args.data_root, args.allow_gil)
    elif args.command == "doctor":
        print(json.dumps(require_free_threading(), indent=2))
    elif args.command == "migrate":
        from .store import Store

        Store(args.dsn).migrate()
        print("schema version 1 ready")
    elif args.command == "worker-health":
        health = json.loads((Path(args.work_root) / "health.json").read_text())
        if time.time() - health["updated_at"] > 15:
            raise SystemExit(1)
        if args.ready and (not health["registered"] or health["draining"]):
            raise SystemExit(1)
    else:
        client = Client(args.url, os.getenv("DATAFLOW_ADMIN_TOKEN", ""))
        if args.command == "submit":
            result = client.submit(json.loads(Path(args.value).read_text()), args.key)
        elif args.command in ("get", "cancel"):
            result = getattr(client, args.command)(args.value)
        elif args.command == "retry":
            result = client.retry(args.value, args.key)
        elif args.command in ("list", "workers", "events"):
            path = (
                f"/v1/tasks/{args.value}/events"
                if args.command == "events"
                else "/v1/tasks"
                if args.command == "list"
                else "/v1/workers"
            )
            result = client.request("GET", path)
        elif args.command == "wait":
            deadline = time.monotonic() + args.timeout
            while True:
                result = client.get(args.value)
                if result["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                    break
                if time.monotonic() >= deadline:
                    raise SystemExit("wait timed out")
                time.sleep(1)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            raise SystemExit(0 if result["state"] == "SUCCEEDED" else 1)
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
