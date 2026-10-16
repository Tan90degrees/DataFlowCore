"""Control plane, executor and user CLI entrypoints."""

import argparse
import json
import logging
import os
import time
from pathlib import Path

from . import __version__
from .client import Client
from .runtime import check_runtime, child_main
from .uploads import DEFAULT_DIR


def runtime_options(command):
    mode = command.add_mutually_exclusive_group()
    mode.add_argument(
        "--require-free-threading",
        dest="allow_gil",
        action="store_false",
        help="reject a GIL-enabled Python runtime, including after operator imports",
    )
    mode.add_argument("--allow-gil", action="store_true", help=argparse.SUPPRESS)
    command.set_defaults(allow_gil=None)


def slots_value(value):
    if value == "auto":
        return None
    try:
        count = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("slots must be auto or a positive integer") from exc
    if count < 1:
        raise argparse.ArgumentTypeError("slots must be auto or a positive integer")
    return count


def pool_width(value):
    count = slots_value(value)
    if count is not None and count > 256:
        raise argparse.ArgumentTypeError("pool width must be auto or an integer in 1..256")
    return count


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
    control.add_argument(
        "--upload-dir",
        default=os.getenv("DATAFLOW_UPLOAD_DIR", DEFAULT_DIR),
        help="upload directory relative to data-root, or an absolute path inside it",
    )
    control.add_argument("--lease", type=float, default=30)
    control.add_argument(
        "--upload-max-bytes",
        type=int,
        default=int(os.getenv("DATAFLOW_UPLOAD_MAX_BYTES", str(256 * 1024 * 1024))),
        help="maximum bytes per uploaded file; 0 disables uploads",
    )
    control.add_argument(
        "--upload-concurrency", type=int, default=int(os.getenv("DATAFLOW_UPLOAD_CONCURRENCY", "4"))
    )
    control.add_argument(
        "--upload-timeout", type=float, default=float(os.getenv("DATAFLOW_UPLOAD_TIMEOUT", "300"))
    )
    control.add_argument(
        "--cors-origins",
        default=os.getenv("DATAFLOW_CORS_ORIGINS", ""),
        help="comma-separated exact browser origins; empty disables cross-origin access",
    )
    control.add_argument("--insecure", action="store_true", help=argparse.SUPPRESS)
    runtime_options(control)
    control.add_argument(
        "--max-unfinished", type=int, default=int(os.getenv("DATAFLOW_MAX_UNFINISHED", "10000"))
    )
    control.add_argument(
        "--db-timeout-ms", type=int, default=int(os.getenv("DATAFLOW_DB_TIMEOUT_MS", "5000"))
    )
    control.add_argument(
        "--upload-min-free-bytes",
        type=int,
        default=int(os.getenv("DATAFLOW_UPLOAD_MIN_FREE_BYTES", "67108864")),
    )
    control.add_argument(
        "--log-format", choices=("text", "json"), default=os.getenv("DATAFLOW_LOG_FORMAT", "json")
    )
    worker = commands.add_parser("worker", help="run executor supervisor")
    worker.add_argument("--url", default=os.getenv("DATAFLOW_URL", "http://127.0.0.1:8080"))
    worker.add_argument("--data-root", default=os.getenv("DATAFLOW_DATA_ROOT", "."))
    worker.add_argument("--work-root", default=os.getenv("DATAFLOW_WORK_ROOT", ".dataflow-worker"))
    worker.add_argument(
        "--slots",
        type=slots_value,
        default=None,
        help="resident file processes; auto uses --cpu with GIL (up to 256), otherwise 1",
    )
    worker.add_argument("--cpu", type=int, default=2)
    worker.add_argument("--memory-mb", type=int, default=1024)
    worker.add_argument("--pool", default="default")
    worker.add_argument("--interval", type=float, default=2)
    worker.add_argument("--stop-grace", type=float, default=5)
    runtime_options(worker)
    worker.add_argument("--runtime-version", default=__version__)
    worker.add_argument(
        "--runner-dag-workers",
        type=pool_width,
        default=None,
        help="CPU DAG pool width; auto uses 1 with GIL, otherwise 8",
    )
    worker.add_argument("--runner-map-workers", type=int, default=8)
    worker.add_argument("--runner-max-tasks", type=int, default=100)
    worker.add_argument("--runner-io-workers", type=int, default=16)
    worker.add_argument("--runner-io-map-workers", type=int, default=16)
    worker.add_argument("--io-concurrency", type=int, default=32)
    worker.add_argument("--resource-aging-seconds", type=float, default=2)
    for name in ("submit", "get", "cancel", "retry", "events", "list", "workers", "wait", "drain"):
        sub = commands.add_parser(name)
        sub.add_argument("--url", default=os.getenv("DATAFLOW_URL", "http://127.0.0.1:8080"))
        sub.add_argument("--key", default=None)
        if name not in ("list", "workers"):
            sub.add_argument("value", help="spec JSON path" if name == "submit" else "task id")
        if name == "wait":
            sub.add_argument("--timeout", type=float, default=3600)
        if name == "list":
            sub.add_argument("--state")
            sub.add_argument("--pool")
            sub.add_argument("--limit", type=int, default=100)
            sub.add_argument("--cursor")
    runtime_options(
        commands.add_parser("doctor", help="inspect and validate Python execution mode")
    )
    migrate = commands.add_parser("migrate")
    migrate.add_argument(
        "--dsn", default=os.getenv("DATAFLOW_DATABASE_URL", "sqlite:///dataflow.db")
    )
    cleanup = commands.add_parser(
        "gc-failed-outputs", help="preview or remove old failed output directories"
    )
    cleanup.add_argument(
        "--dsn", default=os.getenv("DATAFLOW_DATABASE_URL", "sqlite:///dataflow.db")
    )
    cleanup.add_argument("--data-root", default=os.getenv("DATAFLOW_DATA_ROOT", "."))
    cleanup.add_argument("--days", type=float, default=7)
    cleanup.add_argument("--limit", type=int, default=200)
    cleanup.add_argument(
        "--apply", action="store_true", help="delete eligible failed outputs; default is dry-run"
    )
    health = commands.add_parser("worker-health")
    health.add_argument("--work-root", default=os.getenv("DATAFLOW_WORK_ROOT", ".dataflow-worker"))
    health.add_argument("--ready", action="store_true")
    run = commands.add_parser("_run", help=argparse.SUPPRESS)
    run.add_argument("assignment")
    run.add_argument("--workspace", required=True)
    run.add_argument("--data-root", required=True)
    run.add_argument("--parent-pid", type=int, required=True)
    runtime_options(run)
    runner = commands.add_parser("_runner", help=argparse.SUPPRESS)
    runner.add_argument("--workspace", required=True)
    runner.add_argument("--data-root", required=True)
    runner.add_argument("--parent-pid", type=int, required=True)
    runner.add_argument("--dag-workers", type=int, required=True)
    runner.add_argument("--map-workers", type=int, required=True)
    runner.add_argument("--max-tasks", type=int, required=True)
    runtime_options(runner)
    runner.add_argument("--budget-path")
    runner.add_argument("--io-workers", type=int, default=16)
    runner.add_argument("--io-map-workers", type=int, default=16)
    runner.add_argument("--notify-fd", type=int)
    runner.add_argument("--completion-fd", type=int)
    return p


def main():
    args = parser().parse_args()
    logging.basicConfig(
        level=os.getenv("DATAFLOW_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if getattr(args, "log_format", os.getenv("DATAFLOW_LOG_FORMAT", "json")) == "json":
        from .observability import JSONFormatter

        for handler in logging.getLogger().handlers:
            handler.setFormatter(JSONFormatter())
    if args.command == "control":
        from .api import serve

        check_runtime(args.allow_gil)
        serve(
            args.dsn,
            args.host,
            args.port,
            args.data_root,
            "",
            "",
            args.insecure,
            args.lease,
            cors_origins=[
                origin.strip() for origin in args.cors_origins.split(",") if origin.strip()
            ],
            upload_max_bytes=args.upload_max_bytes,
            upload_concurrency=args.upload_concurrency,
            upload_timeout=args.upload_timeout,
            upload_dir=args.upload_dir,
            upload_min_free_bytes=args.upload_min_free_bytes,
            max_unfinished=args.max_unfinished,
            db_timeout_ms=args.db_timeout_ms,
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
            runner_dag_workers=args.runner_dag_workers,
            runner_map_workers=args.runner_map_workers,
            runner_max_tasks=args.runner_max_tasks,
            runner_io_workers=args.runner_io_workers,
            runner_io_map_workers=args.runner_io_map_workers,
            io_concurrency=args.io_concurrency,
            resource_aging_seconds=args.resource_aging_seconds,
        ).run()
    elif args.command == "_runner":
        from .runner import runner_main

        runner_main(
            args.workspace,
            args.data_root,
            args.parent_pid,
            args.dag_workers,
            args.map_workers,
            args.max_tasks,
            args.allow_gil,
            args.budget_path,
            args.io_workers,
            args.io_map_workers,
            args.notify_fd,
            args.completion_fd,
        )
    elif args.command == "_run":
        from .worker import parent_death_signal

        parent_death_signal(args.parent_pid)
        child_main(args.assignment, args.workspace, args.data_root, args.allow_gil)
    elif args.command == "doctor":
        print(json.dumps(check_runtime(args.allow_gil), indent=2))
    elif args.command == "migrate":
        from .store import Store

        Store(args.dsn).migrate()
        print("schema version 1 ready")
    elif args.command == "gc-failed-outputs":
        from .maintenance import clean_failed_outputs
        from .store import Store

        print(
            json.dumps(
                clean_failed_outputs(
                    Store(args.dsn), args.data_root, args.days, args.limit, args.apply
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
    elif args.command == "worker-health":
        health = json.loads((Path(args.work_root) / "health.json").read_text())
        if time.time() - health["updated_at"] > 15 or health.get("watchdog_age_seconds", 0) > 5:
            raise SystemExit(1)
        if args.ready and (
            not health["registered"]
            or health["draining"]
            or health.get("live_runners", 0) < health.get("runner_slots", 1)
            or health.get("control_contact_age_seconds", 1e9) >= health.get("lease_seconds", 30)
        ):
            raise SystemExit(1)
    else:
        client = Client(args.url, os.getenv("DATAFLOW_ADMIN_TOKEN", ""))
        if args.command == "submit":
            result = client.submit(json.loads(Path(args.value).read_text()), args.key)
        elif args.command in ("get", "cancel", "drain"):
            result = getattr(client, args.command)(args.value)
        elif args.command == "retry":
            result = client.retry(args.value, args.key)
        elif args.command == "list":
            from urllib.parse import urlencode

            query = {
                "limit": args.limit,
                "summary": 1,
                "state": args.state,
                "pool": args.pool,
                "cursor": args.cursor,
            }
            result = client.request(
                "GET", "/v1/tasks?" + urlencode({k: v for k, v in query.items() if v is not None})
            )
        elif args.command in ("workers", "events"):
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
