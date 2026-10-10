"""A Node dev proxy, real controller and real free-threaded executor."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from dataflowcore.client import APIError, Client
from dataflowcore.contracts import TaskSpec
from dataflowcore.store import Store

repo = Path(__file__).resolve().parents[2]
root = repo / ".e2e" / "console"
root.mkdir(parents=True, exist_ok=True)
source = root / "input.txt"
source.write_text("DataFlow 文档业务验证 hello world\n" * 100, encoding="utf-8")
state = root / "state.db"
state.unlink(missing_ok=True)
dsn = "sqlite:///" + str(state)
store = Store(dsn)
store.migrate()
for i in range(35):
    store.submit(
        TaskSpec.parse(
            {
                "name": f"历史文件 {i:02d}",
                "input_path": str(source),
                "pool": "archive",
                "steps": [{"id": "read", "callable": "dataflowcore.operators:read_text"}],
            }
        ),
        f"seed-{i}",
    )
processes, streams = [], []
env = os.environ | {
    "PYTHONPATH": str(repo / "src"),
    "DATAFLOW_ADMIN_TOKEN": "a" * 32,
    "DATAFLOW_WORKER_TOKEN": "w" * 32,
}


def launch(*args):
    log = (root / f"service-{len(processes)}.log").open("wb")
    streams.append(log)
    processes.append(
        subprocess.Popen(
            [sys.executable, "-m", "dataflowcore.cli", *args],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    )


launch(
    "control",
    "--dsn",
    dsn,
    "--port",
    "8086",
    "--data-root",
    str(root),
    "--cors-origins",
    "http://127.0.0.1:8000",
    "--lease",
    "3",
    "--upload-max-bytes",
    str(3 * 1024 * 1024),
)
client = Client("http://127.0.0.1:8086", "a" * 32)
for _ in range(100):
    try:
        client.request("GET", "/readyz")
        break
    except OSError, APIError:
        time.sleep(0.1)
else:
    raise RuntimeError("controller did not start")
launch(
    "worker",
    "--url",
    client.url,
    "--data-root",
    str(root),
    "--work-root",
    str(root / "worker"),
    "--slots",
    "2",
    "--cpu",
    "4",
    "--interval",
    "0.2",
    "--stop-grace",
    "0.2",
)
server = subprocess.Popen(
    [
        "node",
        str(repo / "frontend/dev-server.mjs"),
        "--host",
        "127.0.0.1",
        "--port",
        "8000",
        "--api-upstream",
        client.url,
    ]
)
processes.append(server)


def stop(*_):
    raise KeyboardInterrupt


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
try:
    while server.poll() is None:
        time.sleep(0.1)
    raise RuntimeError("frontend dev server exited unexpectedly")
except KeyboardInterrupt:
    pass
finally:
    for process in reversed(processes):
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
    for stream in streams:
        stream.close()
