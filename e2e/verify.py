"""Acceptance evidence from real Kubernetes Pod faults."""

import hashlib
import json
import os
import sqlite3
import subprocess
import time
from pathlib import Path

from dataflowcore.client import APIError, Client
from dataflowcore.examples.ingestion import fixture_vector, pipeline, records

client = Client("http://127.0.0.1:18080", os.environ["DATAFLOW_ADMIN_TOKEN"], timeout=3)


def kubectl(*args):
    return subprocess.check_output(["kubectl", *args], text=True)


def until(check, seconds=120):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except OSError, APIError:
            pass
        time.sleep(0.5)
    raise AssertionError("acceptance condition timed out")


def state(task_id, expected):
    return until(lambda: task if (task := client.get(task_id))["state"] == expected else None)


def submit(seconds):
    flow = pipeline(
        retry_delay=0,
        timeout=180,
        parameters={
            "embedding_url": "http://embedding-fixture:8081",
            "receipt_delay": seconds,
            "document_id": f"k8s-business-{seconds}",
        },
    )
    return client.submit(flow.spec("/dataflow/input.txt"))


def verify_business(task):
    result = task["result"]
    output = Path(".e2e/data") / Path(result["output_dir"]).relative_to("/dataflow")
    for item in result["files"]:
        assert hashlib.sha256((output / item["path"]).read_bytes()).hexdigest() == item["sha256"]
    vectors = list(records(output / "vectors.jsonl"))
    assert all(row["vector"] == fixture_vector(row["text"]) for row in vectors)
    receipt = result["steps"]["receipt"]
    with sqlite3.connect(".e2e/data/index.sqlite") as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM chunks WHERE document = ? AND version = ?",
            (receipt["document_id"], receipt["version"]),
        ).fetchone()[0]
    assert count == receipt["chunks"] == len(vectors)
    assert task["attempts"][-1]["progress"]["fraction"] == 1
    assert result["runtime"]["gil_enabled"] is False


until(lambda: client.request("GET", "/readyz"))
until(lambda: len([w for w in client.request("GET", "/v1/workers")["workers"] if w["online"]]) >= 2)
evidence = {}

task = submit(8)
running = state(task["id"], "RUNNING")
until(
    lambda: (
        client.get(task["id"])["attempts"][0]["progress"]
        .get("steps", {})
        .get("receipt", {})
        .get("state")
        == "RUNNING"
    )
)
session = running["attempts"][0]["worker_session"]
name = next(
    w["name"] for w in client.request("GET", "/v1/workers")["workers"] if w["session_id"] == session
)
kubectl("delete", "pod", name, "--grace-period=0", "--force", "--wait=false")
finished = state(task["id"], "SUCCEEDED")
assert [a["state"] for a in finished["attempts"]] == ["LOST", "SUCCEEDED"]
assert finished["result"]["runtime"]["gil_enabled"] is False
assert finished["attempts"][1]["worker_session"] != session
verify_business(finished)
evidence["pod_fault"] = finished

# Control restart retains metadata and recovers in-flight execution.
task = submit(15)
state(task["id"], "RUNNING")
kubectl("rollout", "restart", "deployment/dataflowcore-control")
kubectl("rollout", "status", "deployment/dataflowcore-control", "--timeout=120s")
# Port-forward dies with the old control Pod; reconnect to its Service.
forward = subprocess.Popen(
    ["kubectl", "port-forward", "service/dataflowcore-control", "18081:8080"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
try:
    client = Client("http://127.0.0.1:18081", os.environ["DATAFLOW_ADMIN_TOKEN"], timeout=3)
    finished = state(task["id"], "SUCCEEDED")
    verify_business(finished)
    evidence["control_restart"] = finished
    # Accept either continued execution or full retry if downtime exceeded the lease.
    assert finished["attempt_count"] <= 3
    stopped = submit(60)
    state(stopped["id"], "RUNNING")
    client.cancel(stopped["id"])
    cancelled = state(stopped["id"], "CANCELLED")
    assert cancelled["attempt_count"] == 1
    evidence["cancel"] = cancelled
    # Drain both executors, keep a queued business file, then replace them.
    live = [w for w in client.request("GET", "/v1/workers")["workers"] if w["online"]]
    for worker in live:
        client.drain(worker["session_id"])
    queued = submit(0)
    time.sleep(2)
    assert client.get(queued["id"])["state"] == "QUEUED"
    kubectl("rollout", "restart", "deployment/dataflowcore-worker")
    kubectl("rollout", "status", "deployment/dataflowcore-worker", "--timeout=120s")
    finished = state(queued["id"], "SUCCEEDED")
    verify_business(finished)
    evidence["drain_and_replace"] = finished
    # Pin consecutive files to one single-slot executor and verify warm reuse.
    live = [w for w in client.request("GET", "/v1/workers")["workers"] if w["online"]]
    selected = finished["attempts"][-1]["worker_session"]
    for worker in live:
        if worker["session_id"] != selected:
            client.drain(worker["session_id"])
    first = state(submit(0)["id"], "SUCCEEDED")
    second = state(submit(0)["id"], "SUCCEEDED")
    verify_business(first)
    verify_business(second)
    assert first["attempts"][-1]["worker_session"] == selected
    assert second["attempts"][-1]["worker_session"] == selected
    a, b = first["result"]["runtime"]["runner"], second["result"]["runtime"]["runner"]
    assert a["pid"] == b["pid"]
    assert b["tasks_before"] == a["tasks_before"] + 1
    evidence["resident_reuse"] = {"first": first, "second": second}
finally:
    forward.terminate()
    forward.wait(timeout=5)
Path(".e2e/evidence.json").write_text(json.dumps(evidence, indent=2))
print(
    "Kubernetes business acceptance passed: artifacts, idempotent index, Pod recovery, "
    "control restart, cancellation, drain, resident PID reuse, no GIL"
)
