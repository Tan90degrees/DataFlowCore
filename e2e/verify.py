"""Acceptance evidence from real Kubernetes Pod faults."""

import json
import os
import subprocess
import time
from pathlib import Path

from dataflowcore.client import APIError, Client

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
    return client.submit(
        {
            "input_path": "/dataflow/input.txt",
            "retry_delay": 0,
            "timeout": 180,
            "steps": [
                {
                    "id": "delay",
                    "callable": "dataflowcore.operators:delay",
                    "parameters": {"seconds": seconds},
                },
                {
                    "id": "write",
                    "callable": "dataflowcore.operators:write_json",
                    "depends_on": ["delay"],
                },
            ],
        }
    )


until(lambda: client.request("GET", "/readyz"))
until(lambda: len([w for w in client.request("GET", "/v1/workers")["workers"] if w["online"]]) >= 2)
evidence = {}

task = submit(8)
running = state(task["id"], "RUNNING")
until(lambda: client.get(task["id"])["attempts"][0]["progress"].get("steps"))
session = running["attempts"][0]["worker_session"]
name = next(
    w["name"] for w in client.request("GET", "/v1/workers")["workers"] if w["session_id"] == session
)
kubectl("delete", "pod", name, "--grace-period=0", "--force", "--wait=false")
finished = state(task["id"], "SUCCEEDED")
assert [a["state"] for a in finished["attempts"]] == ["LOST", "SUCCEEDED"]
assert finished["result"]["runtime"]["gil_enabled"] is False
assert finished["attempts"][1]["worker_session"] != session
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
    evidence["control_restart"] = finished
    # Accept either continued execution or full retry if downtime exceeded the lease.
    assert finished["attempt_count"] <= 3
    stopped = submit(60)
    state(stopped["id"], "RUNNING")
    client.cancel(stopped["id"])
    cancelled = state(stopped["id"], "CANCELLED")
    assert cancelled["attempt_count"] == 1
    evidence["cancel"] = cancelled
finally:
    forward.terminate()
    forward.wait(timeout=5)
Path(".e2e/evidence.json").write_text(json.dumps(evidence, indent=2))
print("Kubernetes acceptance passed: Pod recovery, control restart, cancellation, no GIL")
