"""Real control/worker/task subprocesses. No mocked executor or DAG runtime."""

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from dataflowcore.client import APIError, Client


def until(check, timeout=20):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            result = check()
            if result:
                return result
        except OSError, APIError:
            pass
        time.sleep(0.05)
    raise AssertionError("condition did not become true")


class Cluster:
    def __init__(self, root, dsn):
        self.root, self.dsn, self.processes = root, dsn, []
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.env = os.environ | {
            "PYTHONPATH": str(Path(__file__).parents[1] / "src"),
            "DATAFLOW_ADMIN_TOKEN": "a" * 32,
            "DATAFLOW_WORKER_TOKEN": "w" * 32,
        }
        self.client = Client(f"http://127.0.0.1:{self.port}", "a" * 32, timeout=2)
        self.control = self.start_control()

    def launch(self, *args):
        log = (self.root / f"process-{len(self.processes)}.log").open("wb")
        process = subprocess.Popen(
            [sys.executable, "-m", "dataflowcore.cli", *args],
            env=self.env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.processes.append((process, log))
        return process

    def start_control(self):
        process = self.launch(
            "control",
            "--dsn",
            self.dsn,
            "--port",
            str(self.port),
            "--data-root",
            str(self.root),
            "--lease",
            "3",
        )
        until(lambda: self.client.request("GET", "/readyz"))
        return process

    def worker(self, slots=1, *extra):
        return self.launch(
            "worker",
            "--url",
            self.client.url,
            "--data-root",
            str(self.root),
            "--work-root",
            str(self.root / f"worker-{len(self.processes)}"),
            "--interval",
            "0.2",
            "--stop-grace",
            "0.2",
            "--slots",
            str(slots),
            *extra,
        )

    def submit(self, seconds=0, steps=None, **changes):
        source = self.root / "input.txt"
        source.write_text("hello world hello")
        spec = {
            "input_path": str(source),
            "retry_delay": 0,
            "timeout": 20,
            "steps": steps
            or [
                {
                    "id": "delay",
                    "callable": "dataflowcore.operators:delay",
                    "parameters": {"seconds": seconds},
                }
            ],
        }
        return self.client.submit(spec | changes)

    def state(self, tid, state):
        return until(lambda: task if (task := self.client.get(tid))["state"] == state else None)

    def close(self):
        for process, log in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
            log.close()


def test_actual_file_dag_output_and_gil(cluster):
    cluster.worker()
    task = cluster.submit(
        steps=[
            {"id": "read", "callable": "dataflowcore.operators:read_text"},
            {
                "id": "count",
                "callable": "dataflowcore.operators:word_count",
                "depends_on": ["read"],
            },
            {
                "id": "write",
                "callable": "dataflowcore.operators:write_json",
                "depends_on": ["count"],
            },
        ]
    )
    final = cluster.state(task["id"], "SUCCEEDED")
    assert final["result"]["steps"]["count"]["words"] == 3
    assert final["result"]["runtime"]["gil_enabled"] is False
    assert final["result"]["runtime"]["free_threaded_build"] is True
    assert Path(final["result"]["output_dir"], "write.json").is_file()
    assert final["attempts"][0]["progress"]["completed_steps"] == 3
    assert len(final["attempts"]) == 1


def test_killed_worker_automatically_retries_entire_file(cluster):
    first = cluster.worker()
    task = cluster.submit(seconds=1.5)
    running = cluster.state(task["id"], "RUNNING")
    until(lambda: cluster.client.get(task["id"])["attempts"][0]["progress"].get("steps"))
    first.kill()
    first.wait(timeout=3)
    cluster.worker()
    final = cluster.state(task["id"], "SUCCEEDED")
    assert [a["state"] for a in final["attempts"]] == ["LOST", "SUCCEEDED"]
    assert final["attempts"][1]["worker_session"] != running["attempts"][0]["worker_session"]


def test_control_restart_preserves_existing_attempt(cluster):
    cluster.worker()
    task = cluster.submit(seconds=1.5)
    running = cluster.state(task["id"], "RUNNING")
    cluster.control.terminate()
    cluster.control.wait(timeout=3)
    cluster.control = cluster.start_control()
    final = cluster.state(task["id"], "SUCCEEDED")
    assert len(final["attempts"]) == 1
    assert final["current_attempt"] == running["current_attempt"]


def test_running_cancel_does_not_retry_and_releases_slot(cluster):
    cluster.worker()
    task = cluster.submit(seconds=30)
    cluster.state(task["id"], "RUNNING")
    cluster.client.cancel(task["id"])
    final = cluster.state(task["id"], "CANCELLED")
    assert len(final["attempts"]) == 1
    next_task = cluster.submit()
    assert cluster.state(next_task["id"], "SUCCEEDED")


def test_two_executors_run_separate_files_concurrently(cluster):
    cluster.worker()
    cluster.worker()
    first = cluster.submit(seconds=1)
    second = cluster.submit(seconds=1)
    a = cluster.state(first["id"], "RUNNING")
    b = cluster.state(second["id"], "RUNNING")
    assert a["attempts"][0]["worker_session"] != b["attempts"][0]["worker_session"]
    cluster.state(first["id"], "SUCCEEDED")
    cluster.state(second["id"], "SUCCEEDED")


def test_auth_roles_and_input_path_validation(cluster):
    with pytest.raises(APIError) as error:
        Client(cluster.client.url).request("GET", "/v1/tasks")
    assert error.value.status == 401
    with pytest.raises(APIError) as error:
        Client(cluster.client.url, "w" * 32).request("GET", "/v1/tasks")
    assert error.value.status == 401
    with pytest.raises(APIError) as error:
        cluster.submit(input_path="/etc/passwd")
    assert error.value.status == 400


def test_tampered_input_is_permanent_failure(cluster):
    task = cluster.submit()
    (cluster.root / "input.txt").write_text("changed input")
    cluster.worker()
    final = cluster.state(task["id"], "FAILED")
    assert len(final["attempts"]) == 1
    assert "changed" in final["error"]


def test_parallel_dag_branches(cluster):
    cluster.worker()
    task = cluster.submit(
        steps=[
            {
                "id": "left",
                "callable": "dataflowcore.operators:delay",
                "parameters": {"seconds": 0.5},
            },
            {
                "id": "right",
                "callable": "dataflowcore.operators:delay",
                "parameters": {"seconds": 0.5},
            },
            {
                "id": "join",
                "callable": "dataflowcore.operators:write_json",
                "depends_on": ["left", "right"],
            },
        ]
    )
    final = cluster.state(task["id"], "SUCCEEDED")
    output = json.loads(Path(final["result"]["steps"]["join"]["path"]).read_text())
    assert set(output) == {"left", "right"}


def test_uncooperative_operator_force_cancel(cluster):
    # Installed test operator deliberately ignores cooperative cancellation.
    operator = cluster.root / "stuck.py"
    operator.write_text("import time\ndef run(context, inputs):\n    time.sleep(60)\n")
    cluster.env["PYTHONPATH"] += os.pathsep + str(cluster.root)
    cluster.worker()
    task = cluster.submit(steps=[{"id": "stuck", "callable": "stuck:run"}])
    cluster.state(task["id"], "RUNNING")
    until(lambda: cluster.client.get(task["id"])["attempts"][0]["progress"].get("steps"))
    cluster.client.cancel(task["id"])
    assert cluster.state(task["id"], "CANCELLED")
    next_task = cluster.submit()
    assert cluster.state(next_task["id"], "SUCCEEDED")


def test_supervisor_kill_terminates_task_child(cluster):
    worker = cluster.worker()
    task = cluster.submit(seconds=30)
    cluster.state(task["id"], "RUNNING")
    assignment_file = until(lambda: next(cluster.root.glob("worker-*/*/*/assignment.json"), None))
    workspace = assignment_file.parent
    # The attempt records the assigned resident slot process on Linux.
    if sys.platform != "linux":
        pytest.skip("Linux parent-death signal")

    pid = json.loads((workspace / "runner.json").read_text())["pid"]
    worker.send_signal(signal.SIGKILL)
    worker.wait(timeout=3)

    def dead():
        try:
            return Path(f"/proc/{pid}/stat").read_text().split(") ")[1][0] == "Z"
        except FileNotFoundError:
            return True

    assert until(dead)


def test_noisy_operator_has_bounded_durable_log_tail(cluster):
    operator = cluster.root / "noisy.py"
    operator.write_text("def run(context, inputs):\n    print('x' * 100000)\n    return {}\n")
    cluster.env["PYTHONPATH"] += os.pathsep + str(cluster.root)
    cluster.worker()
    task = cluster.submit(steps=[{"id": "noisy", "callable": "noisy:run"}])
    final = cluster.state(task["id"], "SUCCEEDED")
    tail = final["attempts"][0]["progress"]["log_tail"]
    assert 1000 < len(tail) <= 16384
    assert not list(cluster.root.glob("worker-*/*/*/assignment.json"))


def test_oversized_combined_result_fails_without_endless_submission(cluster):
    operator = cluster.root / "large.py"
    operator.write_text("def run(context, inputs):\n    return {'data': 'x' * 240000}\n")
    cluster.env["PYTHONPATH"] += os.pathsep + str(cluster.root)
    cluster.worker()
    task = cluster.submit(steps=[{"id": f"s{i}", "callable": "large:run"} for i in range(4)])
    final = cluster.state(task["id"], "FAILED")
    assert final["attempt_count"] == 1
    assert "combined result too large" in final["error"]


def test_failed_dag_stops_uncooperative_sibling_and_releases_slot(cluster):
    operator = cluster.root / "sibling.py"
    operator.write_text("import time\ndef run(context, inputs):\n    time.sleep(60)\n")
    cluster.env["PYTHONPATH"] += os.pathsep + str(cluster.root)
    cluster.worker()
    task = cluster.submit(
        steps=[
            {"id": "stuck", "callable": "sibling:run"},
            {"id": "bad", "callable": "dataflowcore.operators:fail"},
        ]
    )
    final = cluster.state(task["id"], "FAILED")
    assert final["attempt_count"] == 1
    assert cluster.state(cluster.submit()["id"], "SUCCEEDED")


def test_control_outage_beyond_lease_forces_full_retry(cluster):
    cluster.worker()
    task = cluster.submit(seconds=5)
    cluster.state(task["id"], "RUNNING")
    cluster.control.kill()
    cluster.control.wait(timeout=3)
    # This wait models an actual outage exceeding the configured three-second lease.
    time.sleep(4)
    cluster.control = cluster.start_control()
    final = cluster.state(task["id"], "SUCCEEDED")
    assert [a["state"] for a in final["attempts"]] == ["LOST", "SUCCEEDED"]


def test_submission_configuration_and_upstream_inputs_are_independent(cluster):
    cluster.worker()
    task = cluster.submit(
        parameters={"encoding": "utf-8", "label": "task", "nested": {"flags": ["global"]}},
        steps=[
            {"id": "read", "callable": "dataflowcore.operators:read_text"},
            {
                "id": "count",
                "callable": "dataflowcore.operators:word_count",
                "depends_on": ["read"],
            },
            {
                "id": "observe",
                "callable": "tests.parameter_operators:snapshot",
                "depends_on": ["read", "count"],
                "parameters": {"label": "node", "nested": {"flags": ["local"]}},
            },
        ],
    )
    final = cluster.state(task["id"], "SUCCEEDED")
    observed = final["result"]["steps"]["observe"]
    assert observed["parameters"] == {
        "encoding": "utf-8",
        "label": "node",
        "nested": {"flags": ["local"]},
    }
    assert observed["inputs"]["read"]["text"] == "hello world hello"
    assert observed["inputs"]["count"]["words"] == 3
    assert final["spec"]["parameters"]["label"] == "task"
    assert final["spec"]["steps"][-1]["parameters"]["label"] == "node"


def test_nested_configuration_isolated_and_manual_retry_preserves_spec(cluster):
    cluster.worker()
    task = cluster.submit(
        parameters={"nested": {"flags": ["base"]}},
        steps=[
            {
                "id": "mutate",
                "callable": "tests.parameter_operators:snapshot",
                "parameters": {"mutate": True},
            },
            {
                "id": "observe",
                "callable": "tests.parameter_operators:snapshot",
                "depends_on": ["mutate"],
            },
        ],
    )
    final = cluster.state(task["id"], "SUCCEEDED")
    assert final["result"]["steps"]["mutate"]["parameters"]["nested"]["flags"] == [
        "base",
        "changed",
    ]
    assert final["result"]["steps"]["observe"]["parameters"]["nested"]["flags"] == ["base"]
    assert final["spec"]["parameters"]["nested"]["flags"] == ["base"]
    pending = cluster.submit(
        pool="unavailable",
        parameters={"nested": {"flags": ["retry"]}},
        steps=[
            {
                "id": "observe",
                "callable": "tests.parameter_operators:snapshot",
                "parameters": {"label": "retry-node"},
            }
        ],
    )
    old = cluster.client.cancel(pending["id"])
    retried = cluster.client.retry(pending["id"])
    assert old["spec"] == retried["spec"]
    assert old["id"] != retried["id"]
