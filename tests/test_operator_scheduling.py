import json
import os
import signal
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from dataflowcore.contracts import Invalid, TaskSpec
from dataflowcore.resources import ResourceBudget
from dataflowcore.sdk import Pipeline
from tests.conftest import register
from tests.test_e2e import until


def node(sid="work", kind="cpu", cpu=1, memory=128, seconds=0.3, depends=(), fn="probe"):
    return {
        "id": sid,
        "callable": f"tests.scheduling_operators:{fn}",
        "workload": kind,
        "resources": {"cpu": cpu, "memory_mb": memory},
        "depends_on": list(depends),
        "parameters": {"seconds": seconds},
    }


def step_progress(cluster, tid, sid="work", state="RUNNING"):
    def check():
        task = cluster.client.get(tid)
        progress = task["attempts"][-1]["progress"] if task["attempts"] else {}
        task["progress"] = progress
        return task if progress.get("steps", {}).get(sid, {}).get("state") == state else None

    return until(check)


def test_contract_normalizes_single_operator_placement_and_sdk(tmp_path):
    spec = (
        Pipeline("mixed", cpu=100, memory_mb=10000)
        .step(
            "io",
            "tests.scheduling_operators:probe",
            workload="io",
            resources={"cpu": 0.125, "memory_mb": 32},
        )
        .step(
            "cpu",
            "tests.scheduling_operators:probe",
            workload="cpu",
            resources={"cpu": 2, "memory_mb": 256},
        )
        .spec(tmp_path / "input")
    )
    assert (spec["scheduling"], spec["cpu"], spec["memory_mb"]) == ("operator", 2, 256)
    assert TaskSpec.parse(spec).json() == spec
    assert spec["steps"][0]["resources"]["cpu"] == 0.125


@pytest.mark.parametrize(
    "changes",
    [
        {"workload": "gpu"},
        {"resources": {"cpu": True}},
        {"resources": {"cpu": float("nan")}},
        {"resources": {"cpu": -1}},
        {"resources": {"cpu": 0.0001}},
        {"resources": {"cpu": 0}},
        {"resources": {"memory_mb": 0}},
        {"resources": {"memory_mb": 1.5}},
        {"resources": {"gpu": 1}},
        {"resources": []},
    ],
)
def test_invalid_operator_resources(spec, changes):
    with pytest.raises(Invalid):
        TaskSpec.parse(spec.json() | {"scheduling": "operator", "steps": [node() | changes]})


def test_control_places_files_without_whole_task_reservation(store, spec):
    register(store, slots=3, cpu=2, memory_mb=256)
    value = TaskSpec.parse(spec.json() | {"scheduling": "operator", "steps": [node(cpu=2)]})
    for i in range(3):
        store.submit(value, f"file-{i}")
    assignments = [store.claim("worker", f"claim-{i}") for i in range(3)]
    assert all(a is not None for a in assignments)
    assert store.claim("worker", "full") is None


def test_control_filters_fractional_and_oversized_nodes_before_queue_limit(store, spec):
    register(store, slots=3, cpu=1, memory_mb=128)
    big = TaskSpec.parse(spec.json() | {"scheduling": "operator", "steps": [node(cpu=2)]})
    for i in range(1050):
        store.submit(big, f"big-{i}")
    small = TaskSpec.parse(
        spec.json() | {"scheduling": "operator", "steps": [node(kind="io", cpu=0.125)]}
    )
    task = store.submit(small, "small")
    assert store.claim("worker", "small-claim")["task_id"] == task["id"]


def test_persisted_legacy_tasks_keep_reservations_and_retry_mode(store, spec):
    register(store, slots=2, cpu=1, memory_mb=256)
    task = store.submit(spec, "old-file")
    old = spec.json()
    old.pop("scheduling")
    old.pop("io_workers")
    with store.transaction() as db:
        db.execute("UPDATE tasks SET spec=? WHERE id=?", (json.dumps(old), task["id"]))
    assignment = store.claim("worker", "old-claim")
    assert "scheduling" not in assignment["spec"]
    assert "io_workers" not in assignment["spec"]
    store.submit(spec, "other-old-file")
    assert store.claim("worker", "old-full") is None
    store.cancel(task["id"])
    store.complete(
        task["id"],
        assignment["attempt_id"],
        "worker",
        assignment["token"],
        state="CANCELLED",
        retryable=False,
    )
    retried = store.retry(task["id"], "manual")
    assert retried["spec"]["scheduling"] == "task"
    assert retried["spec"]["runtime_version"] == old["runtime_version"]
    assert retried["spec"]["steps"] == old["steps"]
    # The next delivery is still readable by pre-0.4 executors.
    next_assignment = store.claim("worker", "old-next")
    assert "scheduling" not in next_assignment["spec"]
    assert "io_workers" not in next_assignment["spec"]


def test_shared_budget_atomic_fractional_cpu_memory_and_io_limit(tmp_path):
    budget = ResourceBudget(tmp_path / "budget", 1, 256, io_concurrency=2)

    def acquire(i):
        return budget.acquire(str(i), "node", "io", {"cpu": 0.25, "memory_mb": 64})

    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(acquire, range(8))) == 2
    current = budget.snapshot()
    assert (current["cpu"], current["memory_mb"], current["io_operators"]) == (0.5, 128, 2)
    for i in range(8):
        budget.release(str(i))
    assert budget.snapshot()["active_operators"] == 0
    assert budget.snapshot()["waiting_operators"] == 0
    with pytest.raises(Invalid, match="exceed"):
        budget.acquire("big", "node", "cpu", {"cpu": 2, "memory_mb": 32})


def test_cpu_fifo_io_bypass_and_owner_generation_cleanup(tmp_path):
    budget = ResourceBudget(tmp_path / "budget", 2, 1024)
    assert budget.acquire("running", "node", "cpu", {"cpu": 1, "memory_mb": 64})
    assert not budget.acquire("large", "node", "cpu", {"cpu": 2, "memory_mb": 64})
    assert not budget.acquire("small", "node", "cpu", {"cpu": 1, "memory_mb": 64})
    assert budget.acquire("io", "node", "io", {"cpu": 0, "memory_mb": 64})
    budget.release("running")
    assert budget.acquire("large", "node", "cpu", {"cpu": 2, "memory_mb": 64})
    assert not budget.acquire("small", "node", "cpu", {"cpu": 1, "memory_mb": 64})
    budget.release("large")
    assert budget.acquire("small", "node", "cpu", {"cpu": 1, "memory_mb": 64})
    budget.release("old-generation")
    assert budget.snapshot()["active_operators"] == 2


def test_budget_survives_repeated_runner_death_with_concurrent_observation(tmp_path):
    budget = ResourceBudget(tmp_path / "budget", 2, 1024)
    stopped, failures = threading.Event(), []

    def observe():
        while not stopped.wait(0.005):
            try:
                assert budget.snapshot()["cpu"] <= 2
            except Exception as exc:
                failures.append(exc)
                return

    monitor = threading.Thread(target=observe)
    monitor.start()
    code = (
        "import sys,time; from dataflowcore.resources import ResourceBudget; "
        "b=ResourceBudget(sys.argv[1]); "
        "b.acquire(sys.argv[2], 'node', 'cpu', {'cpu':1, 'memory_mb':128}); "
        "print('ready',flush=True); time.sleep(60)"
    )
    env = os.environ | {"PYTHONPATH": str(Path(__file__).parents[1] / "src")}
    try:
        for generation in range(12):
            owner = str(generation)
            process = subprocess.Popen(
                [sys.executable, "-c", code, budget.path, owner], stdout=subprocess.PIPE, env=env
            )
            try:
                until(lambda: budget.snapshot()["active_operators"] == 1)
                process.kill()
                process.wait(timeout=3)
                assert budget.snapshot()["active_operators"] == 1
                budget.release(owner)
                assert budget.snapshot()["active_operators"] == 0
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=3)
                process.stdout.close()
        assert not failures
    finally:
        stopped.set()
        monitor.join(timeout=3)
        budget.close()


def test_cpu_and_io_branches_use_independent_pools_on_same_runner(cluster):
    cluster.worker(1, "--runner-dag-workers", "1", "--runner-io-workers", "2")
    task = cluster.submit(
        dag_workers=1,
        steps=[
            node("cpu", cpu=2, seconds=0.8),
            node("io", "io", 0, seconds=0.8),
            node("join", "io", 0, depends=["cpu", "io"], seconds=0),
        ],
    )
    final = cluster.state(task["id"], "SUCCEEDED")
    steps = final["result"]["steps"]
    assert len({s["pid"] for s in steps.values()}) == 1
    assert steps["io"]["started"] < steps["cpu"]["finished"]
    assert steps["cpu"]["started"] < steps["io"]["finished"]
    assert steps["cpu"]["thread"].startswith("dag_")
    assert steps["io"]["thread"].startswith("io-dag_")
    assert final["result"]["runtime"]["scheduler"]["peak_cpu"] <= 2


def test_memory_serializes_io_across_files_and_cpu_can_overlap(cluster):
    cluster.worker(3, "--cpu", "2", "--memory-mb", "768")
    first = cluster.submit(steps=[node(kind="io", cpu=0, memory=600, seconds=1.2)])
    step_progress(cluster, first["id"])
    second = cluster.submit(steps=[node(kind="io", cpu=0, memory=600, seconds=0.2)])
    waiting = step_progress(cluster, second["id"], state="WAITING_RESOURCES")
    assert waiting["progress"]["scheduler"]["waiting_operators"] >= 1
    a = cluster.state(first["id"], "SUCCEEDED")
    b = cluster.state(second["id"], "SUCCEEDED")
    assert a["result"]["steps"]["work"]["finished"] <= b["result"]["steps"]["work"]["started"]
    assert b["attempts"][-1]["progress"]["steps"]["work"]["resource_wait_seconds"] > 0
    assert b["result"]["runtime"]["scheduler"]["peak_memory_mb"] == 600


def test_cancel_waiting_node_reclaims_pending_without_stopping_other_task(cluster):
    cluster.worker(3)
    first = cluster.submit(steps=[node(cpu=2, seconds=2)])
    step_progress(cluster, first["id"])
    waiting = cluster.submit(steps=[node(cpu=2, seconds=0.1)])
    step_progress(cluster, waiting["id"], state="WAITING_RESOURCES")
    cluster.client.cancel(waiting["id"])
    cluster.state(waiting["id"], "CANCELLED")
    third = cluster.submit(steps=[node(cpu=2, seconds=0.1)])
    a = cluster.state(first["id"], "SUCCEEDED")
    final = cluster.state(third["id"], "SUCCEEDED")
    assert a["result"]["steps"]["work"]["finished"] <= final["result"]["steps"]["work"]["started"]
    assert final["result"]["runtime"]["scheduler"]["peak_cpu"] == 2
    assert final["result"]["runtime"]["scheduler"]["waiting_operators"] == 0


def test_killed_runner_reclaims_reservation_before_whole_file_retry(cluster):
    cluster.worker(2)
    task = cluster.submit(
        steps=[node("first", cpu=2, seconds=0.1), node("work", cpu=2, seconds=1, depends=["first"])]
    )
    running = step_progress(cluster, task["id"])
    pid = running["progress"]["runner"]["pid"]
    os.kill(pid, signal.SIGKILL)
    final = cluster.state(task["id"], "SUCCEEDED")
    assert [a["state"] for a in final["attempts"]] == ["FAILED", "SUCCEEDED"]
    assert final["attempts"][-1]["progress"]["runner"]["pid"] != pid
    assert final["attempts"][-1]["progress"]["steps"]["first"]["state"] == "SUCCEEDED"
    assert final["result"]["runtime"]["scheduler"]["peak_cpu"] == 2


def test_map_cpu_budget_clamps_threads_and_io_map_is_separate(cluster):
    cluster.worker(1, "--runner-map-workers", "4", "--runner-io-map-workers", "4")
    task = cluster.submit(steps=[node("cpu", cpu=1, fn="mapped"), node("io", "io", 0, fn="mapped")])
    final = cluster.state(task["id"], "SUCCEEDED")
    steps = final["result"]["steps"]
    assert steps["cpu"]["peak"] == 1
    assert 1 < steps["io"]["peak"] <= 4
    assert all(t.startswith("blocks_") for t in steps["cpu"]["threads"])
    assert all(t.startswith("io-blocks_") for t in steps["io"]["threads"])
