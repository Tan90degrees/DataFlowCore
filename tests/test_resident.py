"""Acceptance with real API, resident processes, threads, signals and artifacts."""

import os
from pathlib import Path

from tests.test_e2e import until

OBSERVE = [{"id": "observe", "callable": "tests.resident_operators:Observe"}]


def observe(cluster, **changes):
    task = cluster.submit(steps=OBSERVE, dag_workers=1, **changes)
    return cluster.state(task["id"], "SUCCEEDED")


def dead(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1][0] == "Z"
    except FileNotFoundError:
        return True


def test_pid_resources_and_both_thread_pools_reused_with_fresh_task_state(cluster):
    cluster.worker(1, "--runner-map-workers", "2")
    first = observe(cluster, parameters={"label": "first", "nested": {"items": [1]}})
    second = observe(cluster, parameters={"label": "second", "nested": {"items": [2]}})
    a, b = first["result"]["steps"]["observe"], second["result"]["steps"]["observe"]
    assert a["pid"] == b["pid"]
    assert a["resource"] == b["resource"]
    assert (a["calls"], b["calls"]) == (1, 2)
    assert a["operator"] != b["operator"]
    assert a["dag_thread"] == b["dag_thread"]
    assert len(a["map_threads"]) == 2
    assert a["map_threads"] == b["map_threads"]
    assert a["parameters"]["label"] == "first"
    assert b["parameters"]["label"] == "second"
    assert a["inputs"] == b["inputs"] == {}
    assert first["result"]["output_dir"] != second["result"]["output_dir"]
    assert second["result"]["runtime"]["runner"]["tasks_before"] == 1
    assert second["result"]["usage"]["rss_scope"] == "runner_lifetime"
    assert second["result"]["usage"]["cpu_seconds"] >= 0


def test_task_limit_recycles_and_closes_cached_resources(cluster):
    cluster.worker(1, "--runner-max-tasks", "1")
    first = observe(cluster)["result"]["steps"]["observe"]
    second = observe(cluster)["result"]["steps"]["observe"]
    assert first["pid"] != second["pid"]
    assert first["resource"] != second["resource"]
    assert second["calls"] == 1
    assert (cluster.root / f"closed-{first['resource']}").is_file()
    assert until(lambda: dead(first["pid"]))


def test_cancel_replaces_one_runner_and_keeps_other_slot_alive(cluster):
    cluster.worker(2)
    stopped = cluster.submit(seconds=30)
    cluster.state(stopped["id"], "RUNNING")
    cancelled_pid = until(
        lambda: (
            cluster.client.get(stopped["id"])["attempts"][0]["progress"]
            .get("runner", {})
            .get("pid")
        )
    )
    other = cluster.submit(seconds=1.5)
    cluster.state(other["id"], "RUNNING")
    other_pid = until(
        lambda: (
            cluster.client.get(other["id"])["attempts"][0]["progress"].get("runner", {}).get("pid")
        )
    )
    cluster.client.cancel(stopped["id"])
    cluster.state(stopped["id"], "CANCELLED")
    final = cluster.state(other["id"], "SUCCEEDED")
    assert final["attempt_count"] == 1
    assert final["result"]["runtime"]["runner"]["pid"] == other_pid
    assert until(lambda: dead(cancelled_pid))
    assert observe(cluster)["result"]["runtime"]["runner"]["pid"] != cancelled_pid


def test_runner_crash_retries_file_on_replacement_without_replacing_executor(cluster):
    cluster.worker()
    task = cluster.submit(steps=[{"id": "run", "callable": "tests.resident_operators:crash_once"}])
    final = cluster.state(task["id"], "SUCCEEDED")
    assert [a["state"] for a in final["attempts"]] == ["FAILED", "SUCCEEDED"]
    assert final["attempts"][0]["worker_session"] == final["attempts"][1]["worker_session"]
    assert final["result"]["steps"]["run"]["pid"] != int(
        (cluster.root / "crashed-once").read_text()
    )


def test_idle_runner_death_is_repaired_before_claim(cluster):
    cluster.worker()
    first = observe(cluster)["result"]["steps"]["observe"]
    os.kill(first["pid"], 9)
    second = observe(cluster)["result"]["steps"]["observe"]
    assert first["pid"] != second["pid"]
    assert second["calls"] == 1


def test_reused_runner_log_tail_does_not_include_previous_task(cluster):
    operator = cluster.root / "logs.py"
    operator.write_text(
        "import os\ndef run(context, inputs):\n"
        "    print(context.parameters['message'], flush=True)\n"
        "    return {'pid': os.getpid()}\n"
    )
    cluster.env["PYTHONPATH"] += os.pathsep + str(cluster.root)
    cluster.worker()
    steps = [{"id": "logs", "callable": "logs:run"}]
    first = cluster.submit(steps=steps, parameters={"message": "unique-first-log"})
    a = cluster.state(first["id"], "SUCCEEDED")
    second = cluster.submit(steps=steps, parameters={"message": "unique-second-log"})
    b = cluster.state(second["id"], "SUCCEEDED")
    assert a["result"]["steps"]["logs"]["pid"] == b["result"]["steps"]["logs"]["pid"]
    assert "unique-first-log" in a["attempts"][0]["progress"]["log_tail"]
    assert "unique-second-log" in b["attempts"][0]["progress"]["log_tail"]
    assert "unique-first-log" not in b["attempts"][0]["progress"]["log_tail"]


def test_success_with_leftover_child_recycles_group_before_next_file(cluster):
    cluster.worker()
    task = cluster.submit(steps=[{"id": "run", "callable": "tests.resident_operators:leftover"}])
    result = cluster.state(task["id"], "SUCCEEDED")["result"]["steps"]["run"]
    assert until(lambda: dead(result["child"]))
    assert observe(cluster)["result"]["steps"]["observe"]["pid"] != result["pid"]


def test_abandoned_map_work_cannot_leak_into_next_task(cluster):
    cluster.worker()
    task = cluster.submit(
        steps=[{"id": "run", "callable": "tests.resident_operators:leave_map_running"}]
    )
    final = cluster.state(task["id"], "FAILED")
    assert "unfinished context.map" in final["error"]
    assert final["attempt_count"] == 1
    assert observe(cluster)


def test_unmanaged_background_thread_retires_successful_runner(cluster):
    cluster.worker()
    task = cluster.submit(
        steps=[{"id": "run", "callable": "tests.resident_operators:leftover_thread"}]
    )
    result = cluster.state(task["id"], "SUCCEEDED")["result"]["steps"]["run"]
    assert observe(cluster)["result"]["steps"]["observe"]["pid"] != result["pid"]


def test_map_thread_budget_shared_by_parallel_dag_nodes(cluster):
    cluster.worker(1, "--runner-map-workers", "2")
    task = cluster.submit(
        steps=[
            {"id": name, "callable": "tests.resident_operators:parallel_map"}
            for name in ("left", "right")
        ]
    )
    result = cluster.state(task["id"], "SUCCEEDED")["result"]
    assert result["runtime"]["runner"]["map_workers"] == 2
    for row in result["steps"].values():
        assert row["values"] == list(range(8))
        assert row["peak"] == 2


def test_nested_map_fails_instead_of_deadlocking_shared_pool(cluster):
    cluster.worker(1, "--runner-map-workers", "1")
    task = cluster.submit(steps=[{"id": "run", "callable": "tests.resident_operators:nested_map"}])
    final = cluster.state(task["id"], "FAILED")
    assert "nested context.map" in final["error"]
    assert final["attempt_count"] == 1
    assert observe(cluster)
