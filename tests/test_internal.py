"""Internal component performance paths retain fencing and bounded admission."""

import json
import signal
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from dataflowcore.contracts import Invalid, TaskSpec
from dataflowcore.resources import ResourceBudget
from dataflowcore.store import Store
from dataflowcore.worker import Running, Worker
from tests.conftest import complete, register
from tests.test_e2e import until
from tests.test_production import production_api as production_api


def renewal(assignment, **changes):
    return {
        "tid": assignment["task_id"],
        "aid": assignment["attempt_id"],
        "token": assignment["token"],
        **changes,
    }


def test_pulse_lost_response_replays_all_claims_without_duplicate_attempts(store, spec):
    register(store, slots=4)
    spec = TaskSpec.parse(spec.json() | {"scheduling": "operator"})
    tasks = [store.submit(spec, str(i)) for i in range(4)]
    first = store.pulse("worker", claims=["a", "b", "c", "d"])
    replay = store.pulse("worker", claims=["a", "b", "c", "d"])
    assert [c["assignment"]["attempt_id"] for c in first["claims"]] == [
        c["assignment"]["attempt_id"] for c in replay["claims"]
    ]
    for task in tasks:
        assert len(store.get(task["id"])["attempts"]) == 1


def test_expired_renewal_cannot_block_healthy_leases_or_cancel_delivery(store, spec):
    register(store, slots=3)
    for i in range(3):
        store.submit(TaskSpec.parse(spec.json() | {"scheduling": "operator"}), str(i))
    assignments = [x["assignment"] for x in store.pulse("worker", claims=["a", "b", "c"])["claims"]]
    with store.transaction() as db:
        db.execute(
            "UPDATE attempts SET lease_until=? WHERE id=?",
            (db.now() - 1, assignments[0]["attempt_id"]),
        )
    store.cancel(assignments[2]["task_id"])
    value = store.pulse("worker", renewals=[renewal(a) for a in assignments])
    assert value["renewals"][0]["status"] == 409
    assert value["renewals"][1]["lease_seconds"] > 0
    assert value["renewals"][2]["cancel"]
    complete(store, assignments[1])


def test_malformed_batch_rolls_back_before_any_lease_or_heartbeat_write(store, spec):
    register(store)
    store.submit(spec, "file")
    a = store.claim("worker", "claimed")
    before = store.workers()[0]["last_seen"]
    with pytest.raises(Invalid):
        store.pulse("worker", renewals=[renewal(a), renewal(a, progress={"bad": "x" * 262145})])
    assert store.workers()[0]["last_seen"] == before
    assert store.get(a["task_id"])["attempts"][0]["progress"] == {}


def test_idle_worker_keeps_resource_snapshot_and_database_slot_counts(store):
    register(store, slots=4)
    store.pulse(
        "worker",
        telemetry={
            "scheduler": {"cpu": 0, "memory_mb": 0},
            "runtime": {"processes": 5, "sampled_rss_mb": None},
        },
    )
    worker = store.workers()[0]
    assert worker["active_tasks"] == 0 and worker["free_slots"] == 4
    assert worker["scheduler"] == {"cpu": 0, "memory_mb": 0}
    assert worker["runtime"]["processes"] == 5


def test_resource_release_wakes_waiters_without_polling(tmp_path):
    first = ResourceBudget(tmp_path / "budget", 1, 256)
    second = ResourceBudget(tmp_path / "budget")
    try:
        assert first.acquire("a", "held", "cpu", {"cpu": 1, "memory_mb": 128})
        assert not second.acquire("b", "waiting", "cpu", {"cpu": 1, "memory_mb": 128})
        second.wait(0)  # Drain hints from initial acquisition.
        waiting = threading.Event()

        def wait_for_release():
            waiting.set()
            second.wait(10)

        thread = threading.Thread(target=wait_for_release)
        thread.start()
        assert waiting.wait(1)
        first.release("a", "held")
        thread.join(timeout=1)
        assert not thread.is_alive(), "release must wake a blocking waiter"
        assert second.acquire("b", "waiting", "cpu", {"cpu": 1, "memory_mb": 128})
    finally:
        second.close()
        first.close()


def test_old_database_gains_telemetry_without_removing_historical_data(tmp_path):
    path = tmp_path / "old.db"
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TABLE workers (session_id TEXT PRIMARY KEY,name TEXT,pool TEXT,"
            "runtime_version TEXT,slots INTEGER,cpu INTEGER,memory_mb INTEGER,"
            "last_seen DOUBLE PRECISION,draining INTEGER)"
        )
        db.execute("INSERT INTO workers VALUES ('old','old','default','old',1,2,1024,0,0)")
        db.execute("CREATE TABLE audit (payload TEXT)")
        db.execute("INSERT INTO audit VALUES ('historical')")
    store = Store("sqlite:///" + str(path))
    try:
        store.migrate()
        assert store.workers()[0]["name"] == "old"
        with store.transaction(False) as db:
            assert db.one("SELECT payload FROM audit")["payload"] == "historical"
    finally:
        store.close()


def test_worker_keeps_claim_ids_when_pulse_response_is_lost(production_api, store, spec, tmp_path):
    for i in range(2):
        store.submit(TaskSpec.parse(spec.json() | {"scheduling": "operator"}), str(i))
    worker = Worker(
        f"http://127.0.0.1:{production_api.server_port}",
        "",
        tmp_path,
        tmp_path / "work",
        slots=2,
        interval=0.2,
    )
    try:
        worker.ensure_runners()
        until(
            lambda: all(
                worker.read_json(s.workspace / "state.json", {}).get("state") == "READY"
                for s in worker.runners.values()
            )
        )
        original = worker.request
        lost = []

        def lose_once(endpoint, data):
            value = original(endpoint, data)
            if endpoint == "pulse" and not lost:
                lost.extend(data["claims"])
                raise TimeoutError("response lost after commit")
            return value

        worker.request = lose_once
        with pytest.raises(TimeoutError):
            worker.tick()
        assert worker.pending_claims == lost and not worker.running
        worker.tick()
        assert len(worker.running) == 2 and not worker.pending_claims
        for running in worker.running.values():
            assert len(store.get(running.assignment["task_id"])["attempts"]) == 1
    finally:
        for slot in worker.runners.values():
            worker.signal_group(slot, signal.SIGKILL)
            slot.process.wait(timeout=3)
            slot.close_pipe()
            slot.logs.close()
        worker.budget.close()
        worker.close_notifications()


def test_postgres_pool_reuses_and_replaces_broken_connections(store):
    if not store.postgres:
        pytest.skip("PostgreSQL connection pool")
    before = store.pool_stats()["connections_num"]
    for _ in range(20):
        store.ping()
    assert store.pool_stats()["connections_num"] <= before + 1
    after = store.pool_stats()["connections_num"]
    connection = store.pool().getconn()
    connection.close()
    store.pool().putconn(connection)
    store.ping()
    until(lambda: store.pool_stats()["connections_num"] > after)
    with store.transaction(False) as db:
        assert int(db.one("SHOW statement_timeout")["statement_timeout"].removesuffix("s")) > 0


def test_completion_commits_final_progress_and_result_together(store, spec):
    register(store)
    task = store.submit(spec, "final-progress")
    assignment = store.claim("worker", "claim")
    identity = {**renewal(assignment), "session_id": "worker"}
    store.renew(**identity, progress={"state": "RUNNING", "completed_steps": 0})
    final = {
        "state": "SUCCEEDED",
        "completed_steps": 1,
        "steps": {"read": {"state": "SUCCEEDED", "duration_seconds": 0.01}},
    }
    store.complete(**identity, state="SUCCEEDED", result={"ok": 1}, progress=final)
    assert store.get(task["id"])["attempts"][0]["progress"] == final
    store.complete(**identity, state="SUCCEEDED", result={"ok": 1}, progress={"state": "RUNNING"})
    assert store.get(task["id"])["attempts"][0]["progress"] == final


def test_file_finishing_during_pulse_still_persists_final_step_progress(
    production_api, store, spec, tmp_path
):
    task = store.submit(
        TaskSpec.parse(
            spec.json()
            | {
                "steps": [
                    {
                        "id": "delay",
                        "callable": "dataflowcore.operators:delay",
                        "parameters": {"seconds": 0.15},
                    }
                ]
            }
        ),
        "pulse-race",
    )
    worker = Worker(
        f"http://127.0.0.1:{production_api.server_port}",
        "",
        tmp_path,
        tmp_path / "work",
        interval=0.2,
    )
    try:
        worker.ensure_runners()
        until(lambda: worker.available_runner())
        worker.tick()
        running = next(iter(worker.running.values()))
        until(lambda: worker.read_json(running.workspace / "progress.json", {}).get("steps"))
        old_pulse = store.pulse

        def delayed_pulse(**kwargs):
            result = old_pulse(**kwargs)
            # The renewal committed before the final on-disk snapshot existed.
            until(
                lambda: (
                    worker.read_json(running.slot.workspace / "state.json", {}).get(
                        "completed_attempt"
                    )
                    == running.assignment["attempt_id"]
                )
            )
            return result

        store.pulse = delayed_pulse
        worker.tick()
        final = store.get(task["id"])
        assert final["state"] == "SUCCEEDED"
        progress = final["attempts"][0]["progress"]
        assert progress["state"] == "SUCCEEDED" and progress["completed_steps"] == 1
        assert progress["steps"]["delay"]["state"] == "SUCCEEDED"
    finally:
        for slot in worker.runners.values():
            worker.signal_group(slot, signal.SIGKILL)
            slot.process.wait(timeout=3)
            slot.close_pipe()
            slot.logs.close()
        worker.budget.close()
        worker.close_notifications()


def test_large_final_result_flushes_progress_without_exceeding_http_budget(
    production_api, store, spec, tmp_path
):
    register(store)
    task = store.submit(spec, "big-final")
    assignment = store.claim("worker", "big-claim")
    workspace = tmp_path / "attempt"
    workspace.mkdir()
    (workspace / "progress.json").write_text(json.dumps({"message": "x" * 210000}))
    worker = Worker(
        f"http://127.0.0.1:{production_api.server_port}", "", tmp_path, tmp_path / "work"
    )
    worker.session = "worker"
    running = Running(
        assignment, None, workspace, time.monotonic() + 6, SimpleNamespace(tail=lambda: "")
    )
    try:
        response = worker.complete(
            running,
            {
                "state": "SUCCEEDED",
                "result": {"blob": "x" * 850000},
                "error": None,
                "retryable": False,
            },
        )
        assert response["accepted"]
        final = store.get(task["id"])
        assert len(final["result"]["blob"]) == 850000
        assert len(final["attempts"][0]["progress"]["message"]) == 210000
    finally:
        worker.budget.close()
        worker.close_notifications()
