import concurrent.futures
import uuid

import pytest

from dataflowcore.contracts import Conflict, Invalid, TaskSpec

from .conftest import complete, register


def expire(store, assignment):
    with store.transaction() as db:
        db.execute("UPDATE attempts SET lease_until = 0 WHERE id = ?", (assignment["attempt_id"],))


def test_submission_idempotency_and_conflict(store, spec):
    first = store.submit(spec, "same")
    assert store.submit(spec, "same")["id"] == first["id"]
    changed = TaskSpec.parse(spec.json() | {"name": "different"})
    with pytest.raises(Conflict):
        store.submit(changed, "same")
    assert len(store.tasks()) == 1


def test_claim_replay_and_success_replay(store, spec):
    task = store.submit(spec, "submit")
    register(store)
    assignment = store.claim("worker", "claim")
    replay = store.claim("worker", "claim")
    assert replay["attempt_id"] == assignment["attempt_id"]
    assert replay["token"] == assignment["token"]
    assert 0 < replay["lease_seconds"] <= assignment["lease_seconds"]
    assert assignment["task_id"] == task["id"]
    assert complete(store, assignment)["accepted"]
    assert complete(store, assignment)["accepted"]
    with pytest.raises(Conflict):
        complete(store, assignment, result={"different": True})
    final = store.get(task["id"])
    assert final["state"] == "SUCCEEDED"
    assert len(final["attempts"]) == 1
    assert "token" not in final["attempts"][0]


def test_concurrent_claims_never_duplicate_task(store, spec):
    for i in range(10):
        store.submit(spec, str(i))
        register(store, str(i), slots=1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        claims = list(pool.map(lambda i: store.claim(str(i), str(uuid.uuid4())), range(10)))
    assert len({a["task_id"] for a in claims}) == 10
    assert all(len(store.get(a["task_id"])["attempts"]) == 1 for a in claims)


def test_worker_capacity_and_pool_version(store, spec):
    register(store, slots=1, cpu=1, memory_mb=256)
    wrong_pool = TaskSpec.parse(spec.json() | {"pool": "gpu"})
    wrong_version = TaskSpec.parse(spec.json() | {"runtime_version": "other"})
    too_big = TaskSpec.parse(spec.json() | {"memory_mb": 512})
    for i, value in enumerate([wrong_pool, wrong_version, too_big, spec, spec]):
        store.submit(value, str(i))
    claim = store.claim("worker", "c1")
    assert claim["spec"]["memory_mb"] == 256
    assert store.claim("worker", "c2") is None
    complete(store, claim)
    assert store.claim("worker", "c3") is not None


def test_resources_across_multiple_slots(store, spec):
    register(store, slots=3, cpu=2, memory_mb=512)
    for i in range(3):
        store.submit(spec, str(i))
    assert store.claim("worker", "1")
    assert store.claim("worker", "2")
    assert store.claim("worker", "3") is None


def test_expired_attempt_reassigned_and_old_writer_fenced(store, spec):
    task = store.submit(spec, "task")
    register(store)
    old = store.claim("worker", "old")
    expire(store, old)
    assert store.reap() == 1
    register(store, "replacement")
    new = store.claim("replacement", "new")
    assert new["number"] == 2 and new["task_id"] == task["id"]
    assert new["attempt_id"] != old["attempt_id"]
    with pytest.raises(Conflict):
        complete(store, old)
    with pytest.raises(Conflict):
        store.renew(old["task_id"], old["attempt_id"], "worker", old["token"])
    complete(store, new, session="replacement")
    assert [a["state"] for a in store.get(task["id"])["attempts"]] == ["LOST", "SUCCEEDED"]


def test_cancel_queued_and_running_completion_race(store, spec):
    queued = store.submit(spec, "queued")
    assert store.cancel(queued["id"])["state"] == "CANCELLED"
    task = store.submit(spec, "running")
    register(store)
    claim = store.claim("worker", "claim")
    assert claim["task_id"] == task["id"]
    assert store.cancel(task["id"])["state"] == "STOPPING"
    assert store.renew(task["id"], claim["attempt_id"], "worker", claim["token"])["cancel"]
    assert complete(store, claim)["state"] == "CANCELLED"
    assert complete(store, claim)["state"] == "CANCELLED"
    assert store.get(task["id"])["result"] is None


def test_cancelled_lost_worker_never_retries(store, spec):
    task = store.submit(spec, "task")
    register(store)
    claim = store.claim("worker", "claim")
    store.cancel(task["id"])
    expire(store, claim)
    store.reap()
    assert store.get(task["id"])["state"] == "CANCELLED"
    assert store.claim("worker", "new") is None


def test_retry_limit_and_permanent_failure(store, spec):
    task = store.submit(TaskSpec.parse(spec.json() | {"max_attempts": 2}), "task")
    register(store)
    first = store.claim("worker", "first")
    complete(store, first, state="FAILED", result=None, error="transient", retryable=True)
    second = store.claim("worker", "second")
    complete(store, second, state="FAILED", result=None, error="again", retryable=True)
    assert store.get(task["id"])["state"] == "FAILED"
    assert store.claim("worker", "third") is None
    replacement = store.retry(task["id"], "manual")
    assert replacement["id"] != task["id"]
    assert store.retry(task["id"], "manual")["id"] == replacement["id"]
    claim = store.claim("worker", "fourth")
    complete(store, claim, state="FAILED", result=None, error="bad file", retryable=False)
    assert store.get(replacement["id"])["state"] == "FAILED"


def test_deadline_expires_despite_heartbeat(store, spec):
    task = store.submit(spec, "task")
    register(store)
    claim = store.claim("worker", "claim")
    with store.transaction() as db:
        db.execute("UPDATE attempts SET deadline = 0 WHERE id = ?", (claim["attempt_id"],))
    with pytest.raises(Conflict):
        store.renew(task["id"], claim["attempt_id"], "worker", claim["token"])
    store.reap()
    assert store.get(task["id"])["state"] == "QUEUED"


def test_registration_immutable_and_drain(store, spec):
    register(store)
    register(store)
    with pytest.raises(Conflict):
        register(store, cpu=10)
    store.submit(spec, "task")
    store.heartbeat("worker", draining=True)
    assert store.claim("worker", "claim") is None


@pytest.mark.parametrize(
    "blocked_by",
    [{"pool": "other"}, {"cpu": 1024}, {"memory_mb": 1048576}, {"runtime_version": "other"}],
)
def test_runnable_task_not_starved_by_large_incompatible_queue(store, spec, blocked_by):
    from dataflowcore.contracts import canonical, fingerprint

    blocked = TaskSpec.parse(spec.json() | blocked_by).json()
    with store.transaction() as db:
        now = db.now()
        for i in range(1001):
            db.execute(
                "INSERT INTO tasks (id, request_key, request_hash, spec, state, attempt_count, "
                "available_at, created_at, updated_at) VALUES (?, ?, ?, ?, 'QUEUED', 0, ?, ?, ?)",
                (
                    str(uuid.uuid4()),
                    str(i),
                    fingerprint(blocked),
                    canonical(blocked),
                    now,
                    now,
                    now,
                ),
            )
    runnable = store.submit(spec, "runnable")
    register(store)
    assert store.claim("worker", "claim")["task_id"] == runnable["id"]


def test_cursor_pagination_filters_and_bulk_statuses(store, spec):
    ids = {store.submit(spec, str(i))["id"] for i in range(107)}
    page, seen = store.task_page(limit=25, pool="default", summary=True), set()
    while True:
        batch = {t["id"] for t in page["tasks"]}
        assert not seen & batch
        assert all("spec" not in t and "result" not in t for t in page["tasks"])
        seen |= batch
        if page["next_cursor"] is None:
            break
        page = store.task_page(limit=25, cursor=page["next_cursor"], pool="default", summary=True)
    assert seen == ids
    assert store.task_page(state="SUCCEEDED")["tasks"] == []
    chosen = sorted(ids)[:3]
    statuses = store.statuses(chosen + ["missing"])
    assert [s["id"] for s in statuses["tasks"]] == chosen
    assert statuses["missing"] == ["missing"]
    assert all("spec" not in t and "result" not in t for t in statuses["tasks"])
    with pytest.raises(Invalid):
        store.task_page(cursor="bad cursor")
    with pytest.raises(Invalid):
        store.statuses(chosen * 40)


def test_admin_drain_is_not_undone_by_worker_heartbeat(store, spec):
    register(store)
    store.submit(spec, "file")
    store.drain("worker")
    assert store.heartbeat("worker", draining=False)["draining"] is True
    assert store.claim("worker", "claim") is None


@pytest.mark.parametrize(
    "changes",
    [
        {"dag_workers": True},
        {"max_attempts": 0},
        {"timeout": float("nan")},
        {"input_path": "relative"},
        {"unknown": 1},
        {"steps": [{"id": "a", "callable": "lambda"}]},
        {"steps": [{"id": "a", "callable": "m:f", "depends_on": ["missing"]}]},
        {
            "steps": [
                {"id": "a", "callable": "m:f", "depends_on": ["b"]},
                {"id": "b", "callable": "m:f", "depends_on": ["a"]},
            ]
        },
    ],
)
def test_invalid_specs(spec, changes):
    with pytest.raises(Invalid):
        TaskSpec.parse(spec.json() | changes)
