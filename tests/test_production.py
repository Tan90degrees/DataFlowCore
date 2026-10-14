"""Production controls exercised through real transactions, HTTP and filesystem IO."""

import io
import json
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from dataflowcore.api import Server
from dataflowcore.contracts import Backpressure, Conflict, Paused, TaskSpec
from dataflowcore.control_lock import ControlLock
from dataflowcore.maintenance import clean_failed_outputs
from dataflowcore.store import Store
from dataflowcore.uploads import UploadError, Uploads
from tests.conftest import complete, register
from tests.test_e2e import until


@pytest.fixture
def production_api(store, tmp_path):
    server = Server(
        ("127.0.0.1", 0),
        store,
        tmp_path,
        "a" * 32,
        "w" * 32,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    thread.join(timeout=5)
    server.server_close()


def http(server, path, method="GET", data=None, token="a" * 32, key=None, raw=False):
    headers = {"Authorization": "Bearer " + token, "X-Request-ID": "test-request"}
    if key:
        headers["Idempotency-Key"] = key
    request = urllib.request.Request(
        f"http://127.0.0.1:{server.server_port}" + path,
        data=json.dumps(data).encode() if data is not None else None,
        headers=headers,
        method=method,
    )
    try:
        response = urllib.request.urlopen(request, timeout=5)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        payload = response.read().decode()
        return response.status, response.headers, payload if raw else json.loads(payload)


def test_bounded_admission_is_atomic_and_preserves_idempotency(store, spec):
    store.max_unfinished = 3

    def submit(i):
        try:
            return store.submit(spec, str(i))
        except Backpressure:
            return None

    with ThreadPoolExecutor(max_workers=12) as threads:
        accepted = [r for r in threads.map(submit, range(20)) if r]
    assert len(accepted) == 3
    assert store.submit(spec, accepted[0]["request_key"])["id"] == accepted[0]["id"]
    store.cancel(accepted[0]["id"])
    assert store.submit(spec, "after-release")["state"] == "QUEUED"


def test_pause_survives_restart_keeps_claim_replay_and_allows_completion(store, spec):
    register(store)
    first = store.submit(spec, "first")
    assignment = store.claim("worker", "delivered")
    store.submit(spec, "queued")
    store.set_paused(True)
    restarted = Store(store.dsn)
    try:
        assert restarted.control()["paused"]
    finally:
        restarted.close()
    assert store.submit(spec, "first")["id"] == first["id"]
    with pytest.raises(Paused):
        store.submit(spec, "new")
    replay = store.claim("worker", "delivered")
    assert (replay["task_id"], replay["attempt_id"], replay["token"]) == (
        assignment["task_id"],
        assignment["attempt_id"],
        assignment["token"],
    )
    assert store.claim("worker", "new-claim") is None
    complete(store, assignment)
    assert store.get(first["id"])["state"] == "SUCCEEDED"
    store.set_paused(False)
    assert store.claim("worker", "new-claim")


@pytest.mark.parametrize(
    "reason",
    [
        "NO_ONLINE_WORKER",
        "POOL_MISMATCH",
        "VERSION_MISMATCH",
        "DRAINING",
        "RESOURCE_TOO_LARGE",
        "NO_FREE_SLOT",
        "WAITING_FOR_CLAIM",
        "RETRY_BACKOFF",
    ],
)
def test_queue_diagnostics_identify_the_actual_blocker(store, spec, reason):
    spec = TaskSpec.parse(spec.json() | {"cpu": 2})
    task = store.submit(spec, "diagnostic")
    if reason != "NO_ONLINE_WORKER":
        register(
            store,
            slots=1,
            pool="other" if reason == "POOL_MISMATCH" else "default",
            runtime_version="other" if reason == "VERSION_MISMATCH" else spec.runtime_version,
            cpu=1 if reason == "RESOURCE_TOO_LARGE" else 4,
        )
    if reason == "DRAINING":
        store.drain("worker")
    if reason == "NO_FREE_SLOT":
        store.claim("worker", "occupy")
        task = store.submit(spec, "second")
    if reason == "RETRY_BACKOFF":
        with store.transaction() as db:
            db.execute("UPDATE tasks SET available_at=? WHERE id=?", (db.now() + 60, task["id"]))
    assert store.get(task["id"])["queue_diagnostic"]["code"] == reason


def test_internal_api_has_no_identity_or_audit_dependencies(production_api, store, spec):
    task = store.submit(spec, "internal")
    assert http(production_api, "/v1/overview", token="")[0] == 200
    assert "role" not in http(production_api, "/v1/overview")[2]
    assert http(production_api, f"/v1/tasks/{task['id']}/cancel", "POST", {}, token="")[0] == 200
    assert http(production_api, "/v1/audit")[0] == 404
    assert store.get(task["id"])["state"] == "CANCELLED"


def test_maintenance_and_backpressure_http_codes_are_actionable(production_api, store, spec):
    store.max_unfinished = 1
    task = store.submit(spec, "already-submitted")
    assert http(production_api, "/v1/control", "POST", {"paused": True})[2]["quiescent"]
    value = spec.json()
    status, headers, body = http(production_api, "/v1/tasks", "POST", value, key="new")
    assert (status, body["code"], headers["Retry-After"]) == (503, "maintenance_paused", "5")
    assert headers["X-Request-ID"] == "test-request"
    assert store.get(task["id"])["queue_diagnostic"]["code"] == "MAINTENANCE"
    http(production_api, "/v1/control", "POST", {"paused": False})
    status, _, body = http(production_api, "/v1/tasks", "POST", value, key="new")
    assert (status, body["code"]) == (429, "admission_limit")


def test_unhealthy_reconciliation_fails_readiness_without_failing_liveness(production_api):
    production_api.stopped.set()
    assert http(production_api, "/readyz")[0] == 503
    assert http(production_api, "/healthz")[0] == 200


def test_trusted_operators_validate_without_importing_and_metrics_have_bounded_labels(
    production_api, spec, store
):
    blocked = spec.json() | {"steps": [{"id": "run", "callable": "os:system"}]}
    assert http(production_api, "/v1/dags/validate", "POST", blocked)[0] == 200
    task = store.submit(spec, "metrics")
    http(production_api, "/v1/tasks/" + task["id"])
    status, _, metrics = http(production_api, "/metrics", token="v" * 32, raw=True)
    assert status == 200
    assert 'route="task"' in metrics
    assert "dataflow_http_request_duration_seconds_bucket" in metrics
    assert task["id"] not in metrics
    assert "dataflow_online_workers" in metrics


def test_controller_exclusion_and_release(store):
    first = ControlLock(store)
    try:
        with pytest.raises(Conflict, match="another controller"):
            ControlLock(store)
        first.check()
    finally:
        first.close()
    second = ControlLock(store)
    second.close()


def test_management_overload_does_not_consume_worker_admission(production_api):
    for _ in range(8):
        assert production_api.management_slots.acquire(blocking=False)
    try:
        status, _, body = http(production_api, "/v1/overview")
        assert (status, body["code"]) == (429, "api_concurrency")
        value = {
            "session_id": "independent",
            "name": "worker",
            "pool": "default",
            "runtime_version": "test",
            "slots": 1,
            "cpu": 2,
            "memory_mb": 512,
        }
        assert http(production_api, "/v1/worker/register", "POST", value, token="w" * 32)[0] == 200
    finally:
        for _ in range(8):
            production_api.management_slots.release()


def test_compact_history_does_not_load_every_attempt_payload(production_api, store, spec):
    spec = TaskSpec.parse(spec.json() | {"max_attempts": 20, "retry_delay": 0})
    register(store)
    task = store.submit(spec, "history")
    first = None
    for index in range(8):
        assignment = store.claim("worker", str(index))
        first = first or assignment["attempt_id"]
        store.renew(
            task["id"],
            assignment["attempt_id"],
            "worker",
            assignment["token"],
            progress={"message": "x" * 100000},
        )
        complete(store, assignment, state="FAILED", retryable=True, result={"blob": "x" * 100000})
    body = http(production_api, "/v1/tasks/" + task["id"])[2]
    assert len(body["attempts"]) == 8
    assert body["attempts"][0]["detail_loaded"] == 0
    assert body["attempts"][0]["progress"] == {}
    assert len(json.dumps(body)) < 400000
    selected = http(production_api, f"/v1/tasks/{task['id']}?attempt={first}")[2]
    assert selected["attempts"][0]["detail_loaded"] == 1
    assert len(selected["attempts"][0]["progress"]["message"]) == 100000
    assert http(production_api, f"/v1/tasks/{task['id']}?attempt=unknown")[0] == 404


def test_upload_headroom_reservation_does_not_leak_on_rejection(tmp_path, monkeypatch):
    uploads = Uploads(tmp_path, min_free_bytes=100)
    connection = SimpleNamespace(gettimeout=lambda: 10, settimeout=lambda value: None)
    monkeypatch.setattr(
        "dataflowcore.uploads.shutil.disk_usage", lambda path: SimpleNamespace(free=50)
    )
    with pytest.raises(UploadError) as rejected:
        uploads.receive("key", "input.txt", 1, io.BytesIO(b"x"), connection)
    assert rejected.value.status == 507
    assert not uploads.active and not uploads.reservations
    monkeypatch.setattr(
        "dataflowcore.uploads.shutil.disk_usage", lambda path: SimpleNamespace(free=1000)
    )
    assert uploads.receive("key", "input.txt", 1, io.BytesIO(b"x"), connection)[0] == 201
    assert not uploads.active and not uploads.reservations


def test_failed_output_gc_is_dry_run_by_default_and_protects_successes_and_links(
    store, spec, tmp_path
):
    register(store)
    failed = store.submit(TaskSpec.parse(spec.json() | {"max_attempts": 1}), "failed")
    a = store.claim("worker", "failed")
    complete(store, a, state="FAILED", error="failure")
    output = tmp_path / "outputs" / failed["id"] / a["attempt_id"]
    output.mkdir(parents=True)
    (output / "partial").write_text("partial")
    success = store.submit(spec, "success")
    b = store.claim("worker", "success")
    good = tmp_path / "outputs" / success["id"] / b["attempt_id"]
    good.mkdir(parents=True)
    (good / "result").write_text("good")
    complete(store, b, result={"output_dir": str(good)})
    with store.transaction() as db:
        db.execute("UPDATE attempts SET finished_at=?", (time.time() - 10 * 86400,))
    preview = clean_failed_outputs(store, tmp_path)
    assert preview["candidates"][0]["status"] == "eligible"
    assert output.exists() and good.exists()
    with store.transaction() as db:
        db.execute(
            "UPDATE tasks SET result=? WHERE id=?",
            (json.dumps({"output_dir": str(output)}), success["id"]),
        )
    assert (
        clean_failed_outputs(store, tmp_path, apply=True)["candidates"][0]["status"]
        == "protected_reference"
    )
    with store.transaction() as db:
        db.execute(
            "UPDATE tasks SET result=? WHERE id=?",
            (json.dumps({"output_dir": str(good)}), success["id"]),
        )
    output.rename(output.with_name("outside"))
    output.symlink_to(output.with_name("outside"), target_is_directory=True)
    assert (
        clean_failed_outputs(store, tmp_path, apply=True)["candidates"][0]["status"]
        == "protected_symlink"
    )
    output.unlink()
    output.with_name("outside").rename(output)
    assert clean_failed_outputs(store, tmp_path, apply=True)["candidates"][0]["status"] == "removed"
    assert not output.exists() and good.exists()
    assert not clean_failed_outputs(store, tmp_path, apply=True)["candidates"]
    assert store.get(failed["id"])["state"] == "FAILED"


def test_postgres_control_reads_a_consistent_snapshot(store):
    if not store.postgres:
        pytest.skip("PostgreSQL snapshot isolation")
    with store.transaction(False) as db:
        assert not store.paused(db)
        store.set_paused(True)
        assert not store.paused(db)
    assert store.control()["paused"]


def test_lost_postgres_ownership_stops_the_controller(production_api, store):
    if not store.postgres:
        pytest.skip("PostgreSQL ownership connection")
    ownership = ControlLock(store)
    production_api.ownership = ownership
    try:
        ownership.connection.close()
        until(lambda: not production_api.ownership_healthy)
        until(lambda: production_api.stopped.is_set())
    finally:
        ownership.close()
