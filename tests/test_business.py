"""Business assertions: artifacts, external side effects and fault recovery."""

import hashlib
import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from dataflowcore.examples.embedding_fixture import FixtureServer
from dataflowcore.examples.ingestion import fixture_vector, pipeline, records
from tests.test_e2e import until


@pytest.fixture
def model():
    server = FixtureServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()
    thread.join(timeout=3)


def submit_file(cluster, text="这是业务文档 DataFlow.\n" * 80, **parameters):
    source = cluster.root / "business.md"
    source.write_text(text, encoding="utf-8")
    flow = pipeline(
        retry_delay=0,
        timeout=30,
        parameters={
            "chunk_size": 128,
            "overlap": 16,
            "document_id": "business-001",
            **parameters,
        },
    )
    return cluster.client.submit(flow.spec(source))


def index_rows(root):
    with sqlite3.connect(root / "index.sqlite") as conn:
        return conn.execute(
            "SELECT document, version, ordinal, payload FROM chunks ORDER BY ordinal"
        ).fetchall()


def verify(task, root):
    assert task["state"] == "SUCCEEDED", task["error"]
    result = task["result"]
    assert result["runtime"]["gil_enabled"] is False
    assert result["usage"]["peak_rss_bytes"] > 0
    assert len(result["files"]) == 4
    for manifest in result["files"]:
        path = Path(result["output_dir"]) / manifest["path"]
        assert path.stat().st_size == manifest["bytes"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == manifest["sha256"]
    text = (root / "business.md").read_text(encoding="utf-8")
    chunks = list(records(result["steps"]["chunk"]["path"]))
    reconstructed = chunks[0]["text"] + "".join(c["text"][16:] for c in chunks[1:])
    assert reconstructed == text
    rows = [r for r in index_rows(root) if r[1] == result["steps"]["receipt"]["version"]]
    assert len(rows) == len(chunks)
    for row, chunk in zip(rows, chunks, strict=True):
        payload = json.loads(row[3])
        assert payload["text"] == chunk["text"]
        assert payload["vector"] == fixture_vector(chunk["text"])
    progress = task["attempts"][-1]["progress"]
    assert progress["fraction"] == 1
    assert progress["state"] == "SUCCEEDED"
    assert all(s["duration_seconds"] >= 0 for s in progress["steps"].values())
    assert all(s["state"] == "SUCCEEDED" for s in progress["steps"].values())


def test_business_artifacts_vectors_progress_and_repeated_submission(cluster):
    cluster.worker(slots=2)
    task = submit_file(cluster)
    final = cluster.state(task["id"], "SUCCEEDED")
    verify(final, cluster.root)
    first_rows = index_rows(cluster.root)
    repeated = submit_file(cluster)
    verify(cluster.state(repeated["id"], "SUCCEEDED"), cluster.root)
    assert index_rows(cluster.root) == first_rows
    statuses = cluster.client.wait_many([task["id"], repeated["id"]])
    assert len(statuses) == 2
    assert all(s["progress"]["fraction"] == 1 for s in statuses)
    assert {t["id"] for t in cluster.client.iter_tasks(page_size=1, pool="default")} == {
        task["id"],
        repeated["id"],
    }


def test_business_killed_after_index_commit_retries_without_duplicate_rows(cluster):
    worker = cluster.worker()
    task = submit_file(cluster, receipt_delay=2)
    cluster.state(task["id"], "RUNNING")
    until(
        lambda: (
            cluster.client.get(task["id"])["attempts"][-1]["progress"]
            .get("steps", {})
            .get("receipt", {})
            .get("state")
            == "RUNNING"
        )
    )
    assert len(index_rows(cluster.root)) > 0
    worker.kill()
    worker.wait(timeout=3)
    cluster.worker()
    final = cluster.state(task["id"], "SUCCEEDED")
    assert [a["state"] for a in final["attempts"]] == ["LOST", "SUCCEEDED"]
    verify(final, cluster.root)


def test_business_invalid_file_permanent_failure_then_manual_retry(cluster):
    cluster.worker()
    task = submit_file(cluster, text="")
    final = cluster.state(task["id"], "FAILED")
    assert final["attempt_count"] == 1
    assert "empty document" in final["error"]
    # Manual retry retains the immutable input contract; fixed bytes require a new submit.
    retried = cluster.client.retry(task["id"], key="manual-retry")
    assert cluster.state(retried["id"], "FAILED")["attempt_count"] == 1
    assert retried["id"] != task["id"]
    good = submit_file(cluster)
    verify(cluster.state(good["id"], "SUCCEEDED"), cluster.root)


def test_worker_drain_finishes_assigned_business_and_stops_claims(cluster):
    cluster.worker()
    task = submit_file(cluster, receipt_delay=1)
    running = cluster.state(task["id"], "RUNNING")
    session = running["attempts"][0]["worker_session"]
    cluster.client.drain(session)
    cluster.state(task["id"], "SUCCEEDED")
    queued = submit_file(cluster)
    until(lambda: cluster.client.request("GET", "/v1/workers")["workers"][0]["draining"])
    assert cluster.client.get(queued["id"])["state"] == "QUEUED"
    cluster.worker()
    assert cluster.state(queued["id"], "SUCCEEDED")


@pytest.mark.parametrize("length", [1, 128, 129, 240, 32769])
def test_chunk_exact_boundary_does_not_emit_overlap_only_tail(cluster, length):
    cluster.worker()
    task = submit_file(cluster, text="x" * length)
    verify(cluster.state(task["id"], "SUCCEEDED"), cluster.root)


def test_business_real_http_embedding_transient_failure_recovers(cluster, model):
    model.failures = 1
    cluster.worker()
    task = submit_file(cluster, embedding_url=model.url)
    final = cluster.state(task["id"], "SUCCEEDED")
    assert [a["state"] for a in final["attempts"]] == ["FAILED", "SUCCEEDED"]
    verify(final, cluster.root)


def test_business_real_http_bad_request_is_permanent(cluster, model):
    model.failures, model.error_status = 1000, 400
    cluster.worker()
    task = submit_file(cluster, embedding_url=model.url)
    final = cluster.state(task["id"], "FAILED")
    assert final["attempt_count"] == 1
    assert "HTTP 400" in final["error"]


def test_business_changed_chunking_has_distinct_immutable_index_version(cluster):
    cluster.worker()
    first = submit_file(cluster)
    final = cluster.state(first["id"], "SUCCEEDED")
    original = final["result"]["steps"]["index"]["version"]
    second = submit_file(cluster, chunk_size=256)
    changed = cluster.state(second["id"], "SUCCEEDED")
    verify(changed, cluster.root)
    assert changed["result"]["steps"]["index"]["version"] != original
    assert len({r[1] for r in index_rows(cluster.root)}) == 2


def test_business_cancel_during_http_embeddings_does_not_index(cluster, model):
    model.delay = 0.1
    cluster.worker()
    task = submit_file(cluster, text="文档测试。" * 2000, embedding_url=model.url)
    cluster.state(task["id"], "RUNNING")
    until(
        lambda: (
            cluster.client.get(task["id"])["attempts"][-1]["progress"]
            .get("steps", {})
            .get("embed", {})
            .get("state")
            == "RUNNING"
        )
    )
    cluster.client.cancel(task["id"])
    final = cluster.state(task["id"], "CANCELLED")
    assert final["attempt_count"] == 1
    assert not (cluster.root / "index.sqlite").exists()
    good = submit_file(cluster)
    verify(cluster.state(good["id"], "SUCCEEDED"), cluster.root)


def test_business_deadline_and_retry_limit_with_slow_model(cluster, model):
    model.delay = 0.2
    cluster.worker()
    source = cluster.root / "business.md"
    source.write_text("文档测试。" * 2000)
    flow = pipeline(
        timeout=0.5,
        max_attempts=2,
        retry_delay=0,
        parameters={
            "embedding_url": model.url,
            "chunk_size": 128,
            "overlap": 16,
        },
    )
    task = cluster.client.submit(flow.spec(source))
    final = cluster.state(task["id"], "FAILED")
    assert len(final["attempts"]) == 2
    assert all(a["state"] == "LOST" for a in final["attempts"])
    assert final["error"] == "deadline exceeded"
    assert not (cluster.root / "index.sqlite").exists()


def test_business_sdk_wait_survives_control_unavailability(cluster):
    cluster.worker()
    task = submit_file(cluster, receipt_delay=2)
    cluster.state(task["id"], "RUNNING")
    with ThreadPoolExecutor(max_workers=1) as pool:
        waiting = pool.submit(cluster.client.wait_many, [task["id"]], timeout=20, interval=0.05)
        cluster.control.kill()
        cluster.control.wait(timeout=3)
        time.sleep(0.2)
        cluster.control = cluster.start_control()
        assert waiting.result(timeout=20)[0]["state"] == "SUCCEEDED"


def test_sdk_wait_unknown_task_and_deadline(cluster):
    from dataflowcore.client import APIError

    with pytest.raises(APIError) as exc:
        cluster.client.wait_many(["missing"])
    assert exc.value.status == 404
    queued = submit_file(cluster)
    with pytest.raises(TimeoutError):
        cluster.client.wait_many([queued["id"]], timeout=0.1, interval=0.01)


def test_node_configuration_overrides_propagate_to_immutable_index_identity(cluster):
    cluster.worker()
    steps = pipeline().steps
    next(step for step in steps if step["id"] == "chunk")["parameters"] = {"chunk_size": 16}
    next(step for step in steps if step["id"] == "embed")["parameters"] = {
        "embedding_profile": "node-profile",
    }
    parameters = {"chunk_size": 64, "overlap": 0, "embedding_profile": "task-profile"}
    first = cluster.submit(steps=steps, parameters=parameters)
    first = cluster.state(first["id"], "SUCCEEDED")
    embedded = first["result"]["steps"]["embed"]
    assert embedded["chunk_size"] == 16
    assert embedded["embedding_profile"] == "node-profile"
    next(step for step in steps if step["id"] == "chunk")["parameters"] = {"chunk_size": 32}
    second = cluster.submit(steps=steps, parameters=parameters)
    second = cluster.state(second["id"], "SUCCEEDED")
    assert first["spec"]["parameters"] == second["spec"]["parameters"]
    assert (
        first["result"]["steps"]["index"]["version"]
        != (second["result"]["steps"]["index"]["version"])
    )
    assert first["result"]["steps"]["index"]["chunks"] == 2
    assert second["result"]["steps"]["index"]["chunks"] == 1
