import json
import threading
from dataclasses import replace
from pathlib import Path

import pytest

from dataflowcore.runtime import Cancelled, Context, Progress


def context(tmp_path):
    progress = Progress(tmp_path / "progress.json", [{"id": "step"}])
    return Context(
        "task", "attempt", "step", tmp_path / "input", tmp_path, {}, threading.Event(), progress
    )


def test_context_map_is_ordered_and_bounds_lazy_producer(tmp_path):
    ctx = context(tmp_path)
    release = threading.Event()
    produced = []

    def source():
        for i in range(100):
            produced.append(i)
            yield i

    def work(i):
        release.wait(5)
        return i * 2

    result = ctx.map(work, source(), max_workers=2, max_pending=4)
    first = []
    thread = threading.Thread(target=lambda: first.append(next(result)))
    thread.start()
    # The first result cannot complete, so only the bounded prefetch can run.
    from tests.test_e2e import until

    until(lambda: len(produced) >= 4)
    assert len(produced) <= 5
    release.set()
    thread.join(timeout=5)
    assert first + list(result) == [i * 2 for i in range(100)]


def test_context_map_cancellation_and_exception_propagation(tmp_path):
    ctx = context(tmp_path)
    ctx.cancelled.set()
    with pytest.raises(Cancelled):
        list(ctx.map(lambda x: x, range(4), max_workers=2))
    fresh = replace(ctx, cancelled=threading.Event())
    with pytest.raises(ZeroDivisionError):
        list(fresh.map(lambda x: 1 / x, [1, 0, 2], max_workers=2))


def test_context_map_early_close_waits_for_inflight_artifact_writes(tmp_path):
    ctx = context(tmp_path)
    started, release, closed = threading.Event(), threading.Event(), threading.Event()

    def write(i):
        if i == 1:
            started.set()
            release.wait(5)
            (tmp_path / "artifact.txt").write_text("complete")
        return i

    items = ctx.map(write, [0, 1, 2], max_workers=2, max_pending=2)
    assert next(items) == 0
    assert started.wait(5)

    def close():
        items.close()
        closed.set()

    thread = threading.Thread(target=close)
    thread.start()
    assert not closed.wait(0.1)
    release.set()
    thread.join(timeout=5)
    assert closed.is_set()
    assert (tmp_path / "artifact.txt").read_text() == "complete"


def test_progress_coalesces_updates_but_flushes_terminal_state(tmp_path):
    path = tmp_path / "progress.json"
    progress = Progress(path, [{"id": "step"}, {"id": "later"}], flush_interval=60)
    progress.update("step", state="RUNNING")
    before = path.read_bytes()
    for i in range(10000):
        progress.update("step", completed=i, total=10000)
    assert path.read_bytes() == before
    progress.update("step", state="FAILED", error="test")
    progress.finish("FAILED")
    snapshot = json.loads(path.read_text())
    assert snapshot["steps"]["step"]["completed"] == 9999
    assert snapshot["steps"]["later"]["state"] == "SKIPPED"
    assert snapshot["state"] == "FAILED"
    progress.update("later", state="SUCCEEDED")
    assert json.loads(Path(path).read_text()) == snapshot


def test_maximum_dag_progress_preserves_states_within_renewal_budget(tmp_path):
    from dataflowcore.contracts import canonical

    path = tmp_path / "progress.json"
    steps = [{"id": f"{i:04d}" + "x" * 96} for i in range(1000)]
    progress = Progress(path, steps)
    for step in steps:
        progress.update(step["id"], state="RUNNING", completed=1000000, total=1000000)
        progress.update(step["id"], state="SUCCEEDED")
    progress.finish("SUCCEEDED")
    snapshot = json.loads(path.read_text())
    assert len(canonical(snapshot)) <= 200000
    assert snapshot["completed_steps"] == 1000
    assert all(s["state"] == "SUCCEEDED" for s in snapshot["steps"].values())
