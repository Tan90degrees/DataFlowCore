"""Explain admission barriers without changing fairness or releasing failed work."""

import json

import pytest

from dataflowcore.contracts import Invalid
from dataflowcore.resources import WAIT_REASONS, ResourceBudget
from tests.conftest import register
from tests.test_production import production_api as production_api


@pytest.mark.parametrize(
    "capacity,kind,held,requested,same_owner,expected",
    [
        ((1, 1024, 8), "cpu", (1, 32), (1, 32), False, ["cpu"]),
        ((4, 64, 8), "io", (0, 64), (0, 1), False, ["memory"]),
        ((4, 1024, 1), "io", (0, 1), (0, 1), False, ["io_concurrency"]),
        ((4, 1024, 8), "cpu", (1, 32), (1, 32), True, ["cpu_pool"]),
        ((4, 1024, 8), "io", (0, 1), (0, 1), True, ["io_pool"]),
        ((1, 64, 1), "io", (1, 64), (1, 1), False, ["cpu", "memory", "io_concurrency"]),
    ],
)
def test_wait_diagnostics_identify_actual_barriers(
    tmp_path, capacity, kind, held, requested, same_owner, expected
):
    budget = ResourceBudget(tmp_path / "budget", *capacity)
    owner = "held" if same_owner else "waiting"
    try:
        budget.configure_owner("held", 1, 1)
        assert budget.acquire("held", "active", kind, {"cpu": held[0], "memory_mb": held[1]})
        admission = budget.admit(
            owner, [("next", kind, {"cpu": requested[0], "memory_mb": requested[1]})]
        )
        assert "next" not in admission["granted"] and admission["waiting"]["next"] == expected
        current = budget.snapshot()
        assert current["waiting_operators"] == 1
        assert current["waiting_by_reason"] == {
            reason: int(reason in expected) for reason in WAIT_REASONS
        }
        budget.release("held", "active")
        assert budget.admit(owner, [])["granted"] == {"next"}
        assert budget.snapshot()["waiting_by_reason"] == dict.fromkeys(WAIT_REASONS, 0)
    finally:
        budget.close()


def test_aging_diagnosis_distinguishes_protection_from_cpu_exhaustion(tmp_path, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr("dataflowcore.resources.monotonic", lambda: clock[0])
    budget = ResourceBudget(tmp_path / "budget", 4, 1024, aging_seconds=2)
    try:
        assert budget.acquire("held", "active", "cpu", {"cpu": 2, "memory_mb": 32})
        assert not budget.acquire("old", "large", "cpu", {"cpu": 4, "memory_mb": 32})
        clock[0] = 2.1
        result = budget.admit("new", [("small", "cpu", {"cpu": 1, "memory_mb": 32})])
        assert result["waiting"]["small"] == ["aging"]
        assert budget.admit("old", [])["waiting"]["large"] == ["cpu"]
        snapshot = budget.snapshot()
        assert snapshot["protected_resources"] == {"cpu": 4, "memory_mb": 32, "io_operators": 0}
        assert snapshot["waiting_by_reason"]["aging"] == 1
        budget.release("held")
        assert budget.admit("old", [])["granted"] == {"large"}
        assert budget.snapshot()["protected_resources"] is None
    finally:
        budget.close()


def test_completion_batch_releases_budget_and_wakes_other_owner(tmp_path):
    first = ResourceBudget(tmp_path / "budget", 2, 1024, aging_seconds=0)
    second = ResourceBudget(tmp_path / "budget")
    try:
        assert first.schedule(
            "a", [(str(index), "cpu", {"cpu": 1, "memory_mb": 32}) for index in range(2)]
        ) == {"0", "1"}
        assert not second.acquire("b", "large", "cpu", {"cpu": 2, "memory_mb": 64})
        second.wait(0)
        first.release_many("a", ["0", "1"])
        second.wait(0.1)
        assert second.admit("b", [])["granted"] == {"large"}
        assert first.attempt_peak("a") == {"cpu": 2, "memory_mb": 64}
        assert first.snapshot()["cpu"] == 2
    finally:
        second.close()
        first.close()


def test_fixed_reason_metrics_and_malformed_telemetry_rollback(production_api, store):
    from urllib.request import urlopen

    register(store)
    store.pulse("worker", telemetry={"scheduler": {"waiting_by_reason": {"cpu": 2, "aging": 1}}})
    with urlopen(f"http://127.0.0.1:{production_api.server_port}/metrics") as response:
        metrics = response.read().decode()
    assert 'dataflow_operator_waiting{reason="cpu"} 2' in metrics
    assert 'dataflow_operator_waiting{reason="aging"} 1' in metrics
    assert 'dataflow_operator_waiting{reason="memory"} 0' in metrics
    previous = store.workers()[0]["last_seen"]
    for reasons in ({"user-task-id": 1}, {"cpu": -1}, {"cpu": True}, {"cpu": 1.5}, []):
        with pytest.raises(Invalid):
            store.pulse("worker", telemetry={"scheduler": {"waiting_by_reason": reasons}})
    assert store.workers()[0]["last_seen"] == previous
    assert store.telemetry()["waiting_by_reason"]["cpu"] == 2


def test_batched_progress_keeps_first_run_wait_and_terminal_diagnostics(tmp_path, monkeypatch):
    import dataflowcore.runtime as engine

    fsyncs = []
    monkeypatch.setattr(engine.os, "fsync", lambda fd: fsyncs.append(fd))
    path = tmp_path / "progress.json"
    progress = engine.Progress(path, [{"id": f"step{i}"} for i in range(1000)], flush_interval=60)
    progress.update("step0", state="RUNNING")
    assert json.loads(path.read_text())["steps"]["step0"]["state"] == "RUNNING"
    for index in range(1, 1000):
        progress.update(f"step{index}", state="WAITING_RESOURCES", wait_reasons=["memory"])
    progress.update(force=True)
    assert json.loads(path.read_text())["steps"]["step999"]["wait_reasons"] == ["memory"]
    assert not fsyncs
    progress.finish("FAILED")
    assert len(fsyncs) == 1
    assert json.loads(path.read_text())["steps"]["step999"]["state"] == "CANCELLED"
