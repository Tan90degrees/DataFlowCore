"""GIL and no-GIL runtimes retain the same file isolation and low-noise paths."""

import json
import subprocess
import sys
from collections import defaultdict

import pytest

from dataflowcore.cli import parser
from dataflowcore.runtime import PermanentError, check_runtime, runtime_capabilities
from dataflowcore.worker import Worker
from tests.test_e2e import until


def test_doctor_auto_and_strict_modes_match_actual_interpreter():
    result = subprocess.run(
        [sys.executable, "-m", "dataflowcore.cli", "doctor"], capture_output=True, text=True
    )
    assert result.returncode == 0
    info = json.loads(result.stdout)
    assert info == runtime_capabilities()
    strict = subprocess.run(
        [sys.executable, "-m", "dataflowcore.cli", "doctor", "--require-free-threading"],
        capture_output=True,
        text=True,
    )
    if info["gil_enabled"]:
        assert strict.returncode != 0 and "GIL disabled is required" in strict.stderr
        with pytest.raises(PermanentError):
            check_runtime(False)
    else:
        assert strict.returncode == 0 and json.loads(strict.stdout) == info


@pytest.mark.parametrize(
    "gil,slots,expected",
    [(True, None, 4), (False, None, 1), (True, "auto", 4), (True, 2, 2), (False, 2, 2)],
)
def test_auto_slots_are_fixed_at_startup_and_explicit_limits_are_preserved(
    tmp_path, monkeypatch, gil, slots, expected
):
    import dataflowcore.worker as module

    info = {**runtime_capabilities(), "gil_enabled": gil}
    monkeypatch.setattr(module, "check_runtime", lambda _: info)
    worker = Worker("http://127.0.0.1:1", "", tmp_path, tmp_path / "work", slots=slots, cpu=4)
    try:
        assert worker.slots == expected
        assert worker.runner_dag_workers == (1 if gil else 8)
        assert worker.cpu_dag_auto
        assert worker.allow_gil is gil
        assert worker.slots_auto is (slots in (None, "auto"))
    finally:
        worker.budget.close()
        worker.close_notifications()


def test_runtime_policy_stays_strict_when_free_threaded_import_enables_gil(monkeypatch):
    import dataflowcore.runtime as module

    monkeypatch.setattr(module.sysconfig, "get_config_var", lambda _: 1)
    monkeypatch.setattr(module.sys, "_is_gil_enabled", lambda: True, raising=False)
    assert check_runtime()["execution_model"] == "gil_process_slots"
    with pytest.raises(PermanentError, match="GIL disabled"):
        check_runtime(False)


def test_runtime_cli_flags_and_auto_slots():
    cli = parser()
    assert cli.parse_args(["worker"]).slots is None
    assert cli.parse_args(["worker", "--slots", "auto"]).slots is None
    assert cli.parse_args(["worker", "--slots", "3"]).slots == 3
    assert cli.parse_args(["worker"]).allow_gil is None
    assert cli.parse_args(["worker"]).runner_dag_workers is None
    assert cli.parse_args(["worker", "--runner-dag-workers", "4"]).runner_dag_workers == 4
    assert cli.parse_args(["worker", "--require-free-threading"]).allow_gil is False
    assert cli.parse_args(["worker", "--allow-gil"]).allow_gil is True
    with pytest.raises(SystemExit):
        cli.parse_args(["worker", "--slots", "0"])
    with pytest.raises(SystemExit):
        cli.parse_args(["worker", "--allow-gil", "--require-free-threading"])


def test_real_cpu_files_use_fixed_process_slots_and_reuse_cache(cluster):
    expected = 4 if runtime_capabilities()["gil_enabled"] else 1
    cluster.worker(1, "--cpu", "4", "--slots", "auto")
    until(
        lambda: (
            (cluster.client.request("GET", "/v1/workers")["workers"] or [{}])[0]
            .get("runtime", {})
            .get("runner_processes")
            == expected
        )
    )
    tasks = [
        cluster.submit(
            steps=[
                {
                    "id": "cpu",
                    "callable": "tests.resident_operators:cpu_work",
                    "workload": "cpu",
                    "resources": {"cpu": 1, "memory_mb": 32},
                    "parameters": {"rounds": 500000, "barrier": expected},
                }
            ]
        )
        for _ in range(8)
    ]
    final = [cluster.state(task["id"], "SUCCEEDED") for task in tasks]
    states = [task["result"]["steps"]["cpu"] for task in final]
    assert len({state["checksum"] for state in states}) == 1
    cached = defaultdict(set)
    for state in states:
        cached[state["pid"]].add(state["resource"])
        assert state["cpu_seconds"] > 0
    assert len(cached) == expected and all(len(values) == 1 for values in cached.values())
    assert max(state["calls"] for state in states) >= 2
    if expected > 1:
        assert any(
            a["pid"] != b["pid"] and a["started"] < b["finished"] and b["started"] < a["finished"]
            for a in states
            for b in states
        )
    assert all(task["attempt_count"] == 1 for task in final)


def test_auto_cpu_branch_admission_preserves_independent_io_progress(cluster):
    cluster.worker(1, "--cpu", "2")
    task = cluster.submit(
        steps=[
            {
                "id": sid,
                "callable": "tests.scheduling_operators:probe",
                "workload": kind,
                "resources": {"cpu": 0 if kind == "io" else 1, "memory_mb": 32},
                "parameters": {"seconds": 1.5 if kind == "io" else 0.3},
            }
            for sid, kind in (("a", "cpu"), ("b", "cpu"), ("io", "io"))
        ]
    )
    final = cluster.state(task["id"], "SUCCEEDED")
    result = final["result"]
    a, b, io = (result["steps"][sid] for sid in ("a", "b", "io"))
    overlap = a["started"] < b["finished"] and b["started"] < a["finished"]
    gil = runtime_capabilities()["gil_enabled"]
    assert overlap is not gil
    assert result["runtime"]["peak_resources"]["cpu"] == (1 if gil else 2)
    assert a["started"] < io["finished"] and io["started"] < a["finished"]
    assert len({a["pid"], b["pid"], io["pid"]}) == 1
