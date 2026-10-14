"""A live task must stop on lease expiry even while HTTP renewal is blocked."""

import json
import signal
import subprocess
import sys
import threading
import time

from dataflowcore.worker import Running, Worker
from tests.test_e2e import until


class Logs:
    def tail(self):
        return ""

    def close(self):
        pass


def test_independent_watchdog_kills_live_task_during_blocked_network(tmp_path):
    worker = Worker("http://127.0.0.1:1", "w" * 32, tmp_path, tmp_path / "work", stop_grace=0.1)
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
    )
    assignment = {"task_id": "task", "attempt_id": "attempt", "token": "token"}
    running = Running(assignment, process, tmp_path, time.monotonic() + 0.2, Logs())
    worker.running["attempt"] = running
    worker.registered = True
    watchdog = threading.Thread(target=worker.lease_watchdog)
    watchdog.start()
    health = threading.Thread(target=worker.health_loop)
    health.start()
    blocked = threading.Event()

    def request(endpoint, data):
        if endpoint == "pulse":
            blocked.wait(5)
            return {
                "draining": False,
                "claims": [],
                "renewals": [{"aid": "attempt", "lease_seconds": 30, "cancel": False}],
            }
        raise AssertionError(endpoint)

    worker.request = request
    tick = threading.Thread(target=worker.tick)
    tick.start()
    try:
        assert until(lambda: process.poll() is not None, timeout=2)
        assert tick.is_alive(), "networking is still blocked when the process is terminated"
        assert running.invalid
        probe = until(lambda: Worker.read_json(tmp_path / "work" / "health.json"))
        assert time.time() - probe["updated_at"] < 2
        assert probe["watchdog_age_seconds"] < 1
    finally:
        blocked.set()
        tick.join(timeout=5)
        worker.watchdog_stopped.set()
        watchdog.join(timeout=2)
        health.join(timeout=2)
        if process.poll() is None:
            worker.signal_group(running, signal.SIGKILL)
        process.wait(timeout=3)


def test_escaped_unicode_log_tail_cannot_break_lease_renewal(tmp_path):
    from dataflowcore.contracts import canonical

    class UnicodeLogs(Logs):
        def tail(self):
            return "\ufffd" * 16384

    worker = Worker("http://127.0.0.1:1", "w" * 32, tmp_path, tmp_path / "work")
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
    )
    (tmp_path / "progress.json").write_text(json.dumps({"message": "x" * 195000}))
    running = Running(
        {"task_id": "task", "attempt_id": "attempt", "token": "token"},
        process,
        tmp_path,
        time.monotonic() + 30,
        UnicodeLogs(),
    )
    worker.running["attempt"] = running
    worker.registered = True
    renewed = []

    def request(endpoint, data):
        assert endpoint == "pulse"
        progress = data["renewals"][0]["progress"]
        assert len(canonical(progress)) <= 262144
        assert progress["log_tail"]
        renewed.append(True)
        return {
            "draining": False,
            "claims": [],
            "renewals": [{"aid": "attempt", "lease_seconds": 30, "cancel": False}],
        }

    worker.request = request
    try:
        worker.tick()
        assert renewed
    finally:
        worker.signal_group(running, signal.SIGKILL)
        process.wait(timeout=3)
