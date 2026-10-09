"""Executor supervisor with conservative local lease expiry and bounded capacity."""

import json
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from . import __version__
from .client import APIError, Client
from .runtime import atomic_json, require_free_threading

log = logging.getLogger(__name__)


@dataclass
class Running:
    assignment: dict
    process: subprocess.Popen
    workspace: Path
    deadline: float
    log_file: object
    stopped_at: float | None = None
    invalid: bool = False
    cancelling: bool = False
    payload: dict | None = None


class Worker:
    def __init__(
        self,
        url,
        token,
        data_root,
        work_root,
        slots=1,
        cpu=2,
        memory_mb=1024,
        pool="default",
        interval=2,
        stop_grace=5,
        allow_gil=False,
        name=None,
        runtime_version=__version__,
    ):
        if min(slots, cpu, memory_mb) < 1 or interval <= 0 or stop_grace < 0:
            raise ValueError("invalid worker capacity/timing")
        self.client = Client(url, token, timeout=2)
        self.session = str(uuid.uuid4())
        self.name = name or socket.gethostname()
        self.data_root = Path(data_root).resolve()
        self.root = Path(work_root).resolve() / self.session
        self.root.mkdir(parents=True, exist_ok=True)
        self.slots, self.cpu, self.memory_mb, self.pool = slots, cpu, memory_mb, pool
        self.interval, self.stop_grace, self.allow_gil = interval, stop_grace, allow_gil
        self.runtime_version = runtime_version
        self.running = {}
        self.stopped = threading.Event()
        self.registered = False
        self.claim_id = str(uuid.uuid4())
        self.lease_seconds = 30
        self.last_contact = time.monotonic()

    def request(self, endpoint, data):
        return self.client.request("POST", "/v1/worker/" + endpoint, data)

    def identity(self, running):
        a = running.assignment
        return {
            "tid": a["task_id"],
            "aid": a["attempt_id"],
            "session_id": self.session,
            "token": a["token"],
        }

    def spawn(self, assignment, request_started):
        workspace = self.root / assignment["attempt_id"]
        workspace.mkdir()
        path = workspace / "assignment.json"
        atomic_json(path, assignment)
        log_file = (workspace / "task.log").open("wb")
        command = [
            sys.executable,
            "-m",
            "dataflowcore.cli",
            "_run",
            str(path),
            "--workspace",
            str(workspace),
            "--data-root",
            str(self.data_root),
            "--parent-pid",
            str(os.getpid()),
        ]
        if self.allow_gil:
            command.append("--allow-gil")
        try:
            process = subprocess.Popen(
                command, stdout=log_file, stderr=subprocess.STDOUT, start_new_session=True
            )
        except BaseException:
            log_file.close()
            raise
        self.running[assignment["attempt_id"]] = Running(
            assignment,
            process,
            workspace,
            request_started + min(assignment["lease_seconds"], assignment["spec"]["timeout"]),
            log_file,
        )
        log.info(
            "task_started task=%s attempt=%s pid=%s",
            assignment["task_id"],
            assignment["attempt_id"],
            process.pid,
        )

    @staticmethod
    def signal_group(running, sig):
        # Task operators must keep subprocesses in this process group.
        try:
            os.killpg(running.process.pid, sig)
        except ProcessLookupError:
            pass

    def stop_task(self, running, invalid=False, cancelling=False):
        running.invalid |= invalid
        running.cancelling |= cancelling
        if running.stopped_at is None:
            running.stopped_at = time.monotonic()
            self.signal_group(running, signal.SIGTERM)
        if time.monotonic() - running.stopped_at >= self.stop_grace:
            self.signal_group(running, signal.SIGKILL)

    @staticmethod
    def read_json(path, default=None):
        try:
            return json.loads(path.read_text())
        except FileNotFoundError, ValueError:
            return default

    def clean(self, aid, running):
        # Reap the direct child, then kill leftover descendants before releasing capacity.
        running.process.wait(timeout=2)
        self.signal_group(running, signal.SIGKILL)
        running.log_file.close()
        # Keep bounded per-attempt logs for diagnostics; remove secrets/spec/progress.
        (running.workspace / "assignment.json").unlink(missing_ok=True)
        self.running.pop(aid)

    def tick(self):
        if not self.registered:
            info = self.request(
                "register",
                {
                    "session_id": self.session,
                    "name": self.name,
                    "pool": self.pool,
                    "runtime_version": self.runtime_version,
                    "slots": self.slots,
                    "cpu": self.cpu,
                    "memory_mb": self.memory_mb,
                },
            )
            self.lease_seconds = info["lease_seconds"]
            if self.interval >= self.lease_seconds / 3:
                raise ValueError("heartbeat interval must be less than one third of lease duration")
            self.registered = True
        self.request("heartbeat", {"session_id": self.session, "draining": self.stopped.is_set()})
        self.last_contact = time.monotonic()
        for aid, running in list(self.running.items()):
            now = time.monotonic()
            if now >= running.deadline:
                self.stop_task(running, invalid=True)
            if running.invalid:
                if running.process.poll() is not None:
                    self.clean(aid, running)
                continue
            try:
                progress = self.read_json(running.workspace / "progress.json", {})
                started = time.monotonic()
                renewed = self.request("renew", {**self.identity(running), "progress": progress})
                running.deadline = started + renewed["lease_seconds"]
                if renewed["cancel"]:
                    self.stop_task(running, cancelling=True)
                elif running.stopped_at is not None:
                    self.stop_task(running)
                if running.payload is None:
                    running.payload = self.read_json(running.workspace / "result.json")
                if running.payload and running.payload["state"] == "FAILED":
                    # A failing DAG may still have an uncooperative sibling thread.
                    self.stop_task(running)
                if running.process.poll() is None:
                    continue
                self.signal_group(running, signal.SIGKILL)
                if running.payload is None:
                    running.payload = {
                        "state": "FAILED",
                        "error": f"task process exited with code {running.process.returncode}",
                        "retryable": True,
                    }
                payload = running.payload
                if running.cancelling:
                    payload = {"state": "CANCELLED", "error": "cancelled", "retryable": False}
                self.request("complete", {**self.identity(running), **payload})
                self.clean(aid, running)
            except APIError as exc:
                if exc.status in (404, 409):
                    self.stop_task(running, invalid=True)
                else:
                    log.warning("attempt_request_failed status=%s attempt=%s", exc.status, aid)
            except OSError, TimeoutError:
                log.warning("attempt_request_unavailable attempt=%s", aid)
        if not self.stopped.is_set() and len(self.running) < self.slots:
            started = time.monotonic()
            try:
                assignment = self.request(
                    "claim", {"session_id": self.session, "claim_id": self.claim_id}
                )["assignment"]
            except APIError as exc:
                if exc.status == 409:
                    self.claim_id = str(uuid.uuid4())
                    return
                raise
            if assignment:
                if assignment["attempt_id"] not in self.running:
                    self.spawn(assignment, started)
                self.claim_id = str(uuid.uuid4())

    def run(self):
        require_free_threading(self.allow_gil)
        signal.signal(signal.SIGTERM, lambda *_: self.stopped.set())
        signal.signal(signal.SIGINT, lambda *_: self.stopped.set())
        next_tick = 0
        try:
            while not self.stopped.is_set() or self.running:
                now = time.monotonic()
                # Safety watchdog runs even when the control plane is unavailable.
                for aid, running in list(self.running.items()):
                    if now >= running.deadline or running.invalid:
                        self.stop_task(running, invalid=True)
                        if running.process.poll() is not None:
                            self.clean(aid, running)
                atomic_json(
                    self.root.parent / "health.json",
                    {
                        "updated_at": time.time(),
                        "session_id": self.session,
                        "draining": self.stopped.is_set(),
                        "registered": self.registered,
                    },
                )
                if now >= next_tick:
                    try:
                        self.tick()
                    except (OSError, TimeoutError, APIError) as exc:
                        log.warning("control_unavailable error=%s", type(exc).__name__)
                    next_tick = time.monotonic() + self.interval
                time.sleep(0.05)
        finally:
            for aid, running in list(self.running.items()):
                self.signal_group(running, signal.SIGKILL)
                self.clean(aid, running)
            # Never delete output files here: accepted results outlive the worker Pod.
            if not any(self.root.iterdir()):
                shutil.rmtree(self.root)


def parent_death_signal(parent_pid):
    """Linux: terminate the task child if the supervisor is killed; close the startup race."""
    if sys.platform == "linux":
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "PR_SET_PDEATHSIG failed")
    if os.getppid() != parent_pid:
        raise SystemExit("executor supervisor exited before task startup")
