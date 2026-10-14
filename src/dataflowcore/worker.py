"""Executor supervisor with conservative local lease expiry and bounded capacity."""

import hashlib
import json
import logging
import os
import select
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from . import __version__
from .client import APIError, Client
from .contracts import canonical
from .observability import FailureLog
from .resources import ResourceBudget
from .runtime import atomic_json, require_free_threading

log = logging.getLogger(__name__)


class LogCapture:
    """Drain stdout continuously while keeping only a bounded diagnostic tail."""

    def __init__(self, path):
        self.file = path.open("wb")
        self.lock = threading.Lock()
        self.buffer = b""
        self.thread = None

    def start(self, stream):
        def drain():
            try:
                while chunk := stream.read1(4096):
                    with self.lock:
                        self.buffer = (self.buffer + chunk)[-16384:]
                        self.file.seek(0)
                        self.file.write(self.buffer)
                        self.file.truncate()
                        self.file.flush()
            except OSError, ValueError:
                log.warning("log_capture_closed")
            finally:
                stream.close()

        self.thread = threading.Thread(target=drain, name="task-logs", daemon=True)
        self.thread.start()

    def tail(self):
        with self.lock:
            return self.buffer.decode("utf-8", errors="replace")

    def close(self):
        if self.thread:
            self.thread.join(timeout=2)
        with self.lock:
            self.file.close()


class FileLogs:
    def __init__(self, path):
        self.path = path

    def tail(self):
        try:
            with self.path.open("rb") as stream:
                stream.seek(0, 2)
                stream.seek(max(0, stream.tell() - 16384))
                return stream.read().decode("utf-8", errors="replace")
        except FileNotFoundError:
            return ""

    def close(self):
        pass


@dataclass
class Slot:
    process: subprocess.Popen
    workspace: Path
    busy: bool = False
    logs: object = None
    started_at: float = field(default_factory=time.monotonic)
    job_fd: int | None = None

    def close_pipe(self):
        if self.job_fd is not None:
            os.close(self.job_fd)
            self.job_fd = None


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
    closed: bool = False
    lock: object = field(default_factory=threading.RLock)
    slot: object = None


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
        runner_dag_workers=8,
        runner_map_workers=8,
        runner_max_tasks=100,
        runner_io_workers=16,
        runner_io_map_workers=16,
        io_concurrency=32,
        resource_aging_seconds=2,
    ):
        if min(slots, cpu, memory_mb) < 1 or interval <= 0 or stop_grace < 0:
            raise ValueError("invalid worker capacity/timing")
        self.client = Client(url, token, timeout=2)
        self.session = str(uuid.uuid4())
        self.name = name or socket.gethostname()
        self.data_root = Path(data_root).resolve()
        self.root = Path(work_root).resolve() / self.session
        self.root.mkdir(parents=True, exist_ok=True)
        self.budget = ResourceBudget(
            self.root / "resources.sqlite", cpu, memory_mb, io_concurrency, resource_aging_seconds
        )
        self.slots, self.cpu, self.memory_mb, self.pool = slots, cpu, memory_mb, pool
        self.interval, self.stop_grace, self.allow_gil = interval, stop_grace, allow_gil
        self.runtime_version = runtime_version
        self.running = {}
        self.running_lock = threading.Lock()
        self.watchdog_stopped = threading.Event()
        self.draining = False
        self.stopped = threading.Event()
        self.registered = False
        self.claim_id = str(uuid.uuid4())
        self.pending_claims = []
        self.safety_wakeup = threading.Event()
        self.telemetry = {}
        self.progress_sent = {}
        self.failures = FailureLog()
        self.completion_read, self.completion_write = os.pipe()
        os.set_blocking(self.completion_read, False)
        os.set_blocking(self.completion_write, False)
        self.lease_seconds = 30
        self.last_contact = time.monotonic()
        if any(
            not 1 <= v <= 256
            for v in (
                runner_dag_workers,
                runner_map_workers,
                runner_io_workers,
                runner_io_map_workers,
            )
        ):
            raise ValueError("runner thread limits must be in 1..256")
        if not 0 <= runner_max_tasks <= 1000000:
            raise ValueError("runner max tasks must be in 0..1000000")
        self.runner_dag_workers, self.runner_map_workers = runner_dag_workers, runner_map_workers
        self.runner_max_tasks = runner_max_tasks
        self.runner_io_workers, self.runner_io_map_workers = (
            runner_io_workers,
            runner_io_map_workers,
        )
        self.runners = {}
        self.runner_snapshot = ()
        self.last_safety_check = time.monotonic()

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

    def ensure_runners(self):
        for index in range(self.slots):
            slot = self.runners.get(index)
            if slot is not None:
                if slot.busy or slot.process.poll() is None:
                    continue
                if time.monotonic() - slot.started_at < 1:
                    continue
                log.info(
                    "runner_replaced pid=%s code=%s tail=%s",
                    slot.process.pid,
                    slot.process.returncode,
                    slot.logs.tail()[-2000:],
                )
                self.signal_group(slot, signal.SIGKILL)
                self.budget.release(slot.workspace.name)
                slot.logs.close()
                slot.close_pipe()
                shutil.rmtree(slot.workspace)
            workspace = self.root / f"runner-{index}-{uuid.uuid4().hex}"
            workspace.mkdir()
            self.runners[index] = self.start_runner(workspace)
            self.runner_snapshot = tuple(self.runners.values())

    def start_runner(self, workspace):
        notify_read, notify_write = os.pipe()
        command = [
            sys.executable,
            "-m",
            "dataflowcore.cli",
            "_runner",
            "--workspace",
            str(workspace),
            "--data-root",
            str(self.data_root),
            "--parent-pid",
            str(os.getpid()),
            "--dag-workers",
            str(self.runner_dag_workers),
            "--map-workers",
            str(self.runner_map_workers),
            "--max-tasks",
            str(self.runner_max_tasks),
            "--budget-path",
            str(self.budget.path),
            "--io-workers",
            str(self.runner_io_workers),
            "--io-map-workers",
            str(self.runner_io_map_workers),
            "--notify-fd",
            str(notify_read),
            "--completion-fd",
            str(self.completion_write),
        ]
        if self.allow_gil:
            command.append("--allow-gil")
        capture = LogCapture(workspace / "runner.log")
        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                pass_fds=(notify_read, self.completion_write),
                env=os.environ | {"PYTHONUNBUFFERED": "1"},
            )
        except BaseException:
            os.close(notify_write)
            capture.close()
            raise
        finally:
            os.close(notify_read)
        capture.start(process.stdout)
        log.info("runner_started pid=%s slot=%s", process.pid, workspace.name)
        return Slot(process, workspace, logs=capture, job_fd=notify_write)

    def available_runner(self):
        for slot in self.runners.values():
            if not slot.busy and slot.process.poll() is None:
                if self.read_json(slot.workspace / "state.json", {}).get("state") == "READY":
                    return slot
        return None

    def spawn(self, assignment, request_started, slot):
        workspace = self.root / assignment["attempt_id"]
        workspace.mkdir()
        atomic_json(workspace / "assignment.json", assignment)
        atomic_json(workspace / "runner.json", {"pid": slot.process.pid})
        running = Running(
            assignment,
            slot.process,
            workspace,
            request_started + min(assignment["lease_seconds"], assignment["spec"]["timeout"]),
            FileLogs(workspace / "task.log"),
            slot=slot,
        )
        slot.busy = True
        with self.running_lock:
            self.running[assignment["attempt_id"]] = running
        self.safety_wakeup.set()
        atomic_json(
            slot.workspace / "job.json",
            {"workspace": str(workspace), "attempt_id": assignment["attempt_id"]},
            durable=False,
        )
        if slot.job_fd is not None:
            os.write(slot.job_fd, b"1")
        log.info(
            "task_started task=%s attempt=%s pid=%s",
            assignment["task_id"],
            assignment["attempt_id"],
            slot.process.pid,
            extra={
                "task_id": assignment["task_id"],
                "attempt_id": assignment["attempt_id"],
                "worker_session": self.session,
                "runner_id": slot.workspace.name,
                "pid": slot.process.pid,
            },
        )

    @staticmethod
    def signal_group(running, sig):
        # Task operators must keep subprocesses in this process group.
        try:
            os.killpg(running.process.pid, sig)
        except ProcessLookupError:
            pass

    def stop_task(self, running, invalid=False, cancelling=False):
        with running.lock:
            if running.closed:
                return
            running.invalid |= invalid
            running.cancelling |= cancelling
            if running.stopped_at is None:
                running.stopped_at = time.monotonic()
                self.signal_group(running, signal.SIGTERM)
            if time.monotonic() - running.stopped_at >= self.stop_grace:
                self.signal_group(running, signal.SIGKILL)

    def lease_watchdog(self):
        # Networking can block for seconds per attempt. Lease safety must not share
        # the networking loop, especially with many concurrent file tasks.
        while not self.watchdog_stopped.is_set():
            self.last_safety_check = time.monotonic()
            with self.running_lock:
                items = list(self.running.values())
            for running in items:
                with running.lock:
                    if time.monotonic() >= running.deadline or running.invalid:
                        self.stop_task(running, invalid=True)
            self.safety_wakeup.wait(0.05 if items else 0.5)
            self.safety_wakeup.clear()

    @staticmethod
    def read_json(path, default=None):
        try:
            return json.loads(path.read_text())
        except FileNotFoundError, ValueError:
            return default

    def clean(self, aid, running):
        with running.lock:
            running.closed = True
            if running.process.poll() is not None:
                running.process.wait(timeout=2)
                self.signal_group(running, signal.SIGKILL)
                if running.slot:
                    self.budget.release(running.slot.workspace.name)
            if running.slot:
                running.slot.busy = False
        running.log_file.close()
        # Final progress/log tail is already durable; don't fill the executor's ephemeral disk.
        shutil.rmtree(running.workspace)
        with self.running_lock:
            self.running.pop(aid)
        self.progress_sent.pop(aid, None)

    def pulse(self, renewals=(), *, claim=False):
        if claim and not self.pending_claims and not self.stopped.is_set() and not self.draining:
            free = sum(
                not s.busy
                and s.process.poll() is None
                and self.read_json(s.workspace / "state.json", {}).get("state") == "READY"
                for s in self.runners.values()
            )
            self.pending_claims = [str(uuid.uuid4()) for _ in range(min(32, free))]
        claims = list(self.pending_claims) if claim else []
        started = time.monotonic()
        result = self.request(
            "pulse",
            {
                "session_id": self.session,
                "draining": self.stopped.is_set(),
                "telemetry": {
                    "scheduler": self.budget.snapshot(),
                    "runtime": self.telemetry.get("runtime", {}),
                },
                "renewals": list(renewals),
                "claims": claims,
            },
        )
        self.last_contact = time.monotonic()
        self.failures.recovered("control", log)
        self.draining = result["draining"]
        empty_queue = False
        for item in result["claims"]:
            assignment = item.get("assignment")
            if assignment and assignment["attempt_id"] not in self.running:
                slot = self.available_runner()
                if slot is None:
                    continue  # Keep the ID and replay; never abandon a delivered lease.
                self.spawn(assignment, started, slot)
            if item["claim_id"] in self.pending_claims:
                self.pending_claims.remove(item["claim_id"])
            empty_queue |= assignment is None
        return {r["aid"]: (r, started) for r in result["renewals"]}, empty_queue

    def progress(self, running):
        progress = self.read_json(running.workspace / "progress.json", {})
        progress["log_tail"] = running.log_file.tail()
        while len(canonical(progress)) > 262144 and progress["log_tail"]:
            progress["log_tail"] = progress["log_tail"][len(progress["log_tail"]) // 2 + 1 :]
        return progress

    def complete(self, running, payload):
        # A file can finish while its earlier pulse is in flight. Persist the
        # final snapshot with the result, rather than that pulse's stale progress.
        progress = self.progress(running)
        body = {**self.identity(running), **payload, "progress": progress}
        if len(canonical(body)) > 1000000:
            # Big results and diagnostics may each fit but not fit together.
            # Flush diagnostics with the fenced renewal before the result.
            started = time.monotonic()
            renewed = self.request("renew", {**self.identity(running), "progress": progress})
            with running.lock:
                if running.invalid:
                    raise APIError(409, "execution lease is no longer valid")
                running.deadline = started + renewed["lease_seconds"]
            if renewed["cancel"]:
                self.stop_task(running, cancelling=True)
                return False
            body.pop("progress")
        return self.request("complete", body)

    def renew_batch(self):
        batches, current, size, hashes = [], [], 0, {}
        for aid, running in list(self.running.items()):
            if running.invalid or time.monotonic() >= running.deadline:
                self.stop_task(running, invalid=True)
                continue
            progress = self.progress(running)
            encoded = canonical(progress)
            hashed = hashlib.sha256(encoded.encode()).hexdigest()
            hashes[aid] = hashed
            identity = self.identity(running)
            identity.pop("session_id")
            if self.progress_sent.get(aid) != hashed:
                identity["progress"] = progress
            length = len(canonical(identity))
            if current and (len(current) >= 32 or size + length > 1000000):
                batches.append(current)
                current, size = [], 0
            current.append(identity)
            size += length
        batches.append(current)
        renewed, empty_queue = {}, False
        for index, batch in enumerate(batches):
            values, empty = self.pulse(batch, claim=index == 0)
            renewed.update(values)
            empty_queue |= empty
            for aid, (value, _) in values.items():
                if "status" not in value:
                    self.progress_sent[aid] = hashes[aid]
        return renewed, empty_queue

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
        renewed_batch, empty_queue = self.renew_batch()
        original_active = len(self.running)
        for aid, running in list(self.running.items()):
            now = time.monotonic()
            if now >= running.deadline:
                self.stop_task(running, invalid=True)
            if running.invalid:
                if running.process.poll() is not None:
                    self.clean(aid, running)
                continue
            try:
                value = renewed_batch.get(aid)
                if value is None:
                    continue  # Just claimed in this pulse; its initial lease is already fenced.
                renewed, started = value
                if "status" in renewed:
                    self.stop_task(running, invalid=True)
                    continue
                with running.lock:
                    if running.invalid:
                        continue
                    running.deadline = started + renewed["lease_seconds"]
                if renewed["cancel"]:
                    self.stop_task(running, cancelling=True)
                elif running.stopped_at is not None:
                    self.stop_task(running)
                if running.payload is None:
                    running.payload = self.read_json(running.workspace / "result.json")
                status = (
                    self.read_json(running.slot.workspace / "state.json", {})
                    if running.slot
                    else {}
                )
                finished = status.get("completed_attempt") == aid
                if running.payload and running.payload["state"] != "SUCCEEDED":
                    self.stop_task(running)
                elif finished and status["state"] == "RETIRE":
                    self.stop_task(running)
                alive = running.process.poll() is None
                reusable = (
                    alive and finished and status["state"] == "READY" and running.stopped_at is None
                )
                if alive and not reusable:
                    continue
                if not alive:
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
                if self.complete(running, payload):
                    self.clean(aid, running)
            except APIError as exc:
                if exc.status in (404, 409):
                    self.stop_task(running, invalid=True)
                else:
                    log.warning("attempt_request_failed status=%s attempt=%s", exc.status, aid)
            except OSError, TimeoutError:
                log.warning("attempt_request_unavailable attempt=%s", aid)
        # Reuse slots completed in this tick, without waiting a full heartbeat interval.
        if len(self.running) < original_active and not empty_queue:
            self.pulse(claim=True)

    def health_loop(self):
        # Dependency outages can block several HTTP calls. They must affect
        # readiness rather than suppress the independent process safety probe.
        while not self.watchdog_stopped.is_set():
            now = time.monotonic()
            try:
                runtime = self.runtime_snapshot()
                self.telemetry = {"runtime": runtime}
                atomic_json(
                    self.root.parent / "health.json",
                    {
                        "updated_at": time.time(),
                        "session_id": self.session,
                        "draining": self.stopped.is_set() or self.draining,
                        "registered": self.registered,
                        "control_contact_age_seconds": now - self.last_contact,
                        "watchdog_age_seconds": now - self.last_safety_check,
                        "lease_seconds": self.lease_seconds,
                        "runner_slots": self.slots,
                        "live_runners": sum(
                            slot.process.poll() is None
                            and self.read_json(slot.workspace / "state.json", {}).get("state")
                            in ("READY", "BUSY")
                            for slot in self.runner_snapshot
                        ),
                        "scheduler": self.budget.snapshot(),
                        "runtime": runtime,
                    },
                    durable=False,
                )
            except Exception:
                log.exception("worker_health_publish_failed")
            self.watchdog_stopped.wait(2)

    def runtime_snapshot(self):
        # RSS is a sampled sum, including shared pages; it is not cgroup usage or
        # the scheduler's declared memory reservation. Null means /proc unavailable.
        pids = [os.getpid()] + [
            s.process.pid for s in self.runner_snapshot if s.process.poll() is None
        ]
        rss = []
        for pid in pids:
            try:
                pages = int(Path(f"/proc/{pid}/statm").read_text().split()[1])
                rss.append(pages * os.sysconf("SC_PAGE_SIZE") / 1048576)
            except OSError, IndexError, ValueError:
                pass
        return {
            "processes": len(pids),
            "runner_processes": len(pids) - 1,
            "sampled_rss_mb": sum(rss) if len(rss) == len(pids) else None,
            "sampled_at": time.time(),
        }

    def run(self):
        require_free_threading(self.allow_gil)

        def stop(*_):
            self.stopped.set()
            try:
                os.write(self.completion_write, b"1")
            except OSError:
                pass

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        next_tick = 0
        watchdog = threading.Thread(target=self.lease_watchdog, name="lease-watchdog", daemon=True)
        watchdog.start()
        health_thread = threading.Thread(target=self.health_loop, name="worker-health", daemon=True)
        health_thread.start()
        try:
            self.ensure_runners()
            while not self.stopped.is_set() or self.running:
                now = time.monotonic()
                # Safety watchdog runs even when the control plane is unavailable.
                for aid, running in list(self.running.items()):
                    if now >= running.deadline or running.invalid:
                        self.stop_task(running, invalid=True)
                        if running.process.poll() is not None:
                            self.clean(aid, running)
                if not self.stopped.is_set():
                    self.ensure_runners()
                if now >= next_tick:
                    try:
                        self.tick()
                    except (OSError, TimeoutError, APIError) as exc:
                        if isinstance(exc, APIError) and exc.status == 404:
                            self.registered = False
                        self.failures.failed(
                            "control", log, "control_unavailable error=%s", type(exc).__name__
                        )
                    next_tick = time.monotonic() + self.interval
                timeout = max(0.001, min(0.5, next_tick - time.monotonic()))
                if select.select([self.completion_read], [], [], timeout)[0]:
                    while True:
                        try:
                            os.read(self.completion_read, 4096)
                        except BlockingIOError:
                            break
                    # Slot readiness/completion is an event, independent of the
                    # periodic lease/progress pulse. Short files need not wait for it.
                    next_tick = 0
        finally:
            self.watchdog_stopped.set()
            self.safety_wakeup.set()
            watchdog.join(timeout=2)
            health_thread.join(timeout=6)
            for aid, running in list(self.running.items()):
                self.signal_group(running, signal.SIGKILL)
                running.process.wait(timeout=2)
                self.clean(aid, running)
            for slot in self.runners.values():
                if slot.process.poll() is None:
                    self.signal_group(slot, signal.SIGTERM)
                    try:
                        slot.process.wait(timeout=max(0.1, min(self.stop_grace, 2)))
                    except subprocess.TimeoutExpired:
                        pass
                self.signal_group(slot, signal.SIGKILL)
                slot.process.wait(timeout=2)
                self.budget.release(slot.workspace.name)
                slot.logs.close()
                slot.close_pipe()
                shutil.rmtree(slot.workspace)
            # Never delete output files here: accepted results outlive the worker Pod.
            self.budget.close()
            self.close_notifications()
            shutil.rmtree(self.root)

    def close_notifications(self):
        os.close(self.completion_read)
        os.close(self.completion_write)


def parent_death_signal(parent_pid):
    """Linux: terminate the task child if the supervisor is killed; close the startup race."""
    if sys.platform == "linux":
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "PR_SET_PDEATHSIG failed")
    if os.getppid() != parent_pid:
        raise SystemExit("executor supervisor exited before task startup")
