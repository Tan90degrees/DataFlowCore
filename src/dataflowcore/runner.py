"""Resident slot process. A slot executes exactly one complete file DAG at a time."""

import json
import os
import signal
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from .runner_runtime import RunnerRuntime
from .runtime import atomic_json, child_main, require_free_threading
from .worker import LogCapture, parent_death_signal


@contextmanager
def task_logs(path):
    """Redirect native/Python stdout and stderr to a bounded per-attempt capture."""
    sys.stdout.flush()
    sys.stderr.flush()
    original = (os.dup(1), os.dup(2))
    read_fd, write_fd = os.pipe()
    capture = LogCapture(path)
    capture.start(os.fdopen(read_fd, "rb"))
    try:
        os.dup2(write_fd, 1)
        os.dup2(write_fd, 2)
        os.close(write_fd)
        yield
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        os.dup2(original[0], 1)
        os.dup2(original[1], 2)
        os.close(original[0])
        os.close(original[1])
        capture.close()


def group_has_children():
    # Children must stay in the runner's process group. Recycle instead of letting
    # a previous task's descendants survive into a new task.
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                return True
        except ChildProcessError:
            return False


def runner_main(root, data_root, parent_pid, dag_workers, map_workers, max_tasks, allow_gil=False):
    parent_death_signal(parent_pid)
    if sys.platform == "linux":
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
            raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER failed")
    require_free_threading(allow_gil)
    root = Path(root)
    runtime = RunnerRuntime(dag_workers, map_workers)
    completed = None

    def shutdown(*_):
        raise SystemExit(0)

    try:
        atomic_json(root / "state.json", {"state": "READY", "pid": os.getpid()})
        while True:
            signal.signal(signal.SIGTERM, shutdown)
            signal.signal(signal.SIGINT, shutdown)
            job_path = root / "job.json"
            try:
                job = json.loads(job_path.read_text())
            except FileNotFoundError:
                time.sleep(0.02)
                continue
            job_path.unlink()
            workspace = Path(job["workspace"])
            atomic_json(root / "state.json", {"state": "BUSY", "attempt_id": job["attempt_id"]})
            with task_logs(workspace / "task.log"):
                payload = child_main(
                    workspace / "assignment.json", workspace, data_root, allow_gil, runtime
                )
            completed = job["attempt_id"]
            runtime.completed_tasks += 1
            retire = (
                payload["state"] != "SUCCEEDED"
                or not runtime.safe_to_reuse()
                or group_has_children()
                or (max_tasks and runtime.completed_tasks >= max_tasks)
            )
            atomic_json(
                root / "state.json",
                {
                    "state": "RETIRE" if retire else "READY",
                    "pid": os.getpid(),
                    "completed_attempt": completed,
                    "completed_tasks": runtime.completed_tasks,
                },
            )
            if retire:
                return
    finally:
        runtime.close()
