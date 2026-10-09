"""A bounded local DAG engine, running entirely inside one task process."""

import asyncio
import hashlib
import importlib
import inspect
import json
import os
import signal
import sys
import sysconfig
import threading
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from .contracts import Invalid, TaskSpec, canonical


class PermanentError(Exception):
    """Operator error which must not be automatically retried."""


class Cancelled(Exception):
    pass


def require_free_threading(allow_gil=False):
    available = sysconfig.get_config_var("Py_GIL_DISABLED") == 1
    enabled = getattr(sys, "_is_gil_enabled", lambda: True)()
    if not allow_gil and (not available or enabled):
        raise PermanentError("Python 3.14t with GIL disabled is required, including after imports")
    return {"python": sys.version, "free_threaded_build": available, "gil_enabled": enabled}


def under_root(path, root):
    resolved, base = Path(path).resolve(), Path(root).resolve()
    if not resolved.is_relative_to(base):
        raise Invalid("path is outside the configured data root")
    return resolved


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_json(path, value):
    path = Path(path)
    temp = path.with_suffix(".tmp")
    with temp.open("w") as stream:
        stream.write(canonical(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


class Progress:
    def __init__(self, path, steps):
        self.path = path
        self.lock = threading.Lock()
        self.steps = {s["id"]: {"state": "PENDING"} for s in steps}
        self.update()

    def update(self, sid=None, **values):
        with self.lock:
            if sid:
                self.steps[sid].update(values)
            completed = sum(s["state"] == "SUCCEEDED" for s in self.steps.values())
            snapshot = {
                "steps": self.steps,
                "completed_steps": completed,
                "total_steps": len(self.steps),
            }
            if len(canonical(snapshot)) > 200_000:
                for step in self.steps.values():
                    step.pop("message", None)
                    step.pop("error", None)
            atomic_json(self.path, snapshot)


@dataclass(frozen=True)
class Context:
    task_id: str
    attempt_id: str
    step_id: str
    input_path: Path
    output_dir: Path
    parameters: dict
    cancelled: threading.Event
    _progress: Progress

    def check_cancelled(self):
        if self.cancelled.is_set():
            raise Cancelled("task cancellation requested")

    def report(self, completed, total=None, message=""):
        self.check_cancelled()
        if type(completed) is not int or completed < 0:
            raise Invalid("completed must be a nonnegative integer")
        if total is not None and (type(total) is not int or total < completed):
            raise Invalid("total must be an integer >= completed")
        self._progress.update(
            self.step_id, completed=completed, total=total, message=str(message)[:2000]
        )


def resolve_symbol(reference):
    module, name = reference.split(":")
    fn = getattr(importlib.import_module(module), name)
    if not callable(fn):
        raise PermanentError(f"{reference} is not callable")
    # Classes create an instance per task/step, never shared between task processes.
    return fn() if inspect.isclass(fn) else fn


def run_dag(assignment, workspace, data_root, allow_gil=False):
    spec = TaskSpec.parse(assignment["spec"])
    cancelled = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: cancelled.set())
    signal.signal(signal.SIGINT, lambda *_: cancelled.set())
    source = under_root(spec.input_path, data_root)
    if not source.is_file():
        raise PermanentError("input file is missing")
    actual_hash = file_hash(source)
    if spec.input_sha256 and actual_hash != spec.input_sha256:
        raise PermanentError("input file changed since submission")
    output = under_root(
        Path(data_root) / "outputs" / assignment["task_id"] / assignment["attempt_id"], data_root
    )
    output.mkdir(parents=True, exist_ok=False)
    functions = {s["id"]: resolve_symbol(s["callable"]) for s in spec.steps}
    runtime_info = require_free_threading(allow_gil)
    progress = Progress(Path(workspace) / "progress.json", spec.steps)
    results, running, submitted = {}, {}, set()
    pool = ThreadPoolExecutor(max_workers=spec.dag_workers, thread_name_prefix="dag")

    def execute(step, inputs):
        sid = step["id"]
        context = Context(
            assignment["task_id"],
            assignment["attempt_id"],
            sid,
            source,
            output,
            {**spec.parameters, **step.get("parameters", {})},
            cancelled,
            progress,
        )
        context.check_cancelled()
        progress.update(sid, state="RUNNING")
        try:
            # Operators must treat dependency results as read-only.
            value = functions[sid](context, MappingProxyType(inputs))
            if inspect.isawaitable(value):
                value = asyncio.run(value)
            context.check_cancelled()
            encoded = canonical(value)
            if len(encoded) > 262144:
                raise PermanentError("step result too large; return a file reference instead")
            progress.update(sid, state="SUCCEEDED")
            return value
        except Exception as exc:
            progress.update(
                sid, state="CANCELLED" if cancelled.is_set() else "FAILED", error=str(exc)[:4000]
            )
            raise

    try:
        while len(results) < len(spec.steps):
            if cancelled.is_set():
                raise Cancelled("task cancellation requested")
            for step in spec.steps:
                deps = step.get("depends_on", [])
                if (
                    step["id"] not in submitted
                    and set(deps) <= results.keys()
                    and len(running) < spec.dag_workers
                ):
                    future = pool.submit(execute, step, {d: results[d] for d in deps})
                    running[future] = step["id"]
                    submitted.add(step["id"])
            done, _ = wait(running, timeout=0.1, return_when=FIRST_COMPLETED)
            for future in done:
                results[running.pop(future)] = future.result()
        if file_hash(source) != actual_hash:
            raise PermanentError("input file changed during execution")
        manifest = []
        for path in sorted(output.rglob("*")):
            if path.is_symlink():
                raise PermanentError("output symlinks are not supported")
            if path.is_file():
                manifest.append(
                    {
                        "path": str(path.relative_to(output)),
                        "bytes": path.stat().st_size,
                        "sha256": file_hash(path),
                    }
                )
        return {
            "steps": results,
            "output_dir": str(output),
            "files": manifest,
            "input_sha256": actual_hash,
            "runtime": runtime_info,
        }
    finally:
        cancelled.set()
        pool.shutdown(wait=False, cancel_futures=True)


def child_main(assignment_path, workspace, data_root, allow_gil=False):
    assignment = json.loads(Path(assignment_path).read_text())
    try:
        result = run_dag(assignment, workspace, data_root, allow_gil)
        payload = {"state": "SUCCEEDED", "result": result, "error": None, "retryable": False}
        if len(canonical(payload)) > 900_000:
            raise PermanentError("combined result too large; return fewer file references")
    except Cancelled as exc:
        payload = {"state": "CANCELLED", "error": str(exc), "retryable": False}
    except Exception as exc:
        traceback.print_exc()
        payload = {
            "state": "FAILED",
            "error": f"{type(exc).__name__}: {exc}"[:8000],
            "retryable": not isinstance(
                exc, (PermanentError, Invalid, ImportError, AttributeError)
            ),
        }
    atomic_json(Path(workspace) / "result.json", payload)


def sleep_cooperatively(context, seconds):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        context.check_cancelled()
        time.sleep(min(0.05, max(0, end - time.monotonic())))
