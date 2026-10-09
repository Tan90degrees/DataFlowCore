"""Real installed operators for resident-process lifecycle acceptance."""

import os
import subprocess
import sys
import threading
import time
import uuid


class Resource:
    def __init__(self, root):
        self.root = root
        self.identity = uuid.uuid4().hex
        self.calls = 0
        self.active = self.peak = 0
        self.lock = threading.Lock()

    def close(self):
        (self.root / f"closed-{self.identity}").write_text("closed")


class Observe:
    def __init__(self):
        self.identity = uuid.uuid4().hex

    def __call__(self, context, inputs):
        shared = context.resource("test-resource", lambda: Resource(context.input_path.parent))
        shared.calls += 1

        def work(i):
            time.sleep(0.02)
            return threading.get_ident()

        return {
            "pid": os.getpid(),
            "operator": self.identity,
            "resource": shared.identity,
            "calls": shared.calls,
            "dag_thread": threading.get_ident(),
            "map_threads": sorted(set(context.map(work, range(8), max_workers=2))),
            "parameters": context.parameters,
            "inputs": dict(inputs),
        }


def crash_once(context, inputs):
    marker = context.input_path.parent / "crashed-once"
    if not marker.exists():
        marker.write_text(str(os.getpid()))
        os._exit(7)
    return {"pid": os.getpid()}


def leftover(context, inputs):
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return {"child": process.pid, "pid": os.getpid()}


def leftover_thread(context, inputs):
    threading.Thread(target=lambda: time.sleep(60), daemon=True).start()
    return {"pid": os.getpid()}


def leave_map_running(context, inputs):
    items = context.map(lambda i: time.sleep(60) if i else i, [0, 1], max_workers=2)
    next(items)
    # Hold the iterator so GC cannot close it before the runtime safety check.
    context.resource("abandoned-iterator", lambda: {"items": items})
    return {}


def nested_map(context, inputs):
    return list(context.map(lambda i: list(context.map(str, [i])), [1]))


def parallel_map(context, inputs):
    shared = context.resource("parallel", lambda: Resource(context.input_path.parent))

    def work(i):
        with shared.lock:
            shared.active += 1
            shared.peak = max(shared.peak, shared.active)
        try:
            time.sleep(0.04)
            return i
        finally:
            with shared.lock:
                shared.active -= 1

    values = list(context.map(work, range(8), max_workers=8))
    return {"values": values, "peak": shared.peak}
