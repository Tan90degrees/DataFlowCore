"""Installed CPU/IO probes exercising real process-wide admission and pools."""

import json
import os
import threading
import time

from dataflowcore.runtime import sleep_cooperatively


def probe(context, inputs):
    started = time.time()
    duration = context.parameters.get("seconds", 0.2)
    if context.workload == "cpu":
        end, value = time.monotonic() + duration, 0
        while time.monotonic() < end:
            context.check_cancelled()
            for i in range(1000):
                value = (value * 31 + i) % 1000000007
    else:
        sleep_cooperatively(context, duration)
    return {
        "pid": os.getpid(),
        "thread": threading.current_thread().name,
        "started": started,
        "finished": time.time(),
        "workload": context.workload,
        "resources": context.resources,
        "inputs": sorted(inputs),
    }


def mapped(context, inputs):
    lock = threading.Lock()
    active, peak = 0, 0

    def work(item):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            sleep_cooperatively(context, 0.05)
            return threading.current_thread().name
        finally:
            with lock:
                active -= 1

    threads = list(context.map(work, range(12), max_workers=12))
    return {"peak": peak, "threads": sorted(set(threads)), "pid": os.getpid()}


def crash(context, inputs):
    marker = context.input_path.parent / "resource-crashed"
    if not marker.exists():
        marker.write_text(json.dumps({"pid": os.getpid(), "step": context.step_id}))
        os._exit(17)
    return probe(context, inputs)
