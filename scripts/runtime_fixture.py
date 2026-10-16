"""Pure-Python CPU fixture for process-slot throughput and cache reuse measurements."""

import os
import time
import uuid


def calculate(context, inputs):
    cached = context.resource("runtime-fixture", lambda: {"id": uuid.uuid4().hex, "calls": 0})
    cached["calls"] += 1
    started, before = time.monotonic(), time.thread_time()
    value = 0
    for index in range(context.parameters["rounds"]):
        value = (value * 31 + index) % 1000000007
        if index % 16384 == 0:
            context.check_cancelled()
    return {
        "pid": os.getpid(),
        "cache_id": cached["id"],
        "cache_calls": cached["calls"],
        "checksum": value,
        "cpu_seconds": time.thread_time() - before,
        "started": started,
        "finished": time.monotonic(),
    }
