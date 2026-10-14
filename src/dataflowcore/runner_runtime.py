"""Bounded resources owned by one resident, single-task-at-a-time runner."""

import math
import tempfile
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .contracts import Invalid, integer
from .resources import ResourceBudget


class TrackedPool:
    def __init__(self, workers, name):
        self.executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix=name)
        self.lock = threading.Lock()
        self.pending = set()
        self.threads = set()

    def submit(self, fn, *args):
        def invoke():
            with self.lock:
                self.threads.add(threading.current_thread())
            return fn(*args)

        future = self.executor.submit(invoke)
        with self.lock:
            self.pending.add(future)

        def finished(value):
            with self.lock:
                self.pending.discard(value)

        future.add_done_callback(finished)
        return future

    def idle(self):
        with self.lock:
            return all(future.done() for future in self.pending)

    def close(self):
        self.executor.shutdown(wait=False, cancel_futures=True)


class RunnerRuntime:
    def __init__(
        self,
        dag_workers=8,
        map_workers=8,
        io_workers=16,
        io_map_workers=16,
        budget=None,
        owner="local",
    ):
        integer(dag_workers, "runner dag workers", 1, 256)
        integer(map_workers, "runner map workers", 1, 256)
        integer(io_workers, "runner io workers", 1, 256)
        integer(io_map_workers, "runner io map workers", 1, 256)
        self.dag_workers, self.map_workers = dag_workers, map_workers
        self.io_workers, self.io_map_workers = io_workers, io_map_workers
        self.dag = TrackedPool(dag_workers, "dag")
        self.blocks = TrackedPool(map_workers, "blocks")
        self.io = TrackedPool(io_workers, "io-dag")
        self.io_blocks = TrackedPool(io_map_workers, "io-blocks")
        self.temp = None
        if budget is None:
            self.temp = tempfile.TemporaryDirectory(prefix="dataflow-resources-")
            budget = ResourceBudget(Path(self.temp.name) / "budget.sqlite", 1024, 1048576)
        self.budget, self.owner = budget, owner
        self.budget.configure_owner(owner, dag_workers, io_workers)
        self.local = threading.local()
        self.lock = threading.RLock()
        self.resources = {}
        self.step_maps = {}
        self.open_maps = {}
        self.completed_tasks = 0
        self.baseline_threads = set(threading.enumerate())

    def resource(self, namespace, key, factory):
        if not isinstance(key, str) or not 1 <= len(key) <= 256:
            raise Invalid("resource key must be a string of 1..256 characters")
        identity = (namespace, key)
        with self.lock:
            if identity not in self.resources:
                if len(self.resources) >= 128:
                    raise Invalid("runner resource limit exceeded (128); use stable resource keys")
                self.resources[identity] = factory()
            return self.resources[identity]

    def map(self, invoke, items, workers, pending, workload="cpu", resources=None, identity=""):
        if getattr(self.local, "mapping", False):
            raise Invalid("nested context.map in a map worker is not supported")
        io = workload == "io"
        pool = self.io_blocks if io else self.blocks
        limit = self.io_map_workers if io else self.map_workers
        if not io and resources is not None:
            limit = min(limit, max(1, math.ceil(resources["cpu"])))
        window = min(workers, limit, pending)
        source, queued = iter(items), deque()
        failed = False
        with self.lock:
            self.open_maps[identity] = self.open_maps.get(identity, 0) + 1

        def work(item):
            self.local.mapping = True
            try:
                return invoke(item)
            finally:
                self.local.mapping = False

        def submit(item):
            future = pool.submit(work, item)
            with self.lock:
                self.step_maps.setdefault(identity, set()).add(future)

            def finished(value):
                with self.lock:
                    self.step_maps.get(identity, set()).discard(value)

            future.add_done_callback(finished)
            return future

        try:
            for _ in range(window):
                try:
                    queued.append(submit(next(source)))
                except StopIteration:
                    break
            while queued:
                yield queued.popleft().result()
                try:
                    queued.append(submit(next(source)))
                except StopIteration:
                    pass
        except BaseException as exc:
            failed = not isinstance(exc, GeneratorExit)
            raise
        finally:
            try:
                for future in queued:
                    future.cancel()
                if not failed:
                    # A closed iterator must not leave writes racing with the next step.
                    for future in queued:
                        if not future.cancelled():
                            future.result()
            finally:
                with self.lock:
                    self.open_maps[identity] -= 1
                    if not self.open_maps[identity]:
                        self.open_maps.pop(identity)

    def idle(self):
        return all(pool.idle() for pool in (self.dag, self.blocks, self.io, self.io_blocks))

    def maps_idle(self, identity):
        with self.lock:
            idle = not self.open_maps.get(identity) and all(
                future.done() for future in self.step_maps.get(identity, ())
            )
            if idle:
                self.step_maps.pop(identity, None)
            return idle

    def safe_to_reuse(self):
        owned = (
            self.baseline_threads
            | self.dag.threads
            | self.blocks.threads
            | self.io.threads
            | self.io_blocks.threads
        )
        return self.idle() and set(threading.enumerate()) <= owned

    def close(self):
        self.dag.close()
        self.blocks.close()
        self.io.close()
        self.io_blocks.close()
        closed = set()
        for value in self.resources.values():
            if id(value) in closed:
                continue
            closed.add(id(value))
            close = getattr(value, "close", None)
            if callable(close):
                close()
        self.budget.close()
        if self.temp is not None:
            self.temp.cleanup()
