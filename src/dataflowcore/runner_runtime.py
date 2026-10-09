"""Bounded resources owned by one resident, single-task-at-a-time runner."""

import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from .contracts import Invalid, integer


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
    def __init__(self, dag_workers=8, map_workers=8):
        integer(dag_workers, "runner dag workers", 1, 256)
        integer(map_workers, "runner map workers", 1, 256)
        self.dag_workers, self.map_workers = dag_workers, map_workers
        self.dag = TrackedPool(dag_workers, "dag")
        self.blocks = TrackedPool(map_workers, "blocks")
        self.local = threading.local()
        self.lock = threading.RLock()
        self.resources = {}
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

    def map(self, invoke, items, workers, pending):
        if getattr(self.local, "mapping", False):
            raise Invalid("nested context.map in a map worker is not supported")
        window = min(workers, self.map_workers, pending)
        source, queued = iter(items), deque()
        failed = False

        def work(item):
            self.local.mapping = True
            try:
                return invoke(item)
            finally:
                self.local.mapping = False

        try:
            for _ in range(window):
                try:
                    queued.append(self.blocks.submit(work, next(source)))
                except StopIteration:
                    break
            while queued:
                yield queued.popleft().result()
                try:
                    queued.append(self.blocks.submit(work, next(source)))
                except StopIteration:
                    pass
        except BaseException as exc:
            failed = not isinstance(exc, GeneratorExit)
            raise
        finally:
            for future in queued:
                future.cancel()
            if not failed:
                # A closed iterator must not leave writes racing with the next step.
                for future in queued:
                    if not future.cancelled():
                        future.result()

    def idle(self):
        return self.dag.idle() and self.blocks.idle()

    def safe_to_reuse(self):
        owned = self.baseline_threads | self.dag.threads | self.blocks.threads
        return self.idle() and set(threading.enumerate()) <= owned

    def close(self):
        self.dag.close()
        self.blocks.close()
        closed = set()
        for value in self.resources.values():
            if id(value) in closed:
                continue
            closed.add(id(value))
            close = getattr(value, "close", None)
            if callable(close):
                close()
