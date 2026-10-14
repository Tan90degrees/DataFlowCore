"""Atomic, executor-wide operator admission across resident runner processes.

This database is local ephemeral coordination, not task persistence. Only the
supervisor may reclaim an owner, after its process group has been terminated.
"""

import hashlib
import os
import select
import sqlite3
import tempfile
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from time import monotonic

from .contracts import Invalid, cpu_units, integer, number

WAIT_REASONS = ("cpu", "memory", "io_concurrency", "cpu_pool", "io_pool", "aging")


def _eligible(request, owners):
    owner, kind = owners[request["owner"]], request["workload"]
    return owner[kind + "_count"] < owner[kind + "_limit"]


def _fits(request, free):
    return (
        request["cpu"] <= free[0]
        and request["memory"] <= free[1]
        and (request["workload"] != "io" or free[2] >= 1)
    )


class ResourceBudget:
    def __init__(self, path, cpu=None, memory_mb=None, io_concurrency=32, aging_seconds=2):
        self.path = str(path)
        self.lock = threading.RLock()
        if cpu is not None:
            cpu_units(cpu)
            integer(memory_mb, "memory_mb", 1, 1048576)
            integer(io_concurrency, "io_concurrency", 1, 4096)
            number(aging_seconds, "resource aging seconds", 0, 3600)
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(self.path) as db:
                db.execute("PRAGMA journal_mode=WAL")
                db.executescript("""
                    CREATE TABLE IF NOT EXISTS capacity (
                        id INTEGER PRIMARY KEY CHECK(id=1), cpu INTEGER, memory INTEGER,
                        io_limit INTEGER, peak_cpu INTEGER DEFAULT 0, peak_memory INTEGER DEFAULT 0,
                        peak_io INTEGER DEFAULT 0, aging_seconds REAL NOT NULL);
                    CREATE TABLE IF NOT EXISTS owners (
                        owner TEXT PRIMARY KEY, cpu_limit INTEGER NOT NULL,
                        io_limit INTEGER NOT NULL, peak_cpu INTEGER DEFAULT 0,
                        peak_memory INTEGER DEFAULT 0, notification TEXT);
                    CREATE TABLE IF NOT EXISTS requests (
                        seq INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL,
                        key TEXT NOT NULL, workload TEXT NOT NULL, cpu INTEGER NOT NULL,
                        memory INTEGER NOT NULL, granted INTEGER NOT NULL DEFAULT 0,
                        created REAL NOT NULL, UNIQUE(owner,key));
                    CREATE INDEX IF NOT EXISTS request_queue ON requests(granted,seq);
                """)
                db.execute(
                    "INSERT OR IGNORE INTO capacity(id,cpu,memory,io_limit,aging_seconds) "
                    "VALUES(1,?,?,?,?)",
                    (round(cpu * 1000), memory_mb, io_concurrency, aging_seconds),
                )
            db.close()
        # Keep one connection for this process's lifetime, serialized across its
        # threads. Avoid per-poll WAL teardown/reopen and deferred GC of connections.
        self.connection = sqlite3.connect(
            self.path, timeout=5, check_same_thread=False, isolation_level=None
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA synchronous=NORMAL")
        # Coordination stays authoritative in SQLite. FIFO notifications are
        # hints only: a bounded wait always rechecks after packet loss or process death.
        digest = hashlib.sha256(os.path.abspath(self.path).encode()).hexdigest()[:24]
        self.notify_dir = Path(tempfile.gettempdir()) / f"dfc-budget-{os.getuid()}-{digest}"
        self.notify_dir.mkdir(mode=0o700, exist_ok=True)
        self.notify_path = self.notify_dir / uuid.uuid4().hex
        temporary = self.notify_path.with_name("." + self.notify_path.name)
        os.mkfifo(temporary, mode=0o600)
        self.notification = os.open(temporary, os.O_RDWR | os.O_NONBLOCK)
        temporary.rename(self.notify_path)

    def wake(self):
        try:
            os.write(self.notification, b"1")
        except OSError:
            pass

    def notify(self, paths, include_self=True):
        for value in paths:
            if not value:
                continue
            path = Path(value)
            if not include_self and path == self.notify_path:
                continue
            try:
                fd = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
            except OSError as exc:
                if exc.errno in (2, 6):
                    path.unlink(missing_ok=True)
                continue
            try:
                os.write(fd, b"1")
            except OSError:
                pass
            finally:
                os.close(fd)

    def wait(self, timeout=0.5):
        if select.select([self.notification], [], [], timeout)[0]:
            while True:
                try:
                    os.read(self.notification, 4096)
                except BlockingIOError:
                    break

    @contextmanager
    def transaction(self):
        with self.lock:
            db = self.connection
            try:
                db.execute("BEGIN IMMEDIATE")
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def totals(db):
        return db.execute(
            "SELECT COALESCE(SUM(cpu),0) cpu, COALESCE(SUM(memory),0) memory, "
            "COALESCE(SUM(workload='io'),0) io, COUNT(*) active FROM requests WHERE granted=1"
        ).fetchone()

    @staticmethod
    def owner_usage(db):
        return {
            row["owner"]: dict(row)
            for row in db.execute(
                "SELECT o.owner,o.cpu_limit,o.io_limit,o.notification,COALESCE(SUM(r.cpu),0) cpu, "
                "COALESCE(SUM(r.memory),0) memory,COALESCE(SUM(r.workload='cpu'),0) cpu_count, "
                "COALESCE(SUM(r.workload='io'),0) io_count FROM owners o LEFT JOIN requests r "
                "ON r.owner=o.owner AND r.granted=1 GROUP BY o.owner"
            )
        }

    @staticmethod
    def queue_diagnostics(pending, capacity, owners, used, owner=None):
        free = (
            capacity["cpu"] - used[0],
            capacity["memory"] - used[1],
            capacity["io_limit"] - used[2],
        )

        head = next((row for row in pending if _eligible(row, owners)), None)
        protected = head is not None and monotonic() - head["created"] >= capacity["aging_seconds"]
        protected = protected and not _fits(head, free)
        surplus = tuple(
            max(0, available - reserved)
            for available, reserved in zip(
                free,
                (head["cpu"], head["memory"], head["workload"] == "io") if protected else (0, 0, 0),
                strict=True,
            )
        )
        counts = dict.fromkeys(WAIT_REASONS, 0)
        waiting = {}
        for row in pending:
            if owner is not None and row["owner"] != owner:
                continue
            reasons = []
            if row["cpu"] > free[0]:
                reasons.append("cpu")
            if row["memory"] > free[1]:
                reasons.append("memory")
            if row["workload"] == "io" and free[2] < 1:
                reasons.append("io_concurrency")
            if not _eligible(row, owners):
                reasons.append(row["workload"] + "_pool")
            if (
                protected
                and row is not head
                and _eligible(row, owners)
                and _fits(row, free)
                and not _fits(row, surplus)
            ):
                reasons.append("aging")
            for reason in reasons:
                counts[reason] += 1
            waiting[row["key"] if owner is not None else (row["owner"], row["key"])] = reasons
        return {
            "waiting": waiting,
            "waiting_by_reason": counts,
            "protected_resources": {
                "cpu": head["cpu"] / 1000,
                "memory_mb": head["memory"],
                "io_operators": int(head["workload"] == "io"),
            }
            if protected
            else None,
        }

    def dispatch(self, db, diagnostic_owner=None):
        pending = [
            dict(r) for r in db.execute("SELECT * FROM requests WHERE granted=0 ORDER BY seq")
        ]
        if not pending:
            return (set(), {}) if diagnostic_owner is not None else set()
        capacity, used = db.execute("SELECT * FROM capacity WHERE id=1").fetchone(), self.totals(db)
        cpu, memory, io = used["cpu"], used["memory"], used["io"]
        owners = self.owner_usage(db)
        now = monotonic()
        grants = set()
        changed_owners = set()

        def share(row):
            owner = owners[row["owner"]]
            return max(
                owner["cpu"] / max(1, capacity["cpu"]),
                owner["memory"] / capacity["memory"],
                owner["io_count"] / capacity["io_limit"],
            ), row["seq"]

        while pending:
            runnable = [r for r in pending if _eligible(r, owners)]
            if not runnable:
                break
            free = (capacity["cpu"] - cpu, capacity["memory"] - memory, capacity["io_limit"] - io)
            head = min(runnable, key=lambda r: r["seq"])
            protected = now - head["created"] >= capacity["aging_seconds"]
            if protected and _fits(head, free):
                selected = head
            else:
                if protected:
                    # Backfill only from the surplus beyond the old request's
                    # needs. Already running operators are never preempted.
                    free = (
                        max(0, free[0] - head["cpu"]),
                        max(0, free[1] - head["memory"]),
                        max(0, free[2] - (head["workload"] == "io")),
                    )
                candidates = [r for r in runnable if _fits(r, free)]
                if not candidates:
                    break
                selected = min(candidates, key=share)
            grants.add(owners[selected["owner"]]["notification"])
            db.execute("UPDATE requests SET granted=1 WHERE seq=?", (selected["seq"],))
            owner = owners[selected["owner"]]
            owner["cpu"] += selected["cpu"]
            owner["memory"] += selected["memory"]
            owner[selected["workload"] + "_count"] += 1
            changed_owners.add(selected["owner"])
            cpu += selected["cpu"]
            memory += selected["memory"]
            io += selected["workload"] == "io"
            pending.remove(selected)
        if grants:
            db.executemany(
                "UPDATE owners SET peak_cpu=MAX(peak_cpu,?), peak_memory=MAX(peak_memory,?) "
                "WHERE owner=?",
                [(owners[name]["cpu"], owners[name]["memory"], name) for name in changed_owners],
            )
            db.execute(
                "UPDATE capacity SET peak_cpu=MAX(peak_cpu,?), peak_memory=MAX(peak_memory,?), "
                "peak_io=MAX(peak_io,?) WHERE id=1",
                (cpu, memory, io),
            )
        if diagnostic_owner is not None:
            diagnostic = self.queue_diagnostics(
                pending, capacity, owners, (cpu, memory, io), diagnostic_owner
            )
            return grants, diagnostic["waiting"]
        return grants

    def configure_owner(self, owner, cpu_limit, io_limit):
        integer(cpu_limit, "CPU pool limit", 1, 256)
        integer(io_limit, "IO pool limit", 1, 256)
        with self.transaction() as db:
            db.execute(
                "INSERT OR IGNORE INTO owners(owner,cpu_limit,io_limit,notification) "
                "VALUES(?,?,?,?)",
                (owner, cpu_limit, io_limit, str(self.notify_path)),
            )

    def begin_attempt(self, owner):
        with self.transaction() as db:
            if db.execute("SELECT 1 FROM requests WHERE owner=? LIMIT 1", (owner,)).fetchone():
                raise Invalid("runner has unreclaimed requests from a previous attempt")
            db.execute("UPDATE owners SET peak_cpu=0,peak_memory=0 WHERE owner=?", (owner,))

    def attempt_peak(self, owner):
        with self.lock:
            row = self.connection.execute(
                "SELECT peak_cpu,peak_memory FROM owners WHERE owner=?", (owner,)
            ).fetchone()
            return {"cpu": row[0] / 1000, "memory_mb": row[1]}

    def schedule(self, owner, requests):
        return self._schedule(owner, requests, diagnostics=False)

    def admit(self, owner, requests):
        """Enqueue new requests, then return grants and snapshot-consistent wait reasons."""
        return self._schedule(owner, requests, diagnostics=True)

    def _schedule(self, owner, requests, diagnostics):
        values = []
        for key, workload, resources in requests:
            cpu, memory = round(cpu_units(resources["cpu"]) * 1000), resources["memory_mb"]
            integer(memory, "resources.memory_mb", 1, 1048576)
            if workload not in ("cpu", "io"):
                raise Invalid("invalid workload")
            values.append((owner, key, workload, cpu, memory, monotonic()))
        with self.transaction() as db:
            capacity = db.execute("SELECT * FROM capacity WHERE id=1").fetchone()
            if any(v[3] > capacity["cpu"] or v[4] > capacity["memory"] for v in values):
                raise Invalid("operator resources exceed executor capacity")
            if values:
                db.execute(
                    "INSERT OR IGNORE INTO owners(owner,cpu_limit,io_limit,notification) "
                    "VALUES(?,256,256,?)",
                    (owner, str(self.notify_path)),
                )
                db.executemany(
                    "INSERT OR IGNORE INTO requests(owner,key,workload,cpu,memory,created) "
                    "VALUES(?,?,?,?,?,?)",
                    values,
                )
                existing = {
                    r["key"]: r
                    for r in db.execute("SELECT * FROM requests WHERE owner=?", (owner,))
                }
                if any(
                    (existing[v[1]]["workload"], existing[v[1]]["cpu"], existing[v[1]]["memory"])
                    != v[2:5]
                    for v in values
                ):
                    raise Invalid("resource request identity cannot change")
            dispatched = self.dispatch(db, diagnostic_owner=owner if diagnostics else None)
            waiting = {}
            if diagnostics:
                dispatched, waiting = dispatched
            granted = {
                r[0]
                for r in db.execute(
                    "SELECT key FROM requests WHERE owner=? AND granted=1", (owner,)
                )
            }
        if dispatched:
            self.notify(dispatched, include_self=False)
        return {"granted": granted, "waiting": waiting} if diagnostics else granted

    def acquire(self, owner, key, workload, resources):
        return key in self.schedule(owner, [(key, workload, resources)])

    def release(self, owner, key=None):
        removed_notification = None
        with self.transaction() as db:
            if key is None:
                row = db.execute(
                    "SELECT notification FROM owners WHERE owner=?", (owner,)
                ).fetchone()
                removed_notification = row[0] if row else None
                db.execute("DELETE FROM requests WHERE owner=?", (owner,))
                db.execute("DELETE FROM owners WHERE owner=?", (owner,))
            else:
                db.execute("DELETE FROM requests WHERE owner=? AND key=?", (owner, key))
            granted = self.dispatch(db)
        self.notify(granted)
        if removed_notification and removed_notification != str(self.notify_path):
            Path(removed_notification).unlink(missing_ok=True)

    def release_many(self, owner, keys):
        """Release a completion batch and dispatch once, without reclaiming the owner."""
        with self.transaction() as db:
            db.executemany(
                "DELETE FROM requests WHERE owner=? AND key=?", [(owner, key) for key in keys]
            )
            granted = self.dispatch(db)
        self.notify(granted)

    def snapshot(self):
        with self.lock:
            db = self.connection
            try:
                db.execute("BEGIN")
                capacity = db.execute("SELECT * FROM capacity WHERE id=1").fetchone()
                used = self.totals(db)
                pending = [
                    dict(row)
                    for row in db.execute("SELECT * FROM requests WHERE granted=0 ORDER BY seq")
                ]
                oldest = min((row["created"] for row in pending), default=None)
                diagnostic = self.queue_diagnostics(
                    pending,
                    capacity,
                    self.owner_usage(db),
                    (used["cpu"], used["memory"], used["io"]),
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return {
            "cpu": used["cpu"] / 1000,
            "memory_mb": used["memory"],
            "active_operators": used["active"],
            "waiting_operators": len(pending),
            "waiting_by_reason": diagnostic["waiting_by_reason"],
            "protected_resources": diagnostic["protected_resources"],
            "io_operators": used["io"],
            "cpu_capacity": capacity["cpu"] / 1000,
            "memory_capacity_mb": capacity["memory"],
            "io_concurrency": capacity["io_limit"],
            "peak_cpu": capacity["peak_cpu"] / 1000,
            "peak_memory_mb": capacity["peak_memory"],
            "peak_io_operators": capacity["peak_io"],
            "policy": "fair_backfill_with_aging",
            "aging_seconds": capacity["aging_seconds"],
            "oldest_wait_seconds": max(0, monotonic() - oldest) if oldest is not None else 0,
        }

    def close(self):
        with self.lock:
            self.connection.close()
            os.close(self.notification)
            self.notify_path.unlink(missing_ok=True)
            try:
                self.notify_dir.rmdir()
            except OSError:
                pass
