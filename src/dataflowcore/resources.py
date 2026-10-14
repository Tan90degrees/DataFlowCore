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

    def dispatch(self, db):
        pending = [
            dict(r) for r in db.execute("SELECT * FROM requests WHERE granted=0 ORDER BY seq")
        ]
        if not pending:
            return set()
        capacity, used = db.execute("SELECT * FROM capacity WHERE id=1").fetchone(), self.totals(db)
        cpu, memory, io = used["cpu"], used["memory"], used["io"]
        owners = {}
        for row in db.execute(
            "SELECT o.owner,o.cpu_limit,o.io_limit,o.notification,COALESCE(SUM(r.cpu),0) cpu, "
            "COALESCE(SUM(r.memory),0) memory,COALESCE(SUM(r.workload='cpu'),0) cpu_count, "
            "COALESCE(SUM(r.workload='io'),0) io_count FROM owners o LEFT JOIN requests r "
            "ON r.owner=o.owner AND r.granted=1 GROUP BY o.owner"
        ):
            owners[row["owner"]] = dict(row)
        now = monotonic()
        grants = set()

        def eligible(row):
            owner = owners[row["owner"]]
            kind = row["workload"]
            return owner[kind + "_count"] < owner[kind + "_limit"]

        def fits(row, free):
            return (
                row["cpu"] <= free[0]
                and row["memory"] <= free[1]
                and (row["workload"] != "io" or free[2] >= 1)
            )

        def share(row):
            owner = owners[row["owner"]]
            return max(
                owner["cpu"] / max(1, capacity["cpu"]),
                owner["memory"] / capacity["memory"],
                owner["io_count"] / capacity["io_limit"],
            ), row["seq"]

        while pending:
            runnable = [r for r in pending if eligible(r)]
            if not runnable:
                break
            free = (capacity["cpu"] - cpu, capacity["memory"] - memory, capacity["io_limit"] - io)
            head = min(runnable, key=lambda r: r["seq"])
            protected = now - head["created"] >= capacity["aging_seconds"]
            if protected and fits(head, free):
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
                candidates = [r for r in runnable if fits(r, free)]
                if not candidates:
                    break
                selected = min(candidates, key=share)
            grants.add(owners[selected["owner"]]["notification"])
            db.execute("UPDATE requests SET granted=1 WHERE seq=?", (selected["seq"],))
            owner = owners[selected["owner"]]
            owner["cpu"] += selected["cpu"]
            owner["memory"] += selected["memory"]
            owner[selected["workload"] + "_count"] += 1
            db.execute(
                "UPDATE owners SET peak_cpu=MAX(peak_cpu,?), peak_memory=MAX(peak_memory,?) "
                "WHERE owner=?",
                (owner["cpu"], owner["memory"], selected["owner"]),
            )
            cpu += selected["cpu"]
            memory += selected["memory"]
            io += selected["workload"] == "io"
            pending.remove(selected)
        if grants:
            db.execute(
                "UPDATE capacity SET peak_cpu=MAX(peak_cpu,?), peak_memory=MAX(peak_memory,?), "
                "peak_io=MAX(peak_io,?) WHERE id=1",
                (cpu, memory, io),
            )
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
                r["key"]: r for r in db.execute("SELECT * FROM requests WHERE owner=?", (owner,))
            }
            if any(
                (existing[v[1]]["workload"], existing[v[1]]["cpu"], existing[v[1]]["memory"])
                != v[2:5]
                for v in values
            ):
                raise Invalid("resource request identity cannot change")
            dispatched = self.dispatch(db)
            granted = {
                r[0]
                for r in db.execute(
                    "SELECT key FROM requests WHERE owner=? AND granted=1", (owner,)
                )
            }
        if dispatched:
            self.notify(dispatched, include_self=False)
        return granted

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

    def snapshot(self):
        with self.lock:
            db = self.connection
            try:
                db.execute("BEGIN")
                capacity = db.execute("SELECT * FROM capacity WHERE id=1").fetchone()
                used = self.totals(db)
                waiting = db.execute("SELECT COUNT(*) FROM requests WHERE granted=0").fetchone()[0]
                oldest = db.execute("SELECT MIN(created) FROM requests WHERE granted=0").fetchone()[
                    0
                ]
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return {
            "cpu": used["cpu"] / 1000,
            "memory_mb": used["memory"],
            "active_operators": used["active"],
            "waiting_operators": waiting,
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
