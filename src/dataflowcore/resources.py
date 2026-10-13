"""Atomic, executor-wide operator admission across resident runner processes.

This database is local ephemeral coordination, not task persistence. Only the
supervisor may reclaim an owner, after its process group has been terminated.
"""

import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from .contracts import Invalid, cpu_units, integer


class ResourceBudget:
    def __init__(self, path, cpu=None, memory_mb=None, io_concurrency=32):
        self.path = str(path)
        self.lock = threading.RLock()
        if cpu is not None:
            cpu_units(cpu)
            integer(memory_mb, "memory_mb", 1, 1048576)
            integer(io_concurrency, "io_concurrency", 1, 4096)
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(self.path) as db:
                db.execute("PRAGMA journal_mode=WAL")
                db.executescript("""
                    CREATE TABLE IF NOT EXISTS capacity (
                        id INTEGER PRIMARY KEY CHECK(id=1), cpu INTEGER, memory INTEGER,
                        io_limit INTEGER, peak_cpu INTEGER DEFAULT 0, peak_memory INTEGER DEFAULT 0,
                        peak_io INTEGER DEFAULT 0);
                    CREATE TABLE IF NOT EXISTS requests (
                        seq INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL,
                        key TEXT NOT NULL, workload TEXT NOT NULL, cpu INTEGER NOT NULL,
                        memory INTEGER NOT NULL, granted INTEGER NOT NULL DEFAULT 0,
                        created REAL NOT NULL, UNIQUE(owner,key));
                    CREATE INDEX IF NOT EXISTS request_queue ON requests(granted,seq);
                """)
                db.execute(
                    "INSERT OR IGNORE INTO capacity(id,cpu,memory,io_limit) VALUES(1,?,?,?)",
                    (round(cpu * 1000), memory_mb, io_concurrency),
                )
            db.close()
        # Keep one connection for this process's lifetime, serialized across its
        # threads. Avoid per-poll WAL teardown/reopen and deferred GC of connections.
        self.connection = sqlite3.connect(
            self.path, timeout=5, check_same_thread=False, isolation_level=None
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA synchronous=NORMAL")

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
        capacity, used = db.execute("SELECT * FROM capacity WHERE id=1").fetchone(), self.totals(db)
        cpu, memory, io = used["cpu"], used["memory"], used["io"]
        blocked = set()
        for row in db.execute("SELECT * FROM requests WHERE granted=0 ORDER BY seq").fetchall():
            # FIFO within each workload prevents large CPU nodes being starved
            # by smaller ones. IO may pass a CPU head blocked on CPU capacity.
            if row["memory"] + memory > capacity["memory"]:
                break  # Shared-memory fairness applies across both classes.
            kind = row["workload"]
            if kind in blocked:
                continue
            if row["cpu"] + cpu > capacity["cpu"] or (kind == "io" and io >= capacity["io_limit"]):
                blocked.add(kind)
                continue
            db.execute("UPDATE requests SET granted=1 WHERE seq=?", (row["seq"],))
            cpu += row["cpu"]
            memory += row["memory"]
            io += kind == "io"
        db.execute(
            "UPDATE capacity SET peak_cpu=MAX(peak_cpu,?), peak_memory=MAX(peak_memory,?), "
            "peak_io=MAX(peak_io,?) WHERE id=1",
            (cpu, memory, io),
        )

    def acquire(self, owner, key, workload, resources):
        cpu, memory = round(cpu_units(resources["cpu"]) * 1000), resources["memory_mb"]
        integer(memory, "resources.memory_mb", 1, 1048576)
        if workload not in ("cpu", "io"):
            raise Invalid("invalid workload")
        with self.transaction() as db:
            capacity = db.execute("SELECT * FROM capacity WHERE id=1").fetchone()
            if cpu > capacity["cpu"] or memory > capacity["memory"]:
                raise Invalid("operator resources exceed executor capacity")
            db.execute(
                "INSERT OR IGNORE INTO requests(owner,key,workload,cpu,memory,created) "
                "VALUES(?,?,?,?,?,?)",
                (owner, key, workload, cpu, memory, time.time()),
            )
            row = db.execute(
                "SELECT * FROM requests WHERE owner=? AND key=?", (owner, key)
            ).fetchone()
            if (row["workload"], row["cpu"], row["memory"]) != (workload, cpu, memory):
                raise Invalid("resource request identity cannot change")
            self.dispatch(db)
            return bool(
                db.execute(
                    "SELECT granted FROM requests WHERE owner=? AND key=?", (owner, key)
                ).fetchone()[0]
            )

    def release(self, owner, key=None):
        with self.transaction() as db:
            if key is None:
                db.execute("DELETE FROM requests WHERE owner=?", (owner,))
            else:
                db.execute("DELETE FROM requests WHERE owner=? AND key=?", (owner, key))
            self.dispatch(db)

    def snapshot(self):
        with self.lock:
            db = self.connection
            try:
                db.execute("BEGIN")
                capacity = db.execute("SELECT * FROM capacity WHERE id=1").fetchone()
                used = self.totals(db)
                waiting = db.execute("SELECT COUNT(*) FROM requests WHERE granted=0").fetchone()[0]
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
        }

    def close(self):
        with self.lock:
            self.connection.close()
