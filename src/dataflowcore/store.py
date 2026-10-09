"""Durable, transactionally fenced orchestration.

PostgreSQL is the production store; SQLite is an explicitly single-host development store.
All writers take one database transaction lock. This intentionally simple single-controller
design makes cancellation, claims, expiry and completion serializable across API threads.
No locks/transactions are held while business code or network requests run.
"""

import json
import sqlite3
import uuid
from contextlib import contextmanager

from .contracts import (
    ACTIVE,
    TERMINAL,
    Conflict,
    Invalid,
    Missing,
    TaskSpec,
    canonical,
    fingerprint,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS tasks (
 id TEXT PRIMARY KEY, request_key TEXT UNIQUE NOT NULL, request_hash TEXT NOT NULL,
 spec TEXT NOT NULL, state TEXT NOT NULL, attempt_count INTEGER NOT NULL,
 current_attempt TEXT, available_at DOUBLE PRECISION NOT NULL,
 created_at DOUBLE PRECISION NOT NULL, updated_at DOUBLE PRECISION NOT NULL,
 result TEXT, error TEXT
);
CREATE INDEX IF NOT EXISTS tasks_queue ON tasks(state, available_at, created_at);
CREATE TABLE IF NOT EXISTS workers (
 session_id TEXT PRIMARY KEY, name TEXT NOT NULL, pool TEXT NOT NULL,
 runtime_version TEXT NOT NULL, slots INTEGER NOT NULL, cpu INTEGER NOT NULL,
 memory_mb INTEGER NOT NULL, last_seen DOUBLE PRECISION NOT NULL,
 draining INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
 id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id),
 number INTEGER NOT NULL, worker_session TEXT NOT NULL REFERENCES workers(session_id),
 token TEXT NOT NULL, claim_id TEXT UNIQUE NOT NULL, state TEXT NOT NULL,
 started_at DOUBLE PRECISION NOT NULL, finished_at DOUBLE PRECISION,
 lease_until DOUBLE PRECISION NOT NULL, deadline DOUBLE PRECISION NOT NULL,
 progress TEXT NOT NULL, result TEXT, error TEXT, completion_hash TEXT,
 UNIQUE(task_id, number)
);
CREATE INDEX IF NOT EXISTS attempts_worker ON attempts(worker_session, state);
CREATE TABLE IF NOT EXISTS events (
 id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id), attempt_id TEXT,
 kind TEXT NOT NULL, payload TEXT NOT NULL, created_at DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS events_task ON events(task_id, created_at);
"""


def uid() -> str:
    return str(uuid.uuid4())


class Session:
    def __init__(self, connection, postgres):
        self.connection = connection
        self.postgres = postgres

    def execute(self, sql, values=()):
        return self.connection.execute(sql.replace("?", "%s") if self.postgres else sql, values)

    def one(self, sql, values=()):
        row = self.execute(sql, values).fetchone()
        return dict(row) if row is not None else None

    def all(self, sql, values=()):
        return [dict(row) for row in self.execute(sql, values).fetchall()]

    def now(self):
        sql = (
            "SELECT EXTRACT(EPOCH FROM clock_timestamp()) AS t"
            if self.postgres
            else ("SELECT (julianday('now') - 2440587.5) * 86400.0 AS t")
        )
        return float(self.one(sql)["t"])


class Store:
    def __init__(self, dsn: str, lease_seconds: float = 30):
        if lease_seconds <= 0:
            raise Invalid("lease_seconds must be positive")
        self.dsn = dsn
        self.postgres = dsn.startswith(("postgresql://", "postgres://"))
        if not self.postgres and not dsn.startswith("sqlite:///"):
            raise Invalid("use postgresql:// or sqlite:///path")
        self.lease_seconds = lease_seconds

    @contextmanager
    def transaction(self, write=True):
        if self.postgres:
            import psycopg
            from psycopg.rows import dict_row

            connection = psycopg.connect(self.dsn, row_factory=dict_row, connect_timeout=5)
        else:
            connection = sqlite3.connect(self.dsn.removeprefix("sqlite:///"), timeout=15)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 15000")
        try:
            session = Session(connection, self.postgres)
            if self.postgres and write:
                session.execute("SELECT pg_advisory_xact_lock(441002, 1)")
            elif not self.postgres:
                connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield session
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def migrate(self):
        with self.transaction() as db:
            for sql in SCHEMA.split(";"):
                if sql.strip():
                    db.execute(sql)
            versions = db.all("SELECT version FROM schema_version")
            if not versions:
                db.execute("INSERT INTO schema_version(version) VALUES (1)")
            elif {v["version"] for v in versions} != {1}:
                raise Invalid("unsupported database schema version")

    @staticmethod
    def event(db, task_id, attempt_id, kind, payload, now):
        db.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)",
            (uid(), task_id, attempt_id, kind, canonical(payload), now),
        )

    def submit(self, spec: TaskSpec, request_key: str):
        encoded = canonical(spec.json())
        hashed = fingerprint(spec.json())
        with self.transaction() as db:
            existing = db.one("SELECT * FROM tasks WHERE request_key = ?", (request_key,))
            if existing:
                if existing["request_hash"] != hashed:
                    raise Conflict("idempotency key reused with a different specification")
                return self.decode_task(existing)
            now, tid = db.now(), uid()
            db.execute(
                """INSERT INTO tasks
                (id, request_key, request_hash, spec, state, attempt_count, available_at,
                 created_at, updated_at) VALUES (?, ?, ?, ?, 'QUEUED', 0, ?, ?, ?)""",
                (tid, request_key, hashed, encoded, now, now, now),
            )
            self.event(db, tid, None, "submitted", {}, now)
            return self.decode_task(db.one("SELECT * FROM tasks WHERE id = ?", (tid,)))

    def register(self, session_id, name, pool, runtime_version, slots, cpu, memory_mb):
        with self.transaction() as db:
            old = db.one("SELECT * FROM workers WHERE session_id = ?", (session_id,))
            values = (name, pool, runtime_version, slots, cpu, memory_mb)
            if old:
                if (
                    tuple(
                        old[k]
                        for k in ("name", "pool", "runtime_version", "slots", "cpu", "memory_mb")
                    )
                    != values
                ):
                    raise Conflict("worker session is immutable")
            else:
                db.execute(
                    "INSERT INTO workers VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)",
                    (session_id, *values, db.now()),
                )
        return {"session_id": session_id, "lease_seconds": self.lease_seconds}

    def heartbeat(self, session_id, draining=False):
        with self.transaction() as db:
            self.worker(db, session_id)
            db.execute(
                "UPDATE workers SET last_seen = ?, draining = ? WHERE session_id = ?",
                (db.now(), int(draining), session_id),
            )
        return {"ok": True}

    @staticmethod
    def worker(db, session_id):
        row = db.one("SELECT * FROM workers WHERE session_id = ?", (session_id,))
        if row is None:
            raise Missing("unknown worker session")
        return row

    def claim(self, session_id, claim_id):
        with self.transaction() as db:
            now = db.now()
            self.reap_locked(db, now)
            worker = self.worker(db, session_id)
            old = db.one("SELECT * FROM attempts WHERE claim_id = ?", (claim_id,))
            if old:
                if old["worker_session"] != session_id:
                    raise Conflict("claim id belongs to another worker")
                self.valid(db, old["task_id"], old["id"], session_id, old["token"], now)
                return self.assignment(db, old)
            if worker["draining"] or now - worker["last_seen"] > self.lease_seconds:
                return None
            running = db.all(
                "SELECT task_id FROM attempts WHERE worker_session = ? "
                "AND state IN ('RUNNING', 'STOPPING')",
                (session_id,),
            )
            if len(running) >= worker["slots"]:
                return None
            used = [
                json.loads(db.one("SELECT spec FROM tasks WHERE id = ?", (r["task_id"],))["spec"])
                for r in running
            ]
            cpu_left = worker["cpu"] - sum(s["cpu"] for s in used)
            mem_left = worker["memory_mb"] - sum(s["memory_mb"] for s in used)
            # Bounded scan: drain incompatible tasks or use separate pools to avoid starvation.
            candidates = db.all(
                "SELECT * FROM tasks WHERE state = 'QUEUED' AND available_at <= ? "
                "ORDER BY created_at, id LIMIT 1000",
                (now,),
            )
            for task in candidates:
                spec = json.loads(task["spec"])
                if (
                    spec["pool"] != worker["pool"]
                    or spec["runtime_version"] != worker["runtime_version"]
                    or spec["cpu"] > cpu_left
                    or spec["memory_mb"] > mem_left
                ):
                    continue
                aid, token = uid(), uid()
                number = task["attempt_count"] + 1
                db.execute(
                    """INSERT INTO attempts
                    (id, task_id, number, worker_session, token, claim_id, state, started_at,
                     lease_until, deadline, progress)
                    VALUES (?, ?, ?, ?, ?, ?, 'RUNNING', ?, ?, ?, '{}')""",
                    (
                        aid,
                        task["id"],
                        number,
                        session_id,
                        token,
                        claim_id,
                        now,
                        now + self.lease_seconds,
                        now + spec["timeout"],
                    ),
                )
                db.execute(
                    "UPDATE tasks SET state = 'RUNNING', current_attempt = ?, "
                    "attempt_count = ?, updated_at = ?, error = NULL WHERE id = ?",
                    (aid, number, now, task["id"]),
                )
                self.event(db, task["id"], aid, "assigned", {"worker_session": session_id}, now)
                return self.assignment(db, db.one("SELECT * FROM attempts WHERE id = ?", (aid,)))
            return None

    def assignment(self, db, attempt):
        task = db.one("SELECT * FROM tasks WHERE id = ?", (attempt["task_id"],))
        return {
            "task_id": task["id"],
            "attempt_id": attempt["id"],
            "token": attempt["token"],
            "number": attempt["number"],
            "spec": json.loads(task["spec"]),
            "lease_seconds": max(0.0, min(attempt["lease_until"], attempt["deadline"]) - db.now()),
        }

    @staticmethod
    def valid(db, tid, aid, session_id, token, now):
        task = db.one("SELECT * FROM tasks WHERE id = ?", (tid,))
        attempt = db.one("SELECT * FROM attempts WHERE id = ?", (aid,))
        if task is None or attempt is None:
            raise Missing("unknown task or attempt")
        if (
            task["current_attempt"] != aid
            or attempt["task_id"] != tid
            or attempt["worker_session"] != session_id
            or attempt["token"] != token
            or attempt["state"] not in ACTIVE
            or task["state"] not in ACTIVE
            or attempt["lease_until"] <= now
            or attempt["deadline"] <= now
        ):
            raise Conflict("execution lease is no longer valid")
        return task, attempt

    def renew(self, tid, aid, session_id, token, progress=None):
        with self.transaction() as db:
            now = db.now()
            task, attempt = self.valid(db, tid, aid, session_id, token, now)
            if progress is None:
                encoded = attempt["progress"]
            else:
                encoded = canonical(progress)
                if not isinstance(progress, dict) or len(encoded) > 262144:
                    raise Invalid("invalid progress payload")
            until = min(now + self.lease_seconds, attempt["deadline"])
            db.execute(
                "UPDATE attempts SET lease_until = ?, progress = ? WHERE id = ?",
                (until, encoded, aid),
            )
            return {"cancel": task["state"] == "STOPPING", "lease_seconds": max(0.0, until - now)}

    def complete(self, tid, aid, session_id, token, state, result=None, error=None, retryable=True):
        if type(retryable) is not bool or (error is not None and not isinstance(error, str)):
            raise Invalid("retryable must be boolean and error must be text")
        if state not in TERMINAL:
            raise Invalid("invalid completion state")
        payload = {"state": state, "result": result, "error": error, "retryable": retryable}
        if len(canonical(payload)) > 1_000_000:
            raise Invalid("completion too large")
        hashed = fingerprint(payload)
        with self.transaction() as db:
            old = db.one("SELECT * FROM attempts WHERE id = ?", (aid,))
            if old and old["completion_hash"]:
                if (
                    old["task_id"] != tid
                    or old["token"] != token
                    or old["worker_session"] != session_id
                    or old["completion_hash"] != hashed
                ):
                    raise Conflict("conflicting completion")
                return {"accepted": True, "state": old["state"]}
            now = db.now()
            task, _ = self.valid(db, tid, aid, session_id, token, now)
            if task["state"] == "STOPPING":
                state, result = "CANCELLED", None
            elif state == "CANCELLED":
                raise Conflict("task was not cancelled")
            db.execute(
                "UPDATE attempts SET state = ?, finished_at = ?, result = ?, error = ?, "
                "completion_hash = ? WHERE id = ?",
                (state, now, canonical(result) if result is not None else None, error, hashed, aid),
            )
            self.finish_task(db, task, state, result, error, retryable, now)
            self.event(db, tid, aid, "completed", {"state": state, "error": error}, now)
            return {"accepted": True, "state": state}

    def finish_task(self, db, task, state, result, error, retryable, now):
        spec = json.loads(task["spec"])
        delay = 0
        if state == "FAILED" and retryable and task["attempt_count"] < spec["max_attempts"]:
            state = "QUEUED"
            delay = min(spec["retry_delay"] * 2 ** (task["attempt_count"] - 1), 3600)
        db.execute(
            "UPDATE tasks SET state = ?, available_at = ?, updated_at = ?, "
            "result = ?, error = ? WHERE id = ?",
            (
                state,
                now + delay,
                now,
                canonical(result) if state == "SUCCEEDED" else None,
                error,
                task["id"],
            ),
        )

    def reap_locked(self, db, now):
        expired = db.all(
            "SELECT * FROM attempts WHERE state IN ('RUNNING', 'STOPPING') "
            "AND (lease_until <= ? OR deadline <= ?)",
            (now, now),
        )
        for attempt in expired:
            task = db.one("SELECT * FROM tasks WHERE id = ?", (attempt["task_id"],))
            if task["current_attempt"] != attempt["id"]:
                continue
            cancelled = task["state"] == "STOPPING"
            state = "CANCELLED" if cancelled else "LOST"
            reason = "deadline exceeded" if attempt["deadline"] <= now else "lease expired"
            db.execute(
                "UPDATE attempts SET state = ?, finished_at = ?, error = ? WHERE id = ?",
                (state, now, reason, attempt["id"]),
            )
            self.finish_task(
                db, task, "CANCELLED" if cancelled else "FAILED", None, reason, True, now
            )
            self.event(db, task["id"], attempt["id"], "expired", {"reason": reason}, now)
        return len(expired)

    def reap(self):
        with self.transaction() as db:
            return self.reap_locked(db, db.now())

    def cancel(self, tid):
        with self.transaction() as db:
            now = db.now()
            task = db.one("SELECT * FROM tasks WHERE id = ?", (tid,))
            if task is None:
                raise Missing("unknown task")
            if task["state"] not in TERMINAL and task["state"] != "STOPPING":
                state = "CANCELLED" if task["state"] == "QUEUED" else "STOPPING"
                db.execute(
                    "UPDATE tasks SET state = ?, updated_at = ? WHERE id = ?", (state, now, tid)
                )
                if state == "STOPPING":
                    db.execute(
                        "UPDATE attempts SET state = 'STOPPING' WHERE id = ?",
                        (task["current_attempt"],),
                    )
                self.event(db, tid, task["current_attempt"], "cancel_requested", {}, now)
            return self.decode_task(db.one("SELECT * FROM tasks WHERE id = ?", (tid,)))

    def retry(self, tid, request_key):
        # A new task identity retains its link to the previous failed/cancelled task.
        old = self.get(tid)
        if old["state"] not in ("FAILED", "CANCELLED"):
            raise Conflict("only failed or cancelled tasks can be manually retried")
        spec = TaskSpec.parse(old["spec"])
        return self.submit(spec, f"retry:{tid}:{request_key}")

    @staticmethod
    def decode_task(row):
        return {
            k: json.loads(v) if k in ("spec", "result") and v is not None else v
            for k, v in row.items()
            if k not in ("request_hash",)
        }

    def get(self, tid):
        with self.transaction(False) as db:
            row = db.one("SELECT * FROM tasks WHERE id = ?", (tid,))
            if row is None:
                raise Missing("unknown task")
            result = self.decode_task(row)
            result["attempts"] = [
                {
                    k: json.loads(v) if k in ("progress", "result") and v is not None else v
                    for k, v in a.items()
                    if k not in ("token", "completion_hash", "claim_id")
                }
                for a in db.all("SELECT * FROM attempts WHERE task_id = ? ORDER BY number", (tid,))
            ]
            return result

    def events(self, tid, limit=200):
        self.get(tid)
        with self.transaction(False) as db:
            rows = db.all(
                "SELECT * FROM events WHERE task_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
                (tid, limit),
            )
            return [{**r, "payload": json.loads(r["payload"])} for r in reversed(rows)]

    def tasks(self, limit=100):
        with self.transaction(False) as db:
            return [
                self.decode_task(r)
                for r in db.all(
                    "SELECT * FROM tasks ORDER BY created_at DESC, id DESC LIMIT ?", (limit,)
                )
            ]

    def workers(self):
        with self.transaction(False) as db:
            now = db.now()
            rows = db.all("SELECT * FROM workers ORDER BY name, session_id")
            return [{**r, "online": now - r["last_seen"] <= self.lease_seconds} for r in rows]

    def stats(self):
        with self.transaction(False) as db:
            return {
                r["state"]: r["n"]
                for r in db.all("SELECT state, COUNT(*) AS n FROM tasks GROUP BY state")
            }
