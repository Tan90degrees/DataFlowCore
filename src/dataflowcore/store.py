"""Durable, transactionally fenced orchestration.

PostgreSQL is the production store; SQLite is an explicitly single-host development store.
All writers take one database transaction lock. This intentionally simple single-controller
design makes cancellation, claims, expiry and completion serializable across API threads.
No locks/transactions are held while business code or network requests run.
"""

import base64
import json
import sqlite3
import uuid
from contextlib import contextmanager

from .contracts import (
    ACTIVE,
    TERMINAL,
    Backpressure,
    Conflict,
    Invalid,
    Missing,
    Paused,
    TaskSpec,
    canonical,
    fingerprint,
    integer,
    number,
)
from .security import request_actor

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
CREATE TABLE IF NOT EXISTS control_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS audit (
 id TEXT PRIMARY KEY, created_at DOUBLE PRECISION NOT NULL, principal TEXT NOT NULL,
 role TEXT NOT NULL, request_id TEXT, action TEXT NOT NULL, target TEXT, payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS audit_time ON audit(created_at, id);
CREATE INDEX IF NOT EXISTS attempts_retention ON attempts(state, finished_at);
CREATE TABLE IF NOT EXISTS attempt_cleanup (
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id), cleaned_at DOUBLE PRECISION NOT NULL
);
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
    def __init__(
        self, dsn: str, lease_seconds: float = 30, max_unfinished=10000, db_timeout_ms=5000
    ):
        if lease_seconds <= 0:
            raise Invalid("lease_seconds must be positive")
        self.dsn = dsn
        self.postgres = dsn.startswith(("postgresql://", "postgres://"))
        if not self.postgres and not dsn.startswith("sqlite:///"):
            raise Invalid("use postgresql:// or sqlite:///path")
        self.lease_seconds = lease_seconds
        self.max_unfinished = integer(max_unfinished, "max unfinished tasks", 0, 10000000)
        self.db_timeout_ms = integer(db_timeout_ms, "database timeout ms", 100, 600000)

    @contextmanager
    def transaction(self, write=True):
        if self.postgres:
            import psycopg
            from psycopg import IsolationLevel
            from psycopg.rows import dict_row

            connection = psycopg.connect(self.dsn, row_factory=dict_row, connect_timeout=5)
            if not write:
                connection.isolation_level = IsolationLevel.REPEATABLE_READ
                connection.read_only = True
        else:
            connection = sqlite3.connect(self.dsn.removeprefix("sqlite:///"), timeout=15)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 15000")
        try:
            session = Session(connection, self.postgres)
            if self.postgres:
                session.execute(
                    "SELECT set_config('statement_timeout', ?, true)", (str(self.db_timeout_ms),)
                )
                session.execute(
                    "SELECT set_config('lock_timeout', ?, true)",
                    (str(min(2000, self.db_timeout_ms)),),
                )
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
            pool = self.json_field("pool")
            runtime = self.json_field("runtime_version")
            db.execute(
                f"CREATE INDEX IF NOT EXISTS tasks_routing ON tasks "
                f"(({pool}), ({runtime}), state, available_at, created_at)"
            )

    def json_field(self, name, numeric=False):
        # Names are internal constants, never interpolated from user input.
        value = f"spec::jsonb->>'{name}'" if self.postgres else f"json_extract(spec, '$.{name}')"
        return f"CAST(({value}) AS DOUBLE PRECISION)" if numeric else value

    @staticmethod
    def event(db, task_id, attempt_id, kind, payload, now):
        db.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)",
            (uid(), task_id, attempt_id, kind, canonical(payload), now),
        )

    @staticmethod
    def audit(db, action, target=None, payload=None):
        actor = request_actor.get() or {"principal": "system", "role": "system"}
        db.execute(
            "INSERT INTO audit VALUES (?,?,?,?,?,?,?,?)",
            (
                uid(),
                db.now(),
                actor["principal"],
                actor["role"],
                actor.get("request_id"),
                action,
                target,
                canonical(payload or {}),
            ),
        )

    @staticmethod
    def paused(db):
        row = db.one("SELECT value FROM control_settings WHERE key='paused'")
        return bool(row and row["value"] == "true")

    def control(self):
        with self.transaction(False) as db:
            count = db.one(
                "SELECT COUNT(*) n FROM tasks WHERE state IN ('QUEUED','RUNNING','STOPPING')"
            )["n"]
            running = db.one(
                "SELECT COUNT(*) n FROM attempts WHERE state IN ('RUNNING','STOPPING')"
            )["n"]
            return {
                "paused": self.paused(db),
                "unfinished_tasks": count,
                "active_attempts": running,
                "max_unfinished_tasks": self.max_unfinished,
            }

    def set_paused(self, value):
        if type(value) is not bool:
            raise Invalid("paused must be a boolean")
        with self.transaction() as db:
            db.execute(
                "INSERT INTO control_settings(key,value) VALUES ('paused',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (canonical(value),),
            )
            self.audit(db, "control.paused" if value else "control.resumed")
        return self.control()

    def audit_page(self, limit=100, before=None):
        integer(limit, "audit limit", 1, 200)
        values = []
        where = ""
        if before:
            try:
                created, identity = json.loads(base64.urlsafe_b64decode(before))
                number(created, "audit cursor timestamp", 0, 1e15)
                if not isinstance(identity, str) or len(identity) > 100:
                    raise ValueError()
            except (ValueError, TypeError, UnicodeError) as exc:
                raise Invalid("invalid audit cursor") from exc
            where = "WHERE (created_at < ? OR (created_at = ? AND id < ?))"
            values = [created, created, identity]
        with self.transaction(False) as db:
            rows = db.all(
                f"SELECT * FROM audit {where} ORDER BY created_at DESC,id DESC LIMIT ?",
                (*values, limit + 1),
            )
        selected = rows[:limit]
        cursor = (
            base64.urlsafe_b64encode(
                canonical([selected[-1]["created_at"], selected[-1]["id"]]).encode()
            ).decode()
            if len(rows) > limit
            else None
        )
        return {
            "events": [{**r, "payload": json.loads(r["payload"])} for r in selected],
            "next_cursor": cursor,
        }

    def submit(self, spec: TaskSpec, request_key: str, retry_of=None):
        spec = TaskSpec.parse(spec.json())
        encoded = canonical(spec.storage())
        hashed = fingerprint(spec.json())
        with self.transaction() as db:
            existing = db.one("SELECT * FROM tasks WHERE request_key = ?", (request_key,))
            if existing:
                previous = TaskSpec.parse({"scheduling": "task", **json.loads(existing["spec"])})
                if existing["request_hash"] != hashed and fingerprint(previous.json()) != hashed:
                    raise Conflict("idempotency key reused with a different specification")
                return self.decode_task(existing)
            if self.paused(db):
                raise Paused("new submissions are paused for maintenance")
            if (
                self.max_unfinished
                and db.one(
                    "SELECT COUNT(*) n FROM tasks WHERE state IN ('QUEUED','RUNNING','STOPPING')"
                )["n"]
                >= self.max_unfinished
            ):
                raise Backpressure("unfinished task limit reached; retry later with the same key")
            now, tid = db.now(), uid()
            db.execute(
                """INSERT INTO tasks
                (id, request_key, request_hash, spec, state, attempt_count, available_at,
                 created_at, updated_at) VALUES (?, ?, ?, ?, 'QUEUED', 0, ?, ?, ?)""",
                (tid, request_key, hashed, encoded, now, now, now),
            )
            self.event(db, tid, None, "submitted", {}, now)
            self.audit(
                db,
                "task.retried" if retry_of else "task.submitted",
                tid,
                {"retry_of": retry_of} if retry_of else {},
            )
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
                "UPDATE workers SET last_seen = ?, "
                "draining = CASE WHEN draining = 1 THEN 1 ELSE ? END WHERE session_id = ?",
                (db.now(), int(draining), session_id),
            )
            actual = self.worker(db, session_id)["draining"]
        return {"ok": True, "draining": bool(actual)}

    def drain(self, session_id):
        with self.transaction() as db:
            self.worker(db, session_id)
            db.execute("UPDATE workers SET draining = 1 WHERE session_id = ?", (session_id,))
            self.audit(db, "worker.drained", session_id)
        return {"session_id": session_id, "draining": True}

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
            if (
                self.paused(db)
                or worker["draining"]
                or now - worker["last_seen"] > self.lease_seconds
            ):
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
            legacy = [s for s in used if s.get("scheduling", "task") == "task"]
            cpu_left = worker["cpu"] - sum(s["cpu"] for s in legacy)
            mem_left = worker["memory_mb"] - sum(s["memory_mb"] for s in legacy)
            mode = f"COALESCE(({self.json_field('scheduling')}), 'task')"
            # Filter eligibility before LIMIT: unrelated pools or oversized files
            # must never starve a runnable task behind the first 1000 queue entries.
            candidates = db.all(
                "SELECT * FROM tasks WHERE state = 'QUEUED' AND available_at <= ? "
                f"AND ({self.json_field('pool')}) = ? "
                f"AND ({self.json_field('runtime_version')}) = ? "
                f"AND (({mode} = 'operator' AND {self.json_field('cpu', True)} <= ? "
                f"AND {self.json_field('memory_mb', True)} <= ?) "
                f"OR ({mode} = 'task' AND {self.json_field('cpu', True)} <= ? "
                f"AND {self.json_field('memory_mb', True)} <= ?)) "
                "ORDER BY created_at, id LIMIT 1",
                (
                    now,
                    worker["pool"],
                    worker["runtime_version"],
                    worker["cpu"],
                    worker["memory_mb"],
                    cpu_left,
                    mem_left,
                ),
            )
            for task in candidates:
                spec = json.loads(task["spec"])
                capacity_cpu, capacity_memory = (
                    (worker["cpu"], worker["memory_mb"])
                    if spec.get("scheduling", "task") == "operator"
                    else (cpu_left, mem_left)
                )
                if (
                    spec["pool"] != worker["pool"]
                    or spec["runtime_version"] != worker["runtime_version"]
                    or spec["cpu"] > capacity_cpu
                    or spec["memory_mb"] > capacity_memory
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
        spec = json.loads(task["spec"])
        if spec.get("scheduling", "task") == "task":
            # Old runners reject unknown fields. Keep the legacy wire contract
            # even after manual retry normalized the persisted definition.
            spec.pop("scheduling", None)
            spec.pop("io_workers", None)
        return {
            "task_id": task["id"],
            "attempt_id": attempt["id"],
            "token": attempt["token"],
            "number": attempt["number"],
            "spec": spec,
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
                self.audit(db, "task.cancelled", tid)
            return self.decode_task(db.one("SELECT * FROM tasks WHERE id = ?", (tid,)))

    def retry(self, tid, request_key):
        # A new task identity retains its link to the previous failed/cancelled task.
        old = self.get(tid, compact=True)
        if old["state"] not in ("FAILED", "CANCELLED"):
            raise Conflict("only failed or cancelled tasks can be manually retried")
        spec = TaskSpec.parse({"scheduling": old["scheduling"], **old["spec"]})
        return self.submit(spec, f"retry:{tid}:{request_key}", retry_of=tid)

    @staticmethod
    def decode_task(row):
        task = {
            k: json.loads(v) if k in ("spec", "result") and v is not None else v
            for k, v in row.items()
            if k not in ("request_hash",)
        }
        if "spec" in task:
            spec = task["spec"]
            task["scheduling"] = spec.get("scheduling", "task")
            task["placement"] = {key: spec[key] for key in ("cpu", "memory_mb")}
            if task["scheduling"] == "operator":
                task["spec"] = {
                    k: v
                    for k, v in spec.items()
                    if k
                    not in (
                        "cpu",
                        "memory_mb",
                        "dag_workers",
                        "io_workers",
                        "scheduling",
                    )
                }
        return task

    def get(self, tid, compact=False, attempt_id=None):
        with self.transaction(False) as db:
            row = db.one("SELECT * FROM tasks WHERE id = ?", (tid,))
            if row is None:
                raise Missing("unknown task")
            result = self.decode_task(row)
            fields = "*"
            values = (tid,)
            if compact:
                fields = (
                    "id,task_id,number,worker_session,state,started_at,finished_at,lease_until,"
                    "deadline,error,CASE WHEN id=? OR id=? THEN progress ELSE '{}' END progress,"
                    "CASE WHEN id=? OR id=? THEN result ELSE NULL END result,"
                    "CASE WHEN id=? OR id=? THEN 1 ELSE 0 END detail_loaded"
                )
                values = (row["current_attempt"], attempt_id) * 3 + (tid,)
            result["attempts"] = [
                {
                    k: json.loads(v) if k in ("progress", "result") and v is not None else v
                    for k, v in a.items()
                    if k not in ("token", "completion_hash", "claim_id")
                }
                for a in db.all(
                    f"SELECT {fields} FROM attempts WHERE task_id = ? ORDER BY number", values
                )
            ]
            if attempt_id is not None and not any(
                a["id"] == attempt_id for a in result["attempts"]
            ):
                raise Missing("unknown attempt for this task")
            if row["state"] == "QUEUED":
                result["queue_diagnostic"] = self.queue_diagnostic(db, row)
            return result

    def queue_diagnostic(self, db, task):
        now = db.now()
        if self.paused(db):
            return {"code": "MAINTENANCE", "message": "管控处于维护暂停，暂不分发新任务"}
        if task["available_at"] > now:
            return {
                "code": "RETRY_BACKOFF",
                "message": "等待故障重试退避",
                "retry_in_seconds": task["available_at"] - now,
            }
        spec = json.loads(task["spec"])
        candidates = db.all(
            "SELECT * FROM workers WHERE last_seen >= ?", (now - self.lease_seconds,)
        )
        stages = (
            (lambda w: True, "NO_ONLINE_WORKER", "没有在线执行器"),
            (lambda w: w["pool"] == spec["pool"], "POOL_MISMATCH", "没有匹配执行池的在线执行器"),
            (
                lambda w: w["runtime_version"] == spec["runtime_version"],
                "VERSION_MISMATCH",
                "没有匹配运行版本的在线执行器",
            ),
            (lambda w: not w["draining"], "DRAINING", "匹配的执行器都在排空"),
            (
                lambda w: w["cpu"] >= spec["cpu"] and w["memory_mb"] >= spec["memory_mb"],
                "RESOURCE_TOO_LARGE",
                "单算子放置需求超过匹配执行器容量",
            ),
        )
        for predicate, code, message in stages:
            candidates = [w for w in candidates if predicate(w)]
            if not candidates:
                return {"code": code, "message": message}
        active = {
            r["worker_session"]: r["n"]
            for r in db.all(
                "SELECT worker_session,COUNT(*) n FROM attempts "
                "WHERE state IN ('RUNNING','STOPPING') GROUP BY worker_session"
            )
        }
        if all(active.get(w["session_id"], 0) >= w["slots"] for w in candidates):
            return {"code": "NO_FREE_SLOT", "message": "匹配的执行器槽位已满"}
        return {
            "code": "WAITING_FOR_CLAIM",
            "message": "等待执行器下一次领取，或历史任务预留资源释放",
        }

    def ping(self):
        with self.transaction(False) as db:
            db.one("SELECT 1 AS ok")

    def telemetry(self):
        with self.transaction(False) as db:
            now = db.now()
            task_counts = {
                r["state"]: r["n"]
                for r in db.all("SELECT state,COUNT(*) n FROM tasks GROUP BY state")
            }
            attempt_counts = {
                r["state"]: r["n"]
                for r in db.all("SELECT state,COUNT(*) n FROM attempts GROUP BY state")
            }
            oldest = db.one(
                "SELECT MIN(available_at) t FROM tasks WHERE state='QUEUED' AND available_at <= ?",
                (now,),
            )["t"]
            workers = db.one(
                "SELECT COUNT(*) online,COALESCE(SUM(slots),0) slots FROM workers "
                "WHERE last_seen >= ? AND draining=0",
                (now - self.lease_seconds,),
            )
            return {
                "tasks": task_counts,
                "attempts": attempt_counts,
                "oldest_queue_seconds": max(0, now - oldest) if oldest else 0,
                "online_workers": workers["online"],
                "worker_slots": workers["slots"],
                "paused": int(self.paused(db)),
            }

    def output_cleanup_candidates(self, cutoff, limit=200):
        number(cutoff, "cleanup cutoff", 0, 1e15)
        integer(limit, "cleanup limit", 1, 10000)
        with self.transaction(False) as db:
            return db.all(
                "SELECT a.id,a.task_id,a.finished_at FROM attempts a "
                "JOIN tasks t ON a.task_id=t.id "
                "WHERE a.state IN ('FAILED','CANCELLED','LOST') AND a.finished_at < ? "
                "AND t.state IN ('SUCCEEDED','FAILED','CANCELLED') "
                "AND NOT EXISTS (SELECT 1 FROM attempt_cleanup c WHERE c.attempt_id=a.id) "
                "ORDER BY a.finished_at,a.id LIMIT ?",
                (cutoff, limit),
            )

    def output_referenced(self, path):
        expression = (
            "result::jsonb->>'output_dir'"
            if self.postgres
            else "json_extract(result,'$.output_dir')"
        )
        with self.transaction(False) as db:
            return bool(
                db.one(f"SELECT id FROM tasks WHERE ({expression}) = ? LIMIT 1", (str(path),))
            )

    def mark_outputs_cleaned(self, attempt_id, removed):
        with self.transaction() as db:
            db.execute(
                "INSERT INTO attempt_cleanup VALUES (?,?) ON CONFLICT(attempt_id) DO NOTHING",
                (attempt_id, db.now()),
            )
            self.audit(db, "outputs.cleaned", attempt_id, {"removed": removed})

    def events(self, tid, limit=200):
        with self.transaction(False) as db:
            if db.one("SELECT id FROM tasks WHERE id=?", (tid,)) is None:
                raise Missing("unknown task")
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

    def task_page(self, limit=100, cursor=None, state=None, pool=None, summary=False):
        integer(limit, "limit", 1, 100)
        clauses, values = [], []
        if state is not None:
            if state not in TERMINAL | ACTIVE | {"QUEUED"}:
                raise Invalid("invalid task state filter")
            clauses.append("state = ?")
            values.append(state)
        if pool is not None:
            if not isinstance(pool, str) or not 1 <= len(pool) <= 4096:
                raise Invalid("invalid pool filter")
            clauses.append(f"({self.json_field('pool')}) = ?")
            values.append(pool)
        if cursor:
            try:
                if len(cursor) > 500:
                    raise ValueError()
                created, tid = json.loads(base64.urlsafe_b64decode(cursor))
                number(created, "cursor timestamp", 0, 1e15)
                if not isinstance(tid, str) or not 1 <= len(tid) <= 100:
                    raise ValueError()
            except (ValueError, TypeError, UnicodeError) as exc:
                raise Invalid("invalid task cursor") from exc
            clauses.append("(created_at < ? OR (created_at = ? AND id < ?))")
            values.extend([created, created, tid])
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        fields = "*"
        if summary:
            fields = (
                "id,state,attempt_count,current_attempt,available_at,created_at,updated_at,error,"
                f"({self.json_field('name')}) AS name,({self.json_field('pool')}) AS pool,"
                f"COALESCE(({self.json_field('scheduling')}),'task') AS scheduling,"
                f"({self.json_field('cpu', True)}) AS placement_cpu,"
                f"({self.json_field('memory_mb', True)}) AS placement_memory"
            )
        with self.transaction(False) as db:
            rows = db.all(
                f"SELECT {fields} FROM tasks {where} ORDER BY created_at DESC, id DESC LIMIT ?",
                (*values, limit + 1),
            )
        selected = rows[:limit]
        if summary:
            tasks = [
                {
                    **{
                        k: v for k, v in t.items() if k not in ("placement_cpu", "placement_memory")
                    },
                    "placement": {"cpu": t["placement_cpu"], "memory_mb": t["placement_memory"]},
                }
                for t in selected
            ]
        else:
            tasks = [self.decode_task(r) for r in selected]
        next_cursor = None
        if len(rows) > limit:
            last = selected[-1]
            next_cursor = base64.urlsafe_b64encode(
                canonical([last["created_at"], last["id"]]).encode()
            ).decode()
        return {"tasks": tasks, "next_cursor": next_cursor}

    def statuses(self, task_ids):
        if not isinstance(task_ids, list) or not 1 <= len(task_ids) <= 100:
            raise Invalid("task_ids must contain 1..100 IDs")
        if any(not isinstance(t, str) or not 1 <= len(t) <= 100 for t in task_ids):
            raise Invalid("invalid task ID")
        placeholders = ",".join("?" for _ in task_ids)
        with self.transaction(False) as db:
            rows = db.all(
                "SELECT t.id, t.state, t.attempt_count, t.current_attempt, t.created_at, "
                "t.updated_at, t.error, a.progress FROM tasks t LEFT JOIN attempts a "
                f"ON t.current_attempt = a.id WHERE t.id IN ({placeholders})",
                task_ids,
            )
        statuses = {}
        for row in rows:
            progress = json.loads(row["progress"]) if row["progress"] else {}
            row["progress"] = {
                k: v
                for k, v in progress.items()
                if k in ("fraction", "completed_steps", "total_steps", "elapsed_seconds", "state")
            }
            statuses[row["id"]] = row
        return {
            "tasks": [statuses[t] for t in dict.fromkeys(task_ids) if t in statuses],
            "missing": [t for t in dict.fromkeys(task_ids) if t not in statuses],
        }

    def workers(self):
        with self.transaction(False) as db:
            now = db.now()
            rows = db.all("SELECT * FROM workers ORDER BY name, session_id")
            snapshots = {}
            for attempt in db.all(
                "SELECT worker_session, progress FROM attempts "
                "WHERE state IN ('RUNNING','STOPPING') "
                "ORDER BY started_at"
            ):
                progress = json.loads(attempt["progress"])
                if "scheduler" in progress:
                    snapshots[attempt["worker_session"]] = progress["scheduler"]
            return [
                {
                    **r,
                    "online": now - r["last_seen"] <= self.lease_seconds,
                    "scheduler": snapshots.get(r["session_id"]),
                }
                for r in rows
            ]

    def stats(self):
        with self.transaction(False) as db:
            return {
                r["state"]: r["n"]
                for r in db.all("SELECT state, COUNT(*) AS n FROM tasks GROUP BY state")
            }

    def overview(self):
        with self.transaction(False) as db:
            return {
                "task_counts": {
                    r["state"]: r["n"]
                    for r in db.all("SELECT state,COUNT(*) n FROM tasks GROUP BY state")
                },
                "paused": self.paused(db),
            }
