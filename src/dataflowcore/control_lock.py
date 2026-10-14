"""One controller per metadata namespace; this is exclusion, not an HA protocol."""

import hashlib
from pathlib import Path

from .contracts import Conflict


class ControlLost(RuntimeError):
    pass


class ControlLock:
    def __init__(self, store):
        self.connection = self.file = None
        try:
            if store.postgres:
                import psycopg

                self.connection = psycopg.connect(
                    store.dsn,
                    autocommit=True,
                    connect_timeout=5,
                    keepalives_idle=10,
                    keepalives_interval=3,
                    keepalives_count=3,
                )
                self.connection.execute("SET statement_timeout = 2000")
                schema = self.connection.execute("SELECT current_schema()").fetchone()[0]
                key = int.from_bytes(
                    hashlib.sha256(("dataflowcore-controller:" + schema).encode()).digest()[:8],
                    signed=True,
                )
                if not self.connection.execute(
                    "SELECT pg_try_advisory_lock(%s)", (key,)
                ).fetchone()[0]:
                    raise Conflict("another controller owns this metadata namespace")
            else:
                import fcntl

                path = Path(store.dsn.removeprefix("sqlite:///")).resolve()
                path.parent.mkdir(parents=True, exist_ok=True)
                self.file = Path(str(path) + ".controller.lock").open("a")
                try:
                    fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise Conflict("another controller owns this metadata namespace") from exc
        except BaseException:
            self.close()
            raise

    def check(self):
        if self.connection is not None:
            try:
                self.connection.execute("SELECT 1")
            except Exception as exc:
                if self.connection.closed or self.connection.broken:
                    raise ControlLost("controller ownership connection was lost") from exc
                raise

    def close(self):
        if self.connection is not None:
            self.connection.close()
        if self.file is not None:
            self.file.close()
