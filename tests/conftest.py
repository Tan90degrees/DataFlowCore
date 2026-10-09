import os
import uuid
from urllib.parse import quote, urlencode

import pytest

from dataflowcore.contracts import TaskSpec
from dataflowcore.store import Store


@pytest.fixture(
    params=["sqlite", "postgres"] if os.getenv("DATAFLOW_TEST_DATABASE_URL") else ["sqlite"]
)
def store(request, tmp_path):
    if request.param == "postgres":
        import psycopg
        from psycopg import sql

        base = os.environ["DATAFLOW_TEST_DATABASE_URL"]
        schema = "test_" + uuid.uuid4().hex
        connection = psycopg.connect(base, autocommit=True)
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        dsn = (
            base
            + ("&" if "?" in base else "?")
            + urlencode({"options": f"-c search_path={schema}"}, quote_via=quote)
        )
    else:
        dsn = "sqlite:///" + str(tmp_path / "state.db")
    value = Store(dsn, lease_seconds=6)
    value.migrate()
    yield value
    if request.param == "postgres":
        connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        connection.close()


@pytest.fixture
def spec(tmp_path):
    source = tmp_path / "input.txt"
    source.write_text("hello world hello")
    return TaskSpec.parse(
        {
            "input_path": str(source),
            "retry_delay": 0,
            "steps": [{"id": "read", "callable": "dataflowcore.operators:read_text"}],
        }
    )


def register(store, session="worker", **kwargs):
    args = {
        "session_id": session,
        "name": session,
        "pool": "default",
        "runtime_version": "0.1.0",
        "slots": 2,
        "cpu": 4,
        "memory_mb": 1024,
    }
    store.register(**(args | kwargs))


def complete(store, assignment, session="worker", **kwargs):
    return store.complete(
        assignment["task_id"],
        assignment["attempt_id"],
        session,
        assignment["token"],
        **({"state": "SUCCEEDED", "result": {"ok": 1}, "retryable": False} | kwargs),
    )
