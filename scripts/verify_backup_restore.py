"""Disposable CI drill: PostgreSQL native dump/restore plus the shared files.

Never point this at production. It creates temporary schemas/databases and deletes
only its own temporary files. The PG service container supplies matching client tools.
"""

import argparse
import hashlib
import json
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import psycopg
from business_benchmark import run
from psycopg import sql

from dataflowcore.store import Store


def scoped_dsn(base, schema, database=None):
    parts = urlsplit(base)
    query = dict(parse_qsl(parts.query))
    query["options"] = "-c search_path=" + schema
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            "/" + database if database else parts.path,
            urlencode(query, quote_via=quote),
            parts.fragment,
        )
    )


def verify(dsn, container):
    identity = uuid.uuid4().hex
    schema, database = "restore_" + identity, "restore_" + identity
    user = urlsplit(dsn).username
    source_database = urlsplit(dsn).path.lstrip("/")
    dump = "/tmp/dataflowcore-" + identity + ".dump"
    source = scoped_dsn(dsn, schema)
    target = scoped_dsn(dsn, schema, database)
    connection = psycopg.connect(dsn, autocommit=True)
    target_created = False

    def pg(*args):
        subprocess.run(
            ["docker", "exec", container, *args], check=True, timeout=60, stdout=subprocess.DEVNULL
        )

    try:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        with tempfile.TemporaryDirectory(prefix="dataflow-restore-") as folder:
            root, saved = Path(folder) / "data", Path(folder) / "saved"
            first = run(root, files=4, workers=1, slots=2, chars=2048, interval=0.1, dsn=source)
            original = Store(source)
            original.set_paused(True)
            assert original.control()["active_attempts"] == 0
            with original.transaction(False) as db:
                task_ids = [r["id"] for r in db.all("SELECT id FROM tasks ORDER BY id")]
            pg("pg_dump", "-U", user, "-d", source_database, "-n", schema, "-Fc", "-f", dump)
            shutil.copytree(root, saved)
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
            target_created = True
            pg("pg_restore", "-U", user, "-d", database, "--exit-on-error", dump)
            # Simulate lost shared data, then restore at its original absolute path.
            shutil.rmtree(root)
            shutil.copytree(saved, root)
            restored = Store(target)
            assert restored.stats() == original.stats()
            assert restored.control()["paused"]
            hashes = 0
            for task_id in task_ids:
                task = restored.get(task_id)
                assert task["state"] == "SUCCEEDED" and task["attempt_count"] == 1
                for file in task["result"]["files"]:
                    path = Path(task["result"]["output_dir"]) / file["path"]
                    assert hashlib.sha256(path.read_bytes()).hexdigest() == file["sha256"]
                    hashes += 1
            restored.set_paused(False)
            resumed = run(
                root / "resumed", files=2, workers=1, slots=2, chars=2048, interval=0.1, dsn=target
            )
            return {
                "source_files": first["successful_files"],
                "restored_tasks": len(task_ids),
                "validated_restored_hashes": hashes,
                "resumed_files": resumed["successful_files"],
                "maintenance_pause_preserved": True,
                "native_pg_restore": True,
                "shared_files_restored": True,
            }
    finally:
        try:
            pg("rm", "-f", dump)
            if target_created:
                connection.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database))
                )
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        finally:
            connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True, help="dedicated disposable CI PostgreSQL")
    parser.add_argument("--pg-container", required=True, help="the disposable PostgreSQL service")
    parser.add_argument("--output", default=".e2e/backup-restore-evidence.json")
    args = parser.parse_args()
    evidence = verify(args.dsn, args.pg_container)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
