"""Run the full reference business flow against an existing control service."""

import argparse
import json
import os
from pathlib import Path

from dataflowcore.client import Client
from dataflowcore.contracts import fingerprint
from dataflowcore.examples.ingestion import pipeline
from dataflowcore.runtime import file_hash


def main():
    p = argparse.ArgumentParser()
    p.add_argument("directory", help="shared directory of UTF-8 .md/.txt documents")
    p.add_argument("--url", default=os.getenv("DATAFLOW_URL", "http://127.0.0.1:8080"))
    p.add_argument("--embedding-url", help="POST {text} -> {vector}; omitted uses test fixture")
    p.add_argument("--index-path", required=True, help="shared absolute SQLite acceptance index")
    p.add_argument("--embedding-workers", type=int, default=4)
    p.add_argument(
        "--embedding-profile", default="fixture-sha256-v1", help="model/revision identity"
    )
    p.add_argument("--timeout", type=float, default=3600)
    args = p.parse_args()
    client = Client(args.url, os.environ["DATAFLOW_ADMIN_TOKEN"])
    flow = pipeline(
        parameters={
            "embedding_url": args.embedding_url,
            "embedding_workers": args.embedding_workers,
            "embedding_profile": args.embedding_profile,
            "index_path": args.index_path,
        }
    )
    files = sorted(
        p.resolve()
        for p in Path(args.directory).iterdir()
        if p.is_file() and p.suffix.lower() in (".md", ".txt")
    )
    if not files:
        raise SystemExit("no UTF-8 .md/.txt input files")
    tasks = []
    for source in files:
        spec = flow.spec(source, input_sha256=file_hash(source))
        task = client.submit(spec, key="ingest:" + fingerprint(spec))
        tasks.append(task["id"])
        print(json.dumps({"file": str(source), "task_id": task["id"]}, ensure_ascii=False))
    statuses = client.wait_many(tasks, timeout=args.timeout)
    print(json.dumps(statuses, ensure_ascii=False, indent=2))
    raise SystemExit(0 if all(t["state"] == "SUCCEEDED" for t in statuses) else 1)


if __name__ == "__main__":
    main()
