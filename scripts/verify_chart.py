"""Validate rendered numeric environment variables against the real CLI parser."""

import argparse
import json
import os
import re
from pathlib import Path

from dataflowcore.cli import parser


def verify(path):
    rendered = Path(path).read_text()
    values = dict(re.findall(r'- name: (DATAFLOW_[A-Z_]+)\s+value: "([^"\n]*)"', rendered))
    required = {
        "DATAFLOW_MAX_UNFINISHED",
        "DATAFLOW_DB_TIMEOUT_MS",
        "DATAFLOW_UPLOAD_MIN_FREE_BYTES",
        "DATAFLOW_UPLOAD_MAX_BYTES",
        "DATAFLOW_UPLOAD_CONCURRENCY",
    }
    assert required <= values.keys(), "rendered control environment is incomplete"
    for key in required:
        assert str(int(values[key])) == values[key], f"{key} must be a decimal integer"
    saved = os.environ.copy()
    try:
        os.environ.update(values)
        control = parser().parse_args(["control"])
        assert control.upload_min_free_bytes == int(values["DATAFLOW_UPLOAD_MIN_FREE_BYTES"])
        assert control.max_unfinished == int(values["DATAFLOW_MAX_UNFINISHED"])
        assert control.db_timeout_ms == int(values["DATAFLOW_DB_TIMEOUT_MS"])
        block = re.search(r"args:\n((?:\s+- [^\n]+\n)+)", rendered)
        assert block, "rendered worker arguments are missing"
        args = [
            json.loads(value) if value.startswith('"') else value
            for value in re.findall(r"^\s+- (.+)$", block[1], flags=re.MULTILINE)
        ]
        worker = parser().parse_args(args)
        assert worker.command == "worker"
        assert worker.slots is None or worker.slots > 0
    finally:
        os.environ.clear()
        os.environ.update(saved)
    print("Rendered Chart environment and worker arguments accepted by CLI")


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("rendered")
    verify(args.parse_args().rendered)
