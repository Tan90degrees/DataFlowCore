"""Validate rendered numeric environment variables against the real CLI parser."""

import argparse
import os
import re
from pathlib import Path

from dataflowcore.cli import parser


def verify(path):
    values = dict(
        re.findall(r'- name: (DATAFLOW_[A-Z_]+)\s+value: "([^"\n]*)"', Path(path).read_text())
    )
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
    finally:
        os.environ.clear()
        os.environ.update(saved)
    print("Rendered Chart numeric environment accepted by CLI")


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("rendered")
    verify(args.parse_args().rendered)
