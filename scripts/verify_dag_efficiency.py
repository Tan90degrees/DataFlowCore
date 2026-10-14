"""Validate work and deterministic overhead bounds; report timing without flaky gates."""

import argparse
import json
from pathlib import Path


def verify(before, after):
    summary = {}
    for shape in ("chain", "wide"):
        old, new = before["trials"][shape], after["trials"][shape]
        assert len(old) == len(new) and old
        for baseline, candidate in zip(old, new, strict=True):
            assert baseline["validated_steps"] == candidate["validated_steps"] == candidate["steps"]
            assert baseline["reports_per_step"] == candidate["reports_per_step"]
            assert baseline["executor_budget"] == candidate["executor_budget"]
            assert baseline["gil_enabled"] is candidate["gil_enabled"] is False
            assert baseline["source_sha256"] != candidate["source_sha256"]
            assert candidate["fsync_calls"] == 1, "only the terminal progress snapshot is fsynced"
            assert candidate["request_insert_statements"] == candidate["steps"]
        summary[shape] = {
            "elapsed_ratio_before_over_after": before["median"][shape]["elapsed_seconds"]
            / after["median"][shape]["elapsed_seconds"],
            "before": before["median"][shape],
            "after": after["median"][shape],
        }
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("before")
    parser.add_argument("after")
    args = parser.parse_args()
    print(
        json.dumps(
            verify(
                json.loads(Path(args.before).read_text()), json.loads(Path(args.after).read_text())
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
