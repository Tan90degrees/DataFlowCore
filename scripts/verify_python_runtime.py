"""Check bounded reuse and idle work; retain CPU speedup as hardware-dependent evidence."""

import argparse
import json
from pathlib import Path


def verify(report):
    checksums = set()
    for name, slots in (("single_slot", 1), ("four_slots", 4)):
        samples = report["trials"][name]
        assert samples
        for sample in samples:
            assert sample["slots"] == sample["runner_processes_used"] == slots
            assert sample["single_attempt_successes"] == sample["files"]
            assert sample["validated_cache_reuse"]
            checksums.add(sample["checksum"])
    assert len(checksums) == 1
    idle = report["idle"]
    assert idle["worker_http_requests"] <= idle["seconds"] * 6 + 2
    assert idle["log_bytes"] == 0
    if idle["cpu_seconds"] is not None:
        assert 0 <= idle["cpu_seconds"] < idle["seconds"] * 0.15
    print(
        json.dumps(
            {
                "runtime": report["runtime"],
                "speedup": report["speedup_four_over_one"],
                "idle_requests": idle["worker_http_requests"],
                "idle_logs": idle["log_bytes"],
            }
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("report")
    verify(json.loads(Path(parser.parse_args().report).read_text()))
