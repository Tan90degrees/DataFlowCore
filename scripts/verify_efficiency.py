"""Verify business correctness and measurable efficiency against the pinned baseline."""

import argparse
import json
from pathlib import Path


def verify(path):
    report = json.loads(Path(path).read_text())
    config = report["configuration"]
    assert config["repetitions"] >= 2
    for name, trials in report["trials"].items():
        digests = {sample["source_digest"] for trial in trials for sample in trial.values()}
        assert len(digests) == 1, f"{name} source changed during measurement"
        for trial in trials:
            result = trial["business"]["result"]
            assert result["successful_files"] == result["attempts"] == config["files"]
            assert result["validated_index_rows"] == config["files"] * 5
            assert result["runner_processes_used"] <= 8, (
                "process creation is approaching per-file churn"
            )
            assert not result["runtime"]["gil_enabled"]
            if name == "candidate" and config["business_store"] == "postgres":
                pool = result["database_pool"]["pool"]
                assert 0 < pool["connections_num"] <= 16
                assert pool["requests_num"] > pool["connections_num"] * 4
    old, new = (report["summary"][name] for name in ("baseline", "candidate"))
    assert new["idle_requests"] <= old["idle_requests"] * 0.65
    assert new["idle_log_bytes"] <= old["idle_log_bytes"] * 0.15
    # Default intervals must stop limiting short-file reuse. Aggressive polling
    # comparisons use a noise tolerance and never claim a universal throughput gain.
    minimum = 2 if config["interval"] >= 1 else 0.8
    assert new["files_per_second"] >= old["files_per_second"] * minimum
    print(
        "Internal efficiency: validated files, stable source, bounded reusable pool, "
        "lower idle traffic/logs and event-driven slot reuse"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("report")
    verify(parser.parse_args().report)
