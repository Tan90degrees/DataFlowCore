"""Consecutive trials must coexist in a persistent control database."""

from scripts.business_benchmark import run


def test_business_benchmark_repeated_trials_on_same_database(tmp_path, store):
    results = [
        run(
            tmp_path / name,
            files=2,
            workers=1,
            slots=2,
            interval=0.1,
            chars=1024,
            delay=0,
            dsn=store.dsn,
        )
        for name in ("first", "second")
    ]
    assert all(r["successful_files"] == r["attempts"] == 2 for r in results)
    assert results[0]["run_id"] != results[1]["run_id"]
    assert store.stats()["SUCCEEDED"] == 4
