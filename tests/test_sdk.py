import pytest

from dataflowcore.contracts import Invalid
from dataflowcore.operators import read_text, write_json
from dataflowcore.sdk import Pipeline


def test_sdk_generates_valid_json_without_running_operators(tmp_path):
    plan = (
        Pipeline("example", cpu=2)
        .step("read", read_text)
        .step("write", write_json, depends_on=["read"])
    )
    spec = plan.spec(tmp_path / "not-yet-existing.txt")
    assert spec["steps"][0]["callable"] == "dataflowcore.operators:read_text"
    assert spec["steps"][1]["depends_on"] == ["read"]
    assert spec["cpu"] == 2


def test_sdk_rejects_local_and_instance_callables():
    with pytest.raises(Invalid):
        Pipeline("bad").step("bad", lambda c, i: None)

    class Instance:
        def __call__(self, context, inputs):
            pass

    with pytest.raises(Invalid):
        Pipeline("bad").step("bad", Instance())
