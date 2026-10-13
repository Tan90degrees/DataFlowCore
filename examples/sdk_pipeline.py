"""Run after putting input.txt on the shared filesystem."""

import os

from dataflowcore.operators import read_text, word_count, write_json
from dataflowcore.sdk import Client, Pipeline

pipeline = (
    Pipeline("word-count")
    .step("read", read_text, workload="io", resources={"cpu": 0.1, "memory_mb": 64})
    .step("count", word_count, depends_on=["read"], resources={"cpu": 1, "memory_mb": 64})
    .step(
        "write",
        write_json,
        depends_on=["count"],
        workload="io",
        resources={"cpu": 0.1, "memory_mb": 32},
    )
)
client = Client(
    os.getenv("DATAFLOW_URL", "http://127.0.0.1:8080"), os.environ["DATAFLOW_ADMIN_TOKEN"]
)
task = client.submit(pipeline.spec("/dataflow/input.txt"))
print(task["id"])
