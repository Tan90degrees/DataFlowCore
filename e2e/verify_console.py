"""Verify the independently deployed frontend and its authenticated API proxy."""

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlencode

from dataflowcore.client import Client

url = "http://127.0.0.1:18082"
for _ in range(60):
    try:
        with urllib.request.urlopen(url + "/index.html", timeout=2) as response:
            assert "任务管控台" in response.read().decode()
        break
    except OSError:
        time.sleep(0.5)
else:
    raise AssertionError("console deployment not ready")
client = Client(url + "/api", os.environ["DATAFLOW_ADMIN_TOKEN"])
overview = client.request("GET", "/v1/overview")
assert sum(overview["task_counts"].values()) > 0
# The frontend proxy must not remove API authentication.
try:
    urllib.request.urlopen(url + "/api/v1/overview", timeout=3)
except urllib.error.HTTPError as error:
    assert error.code == 401
else:
    raise AssertionError("console API proxy bypassed authentication")
payload = "前端上传 DataFlow hello world\n".encode() + b" " * (2 * 1024 * 1024)
upload_key = "k8s-console-upload"
upload_request = urllib.request.Request(
    url + "/api/v1/files?" + urlencode({"filename": "前端上传.txt"}),
    payload,
    {
        "Authorization": "Bearer " + os.environ["DATAFLOW_ADMIN_TOKEN"],
        "Content-Type": "application/octet-stream",
        "Idempotency-Key": upload_key,
    },
    method="POST",
)
with urllib.request.urlopen(upload_request, timeout=15) as response:
    assert response.status == 201
    uploaded = json.load(response)
assert uploaded["input_sha256"] == hashlib.sha256(payload).hexdigest()
assert uploaded["input_path"].startswith("/dataflow/uploads/")
assert client.request("GET", "/v1/files", key=upload_key) == uploaded
task = client.submit(
    {
        "name": "console uploaded document",
        "input_path": uploaded["input_path"],
        "input_sha256": uploaded["input_sha256"],
        "retry_delay": 0,
        "steps": [
            {"id": "parse", "callable": "dataflowcore.examples.ingestion:parse"},
            {
                "id": "stats",
                "callable": "dataflowcore.examples.ingestion:statistics",
                "depends_on": ["parse"],
            },
        ],
    }
)
finished = client.wait_many([task["id"]], timeout=40)[0]
assert finished["state"] == "SUCCEEDED"
result_task = client.get(task["id"])
assert result_task["result"]["steps"]["stats"]["words"] == 4
evidence = Path(".e2e/evidence.json")
result = json.loads(evidence.read_text())
result["console"] = {
    "page": "ok",
    "proxy": "ok",
    "unauthorized": 401,
    "overview": overview,
    "upload": uploaded,
    "uploaded_task": result_task,
}
evidence.write_text(json.dumps(result, indent=2))
print("Kubernetes console page + authenticated API proxy + 2 MiB upload execution: passed")
