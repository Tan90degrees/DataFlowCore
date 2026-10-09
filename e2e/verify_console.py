"""Verify the independently deployed frontend and its authenticated API proxy."""

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

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
evidence = Path(".e2e/evidence.json")
result = json.loads(evidence.read_text())
result["console"] = {"page": "ok", "proxy": "ok", "unauthorized": 401, "overview": overview}
evidence.write_text(json.dumps(result, indent=2))
print("Kubernetes console page + authenticated API proxy: passed")
