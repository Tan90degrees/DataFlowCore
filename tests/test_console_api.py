"""Browser API boundaries: real HTTP, authentication and non-executing DAG validation."""

import json
import threading
import urllib.error
import urllib.request

import pytest

from dataflowcore.api import Server
from dataflowcore.contracts import Invalid


@pytest.fixture
def browser_api(store, tmp_path):
    server = Server(
        ("127.0.0.1", 0),
        store,
        str(tmp_path),
        "a" * 32,
        "w" * 32,
        cors_origins=["http://localhost:8000"],
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    thread.join(timeout=5)
    server.server_close()


def request(url, path, method="GET", data=None, **headers):
    req = urllib.request.Request(
        url + path,
        method=method,
        data=json.dumps(data).encode() if data is not None else None,
        headers={"Authorization": "Bearer " + "a" * 32, **headers},
    )
    try:
        response = urllib.request.urlopen(req, timeout=3)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read()
        return response.status, response.headers, json.loads(raw) if raw else None


def test_cors_requires_exact_origin_and_still_authenticates(browser_api):
    status, headers, _ = request(browser_api, "/v1/overview", Origin="http://localhost:8000")
    assert status == 200
    assert headers["Access-Control-Allow-Origin"] == "http://localhost:8000"
    assert "Access-Control-Allow-Credentials" not in headers
    assert request(browser_api, "/v1/overview", Origin="http://localhost:8000.evil")[0] == 403
    status, headers, _ = request(
        browser_api, "/v1/overview", Origin="http://localhost:8000", Authorization=""
    )
    assert status == 401
    assert headers["Access-Control-Allow-Origin"] == "http://localhost:8000"
    assert request(browser_api, "/v1/overview")[0] == 200


def test_preflight_allowlist(browser_api):
    headers = {
        "Origin": "http://localhost:8000",
        "Access-Control-Request-Method": "POST",
        "Access-Control-Request-Headers": "authorization,content-type,idempotency-key",
        "Authorization": "",
    }
    status, result, _ = request(browser_api, "/v1/tasks", "OPTIONS", **headers)
    assert status == 204
    assert result["Access-Control-Allow-Headers"] == "Authorization, Content-Type, Idempotency-Key"
    assert request(browser_api, "/v1/worker/claim", "OPTIONS", **headers)[0] == 403
    assert (
        request(
            browser_api,
            "/v1/tasks",
            "OPTIONS",
            **(headers | {"Access-Control-Request-Headers": "x-unapproved"}),
        )[0]
        == 403
    )
    assert (
        request(
            browser_api,
            "/v1/tasks",
            "OPTIONS",
            **(headers | {"Origin": "null"}),
        )[0]
        == 403
    )


def test_dag_validation_does_not_import_code_or_require_existing_file(browser_api, store):
    spec = {
        "input_path": "/not-yet-mounted/document.txt",
        "steps": [
            {"id": "start", "callable": "uninstalled.business:parse"},
            {"id": "left", "callable": "uninstalled.business:embed", "depends_on": ["start"]},
            {"id": "right", "callable": "uninstalled.business:stats", "depends_on": ["start"]},
            {
                "id": "sink",
                "callable": "uninstalled.business:index",
                "depends_on": ["left", "right"],
            },
        ],
    }
    status, _, result = request(browser_api, "/v1/dags/validate", "POST", spec)
    assert status == 200
    assert result["layers"] == [["start"], ["left", "right"], ["sink"]]
    assert result["spec"]["max_attempts"] == 3
    assert store.stats() == {}
    spec["steps"][0]["depends_on"] = ["sink"]
    status, _, result = request(browser_api, "/v1/dags/validate", "POST", spec)
    assert status == 400
    assert "cycle" in result["error"]
    spec["steps"][0]["depends_on"] = ["missing"]
    assert request(browser_api, "/v1/dags/validate", "POST", spec)[0] == 400


def test_reject_wildcard_cors_configuration(store, tmp_path):
    with pytest.raises(Invalid):
        Server(("127.0.0.1", 0), store, str(tmp_path), "a" * 32, "w" * 32, cors_origins=["*"])
