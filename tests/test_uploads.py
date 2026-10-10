"""Real HTTP upload boundaries, crash cleanup and uploaded-file DAG execution."""

import errno
import hashlib
import http.client
import json
import threading
import uuid
from pathlib import Path
from urllib.parse import urlencode

import pytest

from dataflowcore.api import Server
from dataflowcore.contracts import Invalid
from dataflowcore.uploads import Uploads
from tests.test_e2e import until


@pytest.fixture
def upload_api(store, tmp_path):
    server = Server(
        ("127.0.0.1", 0),
        store,
        str(tmp_path),
        "a" * 32,
        "w" * 32,
        upload_max_bytes=4 * 1024 * 1024,
        upload_concurrency=1,
        upload_timeout=30,
        cors_origins=["http://localhost:8000"],
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    thread.join(timeout=5)
    server.server_close()


def request(
    api,
    method="POST",
    data=b"hello",
    name="中文 文档.txt",
    key="upload-key",
    path=None,
    headers=None,
):
    port = getattr(api, "server_port", None) or api.port
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    fields = {
        "Authorization": "Bearer " + "a" * 32,
        "Idempotency-Key": key,
        "Content-Type": "application/octet-stream",
    }
    fields.update(headers or {})
    if path is None:
        path = "/v1/files?" + urlencode({"filename": name})
    try:
        connection.request(method, path, data if method == "POST" else None, headers=fields)
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    finally:
        connection.close()


def partial(api, key, size=10):
    connection = http.client.HTTPConnection("127.0.0.1", api.server_port, timeout=5)
    connection.putrequest("POST", "/v1/files?filename=partial.txt")
    connection.putheader("Authorization", "Bearer " + "a" * 32)
    connection.putheader("Idempotency-Key", key)
    connection.putheader("Content-Type", "application/octet-stream")
    connection.putheader("Content-Length", str(size))
    connection.endheaders(b"a")
    until(lambda: key in api.uploads.active, timeout=1)
    return connection


def test_large_binary_upload_and_metadata_can_be_recovered(upload_api, tmp_path):
    data = ("中文输入\n".encode() + b"\x00\xff") * 100000
    assert len(data) > 1_100_000
    status, uploaded = request(upload_api, data=data)
    assert status == 201
    source = Path(uploaded["input_path"])
    assert source.is_relative_to(tmp_path / "uploads")
    assert source.name == "中文 文档.txt"
    assert source.read_bytes() == data
    assert uploaded["input_sha256"] == hashlib.sha256(data).hexdigest()
    assert uploaded["size_bytes"] == len(data)
    assert not source.stat().st_mode & 0o222
    assert request(upload_api, method="GET", path="/v1/files")[1] == uploaded
    assert not list(upload_api.uploads.staging.iterdir())
    # Completed source + metadata survive a fresh controller upload service.
    assert Uploads(tmp_path).get("upload-key") == uploaded


def test_retry_is_idempotent_and_different_bytes_do_not_overwrite(upload_api):
    first = request(upload_api, data=b"same")
    repeated = request(upload_api, data=b"same")
    assert first[0] == 201 and repeated[0] == 200
    assert first[1] == repeated[1]
    assert request(upload_api, data=b"DIFF")[0] == 409
    assert request(upload_api, data=b"longer")[0] == 409
    assert request(upload_api, name="other.txt", data=b"same")[0] == 409
    assert Path(first[1]["input_path"]).read_bytes() == b"same"
    assert len(list(upload_api.uploads.root.glob("*/metadata.json"))) == 1


@pytest.mark.parametrize(
    "name", ["../escape.txt", r"dir\file.txt", "", "..", "bad\nname", "文" * 86 + ".txt"]
)
def test_unsafe_names_are_rejected_without_publishing(upload_api, name):
    assert request(upload_api, name=name)[0] == 400
    assert not list(upload_api.uploads.root.glob("*/metadata.json"))
    assert not list(upload_api.uploads.staging.iterdir())


def test_authentication_and_cors_are_applied_to_uploads(upload_api):
    for token in ("", "Bearer " + "w" * 32):
        assert request(upload_api, headers={"Authorization": token})[0] == 401
    assert request(upload_api, headers={"Origin": "http://evil:8000"})[0] == 403
    assert request(upload_api, headers={"Origin": "http://localhost:8000"})[0] == 201
    assert (
        request(upload_api, method="GET", path="/v1/files", headers={"Authorization": ""})[0] == 401
    )


def test_limits_framing_and_disabled_uploads(upload_api, tmp_path):
    assert request(upload_api, method="GET", path="/v1/files/limits")[1] == {
        "enabled": True,
        "max_bytes": 4 * 1024 * 1024,
        "max_concurrent": 1,
        "timeout_seconds": 30,
    }
    for lengths, transfer, status in [
        ([], None, 411),
        (["-1"], None, 400),
        (["1", "1"], None, 400),
        (["0"], "chunked", 400),
        ([str(4 * 1024 * 1024 + 1)], None, 413),
    ]:
        connection = http.client.HTTPConnection("127.0.0.1", upload_api.server_port, timeout=3)
        try:
            connection.putrequest("POST", "/v1/files?filename=test.txt")
            connection.putheader("Authorization", "Bearer " + "a" * 32)
            connection.putheader("Idempotency-Key", "framing")
            connection.putheader("Content-Type", "application/octet-stream")
            for length in lengths:
                connection.putheader("Content-Length", length)
            if transfer:
                connection.putheader("Transfer-Encoding", transfer)
            connection.endheaders()
            response = connection.getresponse()
            assert response.status == status
            response.read()
        finally:
            connection.close()
    assert request(upload_api, headers={"Content-Type": "application/json"})[0] == 415
    assert request(upload_api, key="")[0] == 400
    assert request(upload_api, path="/v1/files?filename=a&filename=b")[0] == 400
    disabled_root = tmp_path / "read-only-data"
    upload_api.uploads = Uploads(disabled_root, max_bytes=0)
    assert not disabled_root.exists()
    assert request(upload_api)[0] == 403


def test_partial_disconnect_cleans_staging_and_releases_slots(upload_api):
    connection = partial(upload_api, "aborted")
    assert list(upload_api.uploads.staging.iterdir())
    connection.close()
    until(lambda: not upload_api.uploads.active, timeout=2)
    assert not list(upload_api.uploads.staging.iterdir())
    assert request(upload_api, method="GET", path="/v1/files", key="aborted")[0] == 404
    assert request(upload_api, key="aborted")[0] == 201


def test_duplicate_control_start_does_not_delete_active_upload(upload_api):
    connection = partial(upload_api, "active-upload")
    try:
        with pytest.raises(OSError):
            Server(
                upload_api.server_address,
                upload_api.store,
                str(upload_api.uploads.data_root),
                "a" * 32,
                "w" * 32,
            )
        assert list(upload_api.uploads.staging.iterdir())
        connection.send(b"bcdefghij")
        response = connection.getresponse()
        assert response.status == 201
        metadata = json.loads(response.read())
        assert Path(metadata["input_path"]).read_bytes() == b"abcdefghij"
    finally:
        connection.close()


def test_admission_keeps_control_api_available_and_timeout_cleans_partial(upload_api):
    upload_api.uploads.timeout = 1
    connection = partial(upload_api, "slow")
    try:
        assert request(upload_api, key="slow")[0] == 409
        assert request(upload_api, key="other")[0] == 429
        assert request(upload_api, method="GET", path="/readyz")[0] == 200
        response = connection.getresponse()
        assert response.status == 408
        response.read()
    finally:
        connection.close()
    until(lambda: not upload_api.uploads.active, timeout=2)
    assert not list(upload_api.uploads.staging.iterdir())
    assert request(upload_api, key="slow")[0] == 201


def test_disk_full_cleans_partial_and_can_retry(upload_api, monkeypatch):
    def full(_):
        raise OSError(errno.ENOSPC, "disk full")

    with monkeypatch.context() as patch:
        patch.setattr("dataflowcore.uploads.os.fsync", full)
        assert request(upload_api)[0] == 507
    assert not list(upload_api.uploads.staging.iterdir())
    assert request(upload_api, method="GET", path="/v1/files")[0] == 404
    assert request(upload_api)[0] == 201


def test_empty_file_is_valid_and_restart_removes_only_staging(upload_api, tmp_path):
    status, uploaded = request(upload_api, data=b"")
    assert status == 201 and uploaded["size_bytes"] == 0
    abandoned = upload_api.uploads.staging / uuid.uuid4().hex
    abandoned.mkdir()
    (abandoned / "partial").write_bytes(b"partial bytes")
    recovered = Uploads(tmp_path)
    assert not abandoned.exists()
    assert recovered.get("upload-key") == uploaded


def test_upload_directory_must_stay_inside_data_root(tmp_path):
    root, outside = tmp_path / "root", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "uploads").symlink_to(outside, target_is_directory=True)
    with pytest.raises(Invalid):
        Uploads(root)
    assert not list(outside.iterdir())


def test_uploaded_file_executes_on_real_worker_and_survives_controller_restart(cluster):
    text = "上传后的真实文件 DataFlow hello world\n" * 100 + " " * (2 * 1024 * 1024)
    status, uploaded = request(cluster, data=text.encode(), key="business-upload")
    assert status == 201
    task = cluster.client.submit(
        {
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
    cluster.worker()
    finished = until(
        lambda: (
            value if (value := cluster.client.get(task["id"]))["state"] == "SUCCEEDED" else None
        ),
        timeout=25,
    )
    assert finished["result"]["steps"]["stats"]["words"] == 400
    assert finished["result"]["runtime"]["gil_enabled"] is False
    cluster.control.kill()
    cluster.control.wait(timeout=5)
    cluster.control = cluster.start_control()
    assert request(cluster, method="GET", path="/v1/files", key="business-upload")[1] == uploaded
    assert cluster.client.get(task["id"])["state"] == "SUCCEEDED"
