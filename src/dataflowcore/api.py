"""Authenticated HTTP control plane. Worker requests never execute business code."""

import hashlib
import hmac
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from . import __version__
from .contracts import Conflict, Invalid, Missing, TaskSpec, integer
from .runtime import under_root
from .store import Store

log = logging.getLogger(__name__)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64

    def __init__(
        self, address, store, data_root, admin_token, worker_token, insecure=False, reap_interval=1
    ):
        if not insecure and (
            len(admin_token) < 24 or len(worker_token) < 24 or admin_token == worker_token
        ):
            raise Invalid("configure distinct admin/worker tokens of at least 24 characters")
        self.store, self.data_root = store, data_root
        self.admin_token, self.worker_token, self.insecure = admin_token, worker_token, insecure
        self.reap_interval = reap_interval
        self.stopped = threading.Event()
        self.slots = threading.BoundedSemaphore(64)
        super().__init__(address, Handler)

    def process_request(self, request, client_address):
        if not self.slots.acquire(blocking=False):
            request.close()
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()

    def serve_forever(self, poll_interval=0.2):
        self.reaper = threading.Thread(target=self.reconcile, name="reaper", daemon=True)
        self.reaper.start()
        try:
            super().serve_forever(poll_interval)
        finally:
            self.stopped.set()
            self.reaper.join(timeout=10)

    def reconcile(self):
        while not self.stopped.wait(self.reap_interval):
            try:
                count = self.store.reap()
                if count:
                    log.info("recovered_expired_attempts count=%s", count)
            except Exception:
                log.exception("reconciliation_failed")


class Handler(BaseHTTPRequestHandler):
    server_version = "DataFlowCore/" + __version__

    def setup(self):
        super().setup()
        self.connection.settimeout(10)

    def log_message(self, fmt, *args):
        log.info("http peer=%s %s", self.client_address[0], fmt % args)

    def send_json(self, status, value):
        data = json.dumps(value, allow_nan=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def body(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise Invalid("invalid content length") from exc
        if not 0 < length <= 1_100_000:
            raise Invalid("body must be a JSON object of at most 1.1 MB")
        try:
            value = json.loads(self.rfile.read(length))
        except (ValueError, UnicodeError) as exc:
            raise Invalid("invalid JSON") from exc
        if not isinstance(value, dict):
            raise Invalid("expected JSON object")
        return value

    def dispatch(self, method):
        path = urlsplit(self.path).path
        try:
            if method == "GET" and path in ("/healthz", "/version"):
                self.send_json(200, {"ok": True, "version": __version__})
                return
            role = "worker" if path.startswith("/v1/worker/") else "admin"
            expected = self.server.worker_token if role == "worker" else self.server.admin_token
            if (
                path != "/readyz"
                and not self.server.insecure
                and not hmac.compare_digest(
                    self.headers.get("Authorization", ""), f"Bearer {expected}"
                )
            ):
                self.send_json(401, {"error": "authentication required"})
                return
            if method == "GET" and path == "/readyz":
                self.server.store.stats()
                self.send_json(200, {"ready": True})
                return
            if method == "GET" and path == "/metrics":
                counts = self.server.store.stats()
                data = (
                    "# TYPE dataflow_tasks gauge\n"
                    + "".join(
                        f'dataflow_tasks{{state="{state}"}} {counts.get(state, 0)}\n'
                        for state in (
                            "QUEUED",
                            "RUNNING",
                            "STOPPING",
                            "SUCCEEDED",
                            "FAILED",
                            "CANCELLED",
                        )
                    )
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            result = self.route(method, path, self.body() if method == "POST" else {})
            self.send_json(200, result)
        except Missing as exc:
            self.send_json(404, {"error": str(exc)})
        except Conflict as exc:
            self.send_json(409, {"error": str(exc)})
        except (Invalid, KeyError, TypeError, ValueError) as exc:
            self.send_json(400, {"error": str(exc)})
        except Exception:
            log.exception("request_failed path=%s", path)
            self.send_json(503, {"error": "service temporarily unavailable"})

    def route(self, method, path, data):
        store = self.server.store
        parts = path.strip("/").split("/")
        if method == "POST" and path == "/v1/tasks":
            spec = TaskSpec.parse(data)
            source = under_root(spec.input_path, self.server.data_root)
            if not source.is_file():
                raise Invalid("input must be an existing shared file")
            # Pin input bytes before persistence; hashing is streaming and bounded in memory.
            with source.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if spec.input_sha256 and digest != spec.input_sha256:
                raise Invalid("input checksum mismatch")
            normalized = {**spec.json(), "input_sha256": digest}
            return store.submit(TaskSpec.parse(normalized), self.request_key())
        if method == "GET" and path == "/v1/tasks":
            return {"tasks": store.tasks()}
        if method == "GET" and path == "/v1/workers":
            return {"workers": store.workers()}
        if len(parts) >= 3 and parts[:2] == ["v1", "tasks"]:
            tid = parts[2]
            if method == "GET" and len(parts) == 3:
                return store.get(tid)
            if method == "GET" and parts[3:] == ["events"]:
                return {"events": store.events(tid)}
            if method == "POST" and parts[3:] == ["cancel"]:
                return store.cancel(tid)
            if method == "POST" and parts[3:] == ["retry"]:
                return store.retry(tid, self.request_key())
        if method == "POST" and path == "/v1/worker/register":
            for key in ("session_id", "name", "pool", "runtime_version"):
                if not isinstance(data.get(key), str) or not 1 <= len(data[key]) <= 200:
                    raise Invalid(f"invalid {key}")
            for key in ("slots", "cpu", "memory_mb"):
                integer(data.get(key), key, 1, 1048576)
            return store.register(**data)
        if method == "POST" and path == "/v1/worker/heartbeat":
            return store.heartbeat(**data)
        if method == "POST" and path == "/v1/worker/claim":
            if not isinstance(data.get("claim_id"), str) or not 1 <= len(data["claim_id"]) <= 200:
                raise Invalid("invalid claim_id")
            return {"assignment": store.claim(**data)}
        if method == "POST" and path == "/v1/worker/renew":
            return store.renew(**data)
        if method == "POST" and path == "/v1/worker/complete":
            return store.complete(**data)
        raise Missing("unknown endpoint")

    def request_key(self):
        key = self.headers.get("Idempotency-Key", "")
        if not 1 <= len(key) <= 200:
            raise Invalid("Idempotency-Key header is required (1..200 characters)")
        return key

    def do_GET(self):
        self.dispatch("GET")

    def do_POST(self):
        self.dispatch("POST")


def serve(dsn, host, port, data_root, admin_token, worker_token, insecure=False, lease=30):
    store = Store(dsn, lease)
    store.migrate()
    server = Server((host, port), store, data_root, admin_token, worker_token, insecure)
    import signal

    def stop(*_):
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        server.serve_forever()
    finally:
        server.server_close()
