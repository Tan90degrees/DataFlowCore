"""Authenticated HTTP control plane. Worker requests never execute business code."""

import hashlib
import json
import logging
import re
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from . import __version__
from .contracts import APIBusy, Backpressure, Conflict, Invalid, Missing, Paused, TaskSpec, integer
from .control_lock import ControlLock, ControlLost
from .observability import HTTPMetrics
from .runtime import under_root
from .security import Credentials, request_actor
from .store import Store
from .uploads import DEFAULT_DIR, DEFAULT_MAX_BYTES, UploadError, Uploads

log = logging.getLogger(__name__)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64

    def __init__(
        self,
        address,
        store,
        data_root,
        admin_token,
        worker_token,
        insecure=False,
        reap_interval=1,
        cors_origins=(),
        upload_max_bytes=DEFAULT_MAX_BYTES,
        upload_concurrency=4,
        upload_timeout=300,
        upload_dir=DEFAULT_DIR,
        viewer_tokens=(),
        admin_tokens=(),
        worker_tokens=(),
        operator_modules=(),
        upload_min_free_bytes=0,
        ownership=None,
        management_concurrency=8,
    ):
        self.credentials = Credentials(
            ((admin_token,) if admin_token else ()) + tuple(admin_tokens),
            ((worker_token,) if worker_token else ()) + tuple(worker_tokens),
            viewer_tokens,
            insecure,
        )
        self.store, self.data_root = store, data_root
        self.admin_token, self.worker_token, self.insecure = admin_token, worker_token, insecure
        self.ownership = ownership
        self.ownership_healthy = True
        self.last_reconciliation = time.time()
        self.metrics = HTTPMetrics()
        integer(management_concurrency, "management concurrency", 1, 32)
        self.management_slots = threading.BoundedSemaphore(management_concurrency)
        self.operator_modules = tuple(operator_modules)
        if any(
            not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*", m) for m in self.operator_modules
        ):
            raise Invalid("operator modules must be Python module prefixes")
        self.reap_interval = reap_interval
        self.cors_origins = frozenset(cors_origins)
        for origin in self.cors_origins:
            url = urlsplit(origin)
            if (
                "*" in origin
                or url.scheme not in ("http", "https")
                or not url.netloc
                or url.username
                or url.password
                or url.path
                or url.query
                or url.fragment
            ):
                raise Invalid("CORS origins must be exact http(s) origins without paths")
        self.stopped = threading.Event()
        self.slots = threading.BoundedSemaphore(64)
        super().__init__(address, Handler)
        try:
            self.uploads = Uploads(
                data_root,
                upload_max_bytes,
                upload_concurrency,
                upload_timeout,
                upload_dir=upload_dir,
                min_free_bytes=upload_min_free_bytes,
                admission=self.upload_admission,
            )
        except BaseException:
            self.server_close()
            raise

    def upload_admission(self):
        if not self.ownership_healthy or self.stopped.is_set():
            raise Paused("controller is not accepting writes")
        if self.store.control()["paused"]:
            raise Paused("file uploads are paused for maintenance")

    def operational_state(self):
        result = self.store.control()
        with self.uploads.lock:
            result["active_uploads"] = len(self.uploads.active)
        result["quiescent"] = (
            result["paused"] and result["active_attempts"] == 0 and result["active_uploads"] == 0
        )
        result["operator_modules"] = self.operator_modules
        result["reconciliation_last_success_at"] = self.last_reconciliation
        return result

    def validate(self, data):
        spec = TaskSpec.parse(data)
        if self.operator_modules and any(
            not any(
                s["callable"].partition(":")[0] == prefix
                or s["callable"].partition(":")[0].startswith(prefix + ".")
                for prefix in self.operator_modules
            )
            for s in spec.steps
        ):
            raise UploadError(403, "operator module is not allowed by this controller")
        return spec

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
                if self.ownership is not None:
                    self.ownership.check()
                count = self.store.reap()
                self.last_reconciliation = time.time()
                self.ownership_healthy = True
                if count:
                    log.info("recovered_expired_attempts count=%s", count)
            except ControlLost:
                self.ownership_healthy = False
                log.exception("controller_ownership_lost")
                threading.Thread(target=self.shutdown, daemon=True).start()
                return
            except Exception:
                self.ownership_healthy = False
                log.exception("reconciliation_failed")


class Handler(BaseHTTPRequestHandler):
    server_version = "DataFlowCore/" + __version__

    def setup(self):
        super().setup()
        self.connection.settimeout(10)

    def handle_one_request(self):
        self.request_id = uuid.uuid4().hex
        self.response_code = 0
        started = time.monotonic()
        try:
            super().handle_one_request()
        finally:
            if self.response_code:
                self.server.metrics.observe(
                    getattr(self, "command", "OTHER"),
                    urlsplit(getattr(self, "path", "")).path,
                    self.response_code,
                    time.monotonic() - started,
                )

    def send_response(self, code, message=None):
        self.response_code = code
        super().send_response(code, message)
        self.send_header("X-Request-ID", self.request_id)

    def log_message(self, fmt, *args):
        log.info(
            "http request_id=%s peer=%s %s", self.request_id, self.client_address[0], fmt % args
        )

    def end_headers(self):
        origin = self.headers.get("Origin")
        self.send_header("Vary", "Origin")
        if origin in self.server.cors_origins:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Expose-Headers", "X-Request-ID, Retry-After")
        super().end_headers()

    def send_json(self, status, value):
        data = json.dumps(value, allow_nan=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def body(self):
        lengths = self.headers.get_all("Content-Length", [])
        if (
            len(lengths) != 1
            or not lengths[0].isascii()
            or not lengths[0].isdigit()
            or self.headers.get("Transfer-Encoding")
        ):
            raise Invalid("JSON requires one valid Content-Length, without Transfer-Encoding")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise Invalid("invalid content length") from exc
        if not 0 < length <= 1_100_000:
            raise Invalid("body must be a JSON object of at most 1.1 MB")
        try:
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise Invalid("incomplete JSON body")
            value = json.loads(raw)
        except (ValueError, UnicodeError) as exc:
            raise Invalid("invalid JSON") from exc
        if not isinstance(value, dict):
            raise Invalid("expected JSON object")
        return value

    def dispatch(self, method):
        path = urlsplit(self.path).path
        actor_context = None
        management_acquired = False
        try:
            supplied_id = self.headers.get("X-Request-ID", "")
            if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", supplied_id):
                self.request_id = supplied_id
            origin = self.headers.get("Origin")
            if origin and origin not in self.server.cors_origins:
                self.send_json(403, {"error": "browser origin is not allowed"})
                return
            if method == "GET" and path in ("/healthz", "/version"):
                self.send_json(200, {"ok": True, "version": __version__})
                return
            if method == "GET" and path == "/readyz":
                if (
                    not self.server.ownership_healthy
                    or self.server.stopped.is_set()
                    or time.time() - self.server.last_reconciliation
                    > max(5, self.server.reap_interval * 3)
                ):
                    raise Paused("controller reconciliation is not healthy")
                self.server.store.ping()
                self.send_json(200, {"ready": True})
                return
            actor = self.server.credentials.authenticate(
                self.headers.get("Authorization", ""), path.startswith("/v1/worker/")
            )
            if actor is None:
                self.send_json(401, {"error": "authentication required"})
                return
            self.role = actor["role"]
            actor_context = request_actor.set({**actor, "request_id": self.request_id})
            if self.role != "worker":
                management_acquired = self.server.management_slots.acquire(blocking=False)
                if not management_acquired:
                    raise APIBusy("management requests are busy; retry later")
            if (
                self.role == "viewer"
                and method == "POST"
                and path not in ("/v1/tasks/status", "/v1/dags/validate")
            ):
                self.send_json(
                    403, {"error": "read-only credential cannot change state", "code": "read_only"}
                )
                return
            if method == "POST" and (
                not self.server.ownership_healthy or self.server.stopped.is_set()
            ):
                raise Paused("controller is not accepting writes")
            if method == "GET" and path == "/metrics":
                telemetry = self.server.store.telemetry()
                data = (
                    self.server.metrics.render()
                    + "# TYPE dataflow_tasks gauge\n"
                    + "".join(
                        f'dataflow_tasks{{state="{state}"}} {telemetry["tasks"].get(state, 0)}\n'
                        for state in (
                            "QUEUED",
                            "RUNNING",
                            "STOPPING",
                            "SUCCEEDED",
                            "FAILED",
                            "CANCELLED",
                        )
                    )
                    + "# TYPE dataflow_attempts gauge\n"
                    + "".join(
                        f'dataflow_attempts{{state="{state}"}} '
                        f"{telemetry['attempts'].get(state, 0)}\n"
                        for state in (
                            "RUNNING",
                            "STOPPING",
                            "SUCCEEDED",
                            "FAILED",
                            "CANCELLED",
                            "LOST",
                        )
                    )
                    + "".join(
                        f"# TYPE dataflow_{key} gauge\ndataflow_{key} {telemetry[key]}\n"
                        for key in (
                            "oldest_queue_seconds",
                            "online_workers",
                            "worker_slots",
                            "paused",
                        )
                    )
                    + "# TYPE dataflow_reconciliation_last_success_timestamp_seconds gauge\n"
                    + "dataflow_reconciliation_last_success_timestamp_seconds "
                    + f"{self.server.last_reconciliation}\n"
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            if method == "POST" and path == "/v1/files":
                names = parse_qs(urlsplit(self.path).query, keep_blank_values=True).get(
                    "filename", []
                )
                if len(names) != 1:
                    raise Invalid("provide exactly one filename query parameter")
                if self.headers.get_content_type() != "application/octet-stream":
                    raise UploadError(415, "upload Content-Type must be application/octet-stream")
                lengths = self.headers.get_all("Content-Length", [])
                if not lengths:
                    raise UploadError(411, "upload Content-Length is required")
                if (
                    len(lengths) != 1
                    or not lengths[0].isascii()
                    or not lengths[0].isdigit()
                    or self.headers.get("Transfer-Encoding")
                ):
                    raise Invalid(
                        "upload requires one valid Content-Length, without Transfer-Encoding"
                    )
                status, result = self.server.uploads.receive(
                    self.request_key(), names[0], int(lengths[0]), self.rfile, self.connection
                )
                self.send_json(status, result)
                return
            result = self.route(method, path, self.body() if method == "POST" else {})
            self.send_json(200, result)
        except Backpressure as exc:
            self.send_response(exc.status)
            self.send_header("Retry-After", "5")
            data = json.dumps({"error": str(exc), "code": exc.code}).encode()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
        except UploadError as exc:
            self.send_json(exc.status, {"error": str(exc)})
        except Missing as exc:
            self.send_json(404, {"error": str(exc)})
        except Conflict as exc:
            self.send_json(409, {"error": str(exc)})
        except (Invalid, KeyError, TypeError, ValueError) as exc:
            self.send_json(400, {"error": str(exc)})
        except Exception:
            log.exception("request_failed path=%s request_id=%s", path, self.request_id)
            self.send_json(503, {"error": "service temporarily unavailable"})
        finally:
            if management_acquired:
                self.server.management_slots.release()
            if actor_context is not None:
                request_actor.reset(actor_context)

    def route(self, method, path, data):
        store = self.server.store
        parts = path.strip("/").split("/")
        if method == "GET" and path == "/v1/overview":
            return {**store.overview(), "version": __version__, "role": self.role}
        if method == "GET" and path == "/v1/control":
            return self.server.operational_state()
        if method == "POST" and path == "/v1/control":
            # Same lock as upload admission: pause cannot report quiescence while
            # an already accepted upload has not yet registered its reservation.
            with self.server.uploads.lock:
                store.set_paused(data.get("paused"))
            return self.server.operational_state()
        if method == "GET" and path == "/v1/audit":
            query = parse_qs(urlsplit(self.path).query)
            return store.audit_page(
                int(query.get("limit", ["100"])[0]), query.get("cursor", [None])[0]
            )
        if method == "GET" and path == "/v1/files/limits":
            return self.server.uploads.limits()
        if method == "GET" and path == "/v1/files":
            return self.server.uploads.get(self.request_key())
        if method == "POST" and path == "/v1/dags/validate":
            spec = self.server.validate(data)
            resolved, layers = set(), []
            while len(resolved) < len(spec.steps):
                layer = [
                    step["id"]
                    for step in spec.steps
                    if step["id"] not in resolved and set(step.get("depends_on", [])) <= resolved
                ]
                layers.append(layer)
                resolved.update(layer)
            return {"valid": True, "spec": spec.json(), "layers": layers}
        if method == "POST" and path == "/v1/tasks":
            spec = self.server.validate(data)
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
            query = parse_qs(urlsplit(self.path).query)
            summary = query.get("summary", ["1"])[0] == "1"
            limit = int(query.get("limit", ["100" if summary else "10"])[0])
            if not summary and limit > 10:
                raise Invalid("full task definitions are limited to 10; use summary=1 for paging")
            return store.task_page(
                limit=limit,
                cursor=query.get("cursor", [None])[0],
                state=query.get("state", [None])[0],
                pool=query.get("pool", [None])[0],
                summary=summary,
            )
        if method == "POST" and path == "/v1/tasks/status":
            return store.statuses(data.get("task_ids"))
        if method == "GET" and path == "/v1/workers":
            return {"workers": store.workers()}
        if method == "POST" and len(parts) == 4 and parts[:2] == ["v1", "workers"]:
            if parts[3] == "drain":
                return store.drain(parts[2])
        if len(parts) >= 3 and parts[:2] == ["v1", "tasks"]:
            tid = parts[2]
            if method == "GET" and len(parts) == 3:
                query = parse_qs(urlsplit(self.path).query)
                return store.get(tid, compact=True, attempt_id=query.get("attempt", [None])[0])
            if method == "GET" and parts[3:] == ["events"]:
                return {"events": store.events(tid)}
            if method == "POST" and parts[3:] == ["cancel"]:
                return store.cancel(tid)
            if method == "POST" and parts[3:] == ["retry"]:
                old = store.get(tid, compact=True)
                self.server.validate({"scheduling": old["scheduling"], **old["spec"]})
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

    def do_OPTIONS(self):
        origin = self.headers.get("Origin")
        path = urlsplit(self.path).path
        requested = {
            name.strip().lower()
            for name in self.headers.get("Access-Control-Request-Headers", "").split(",")
            if name.strip()
        }
        if (
            origin not in self.server.cors_origins
            or self.headers.get("Access-Control-Request-Method") not in ("GET", "POST")
            or not requested <= {"authorization", "content-type", "idempotency-key", "x-request-id"}
            or not path.startswith("/v1/")
            or path.startswith("/v1/worker/")
        ):
            self.send_json(403, {"error": "CORS preflight is not allowed"})
            return
        self.send_response(204)
        self.send_header("Access-Control-Allow-Methods", "GET, POST")
        self.send_header(
            "Access-Control-Allow-Headers",
            "Authorization, Content-Type, Idempotency-Key, X-Request-ID",
        )
        self.send_header("Access-Control-Max-Age", "600")
        self.send_header("Content-Length", "0")
        self.end_headers()


def serve(
    dsn,
    host,
    port,
    data_root,
    admin_token,
    worker_token,
    insecure=False,
    lease=30,
    cors_origins=(),
    upload_max_bytes=DEFAULT_MAX_BYTES,
    upload_concurrency=4,
    upload_timeout=300,
    upload_dir=DEFAULT_DIR,
    viewer_tokens=(),
    admin_tokens=(),
    worker_tokens=(),
    operator_modules=(),
    upload_min_free_bytes=0,
    max_unfinished=10000,
    db_timeout_ms=5000,
):
    store = Store(dsn, lease, max_unfinished, db_timeout_ms)
    ownership = ControlLock(store)
    try:
        store.migrate()
        server = Server(
            (host, port),
            store,
            data_root,
            admin_token,
            worker_token,
            insecure,
            cors_origins=cors_origins,
            upload_max_bytes=upload_max_bytes,
            upload_concurrency=upload_concurrency,
            upload_timeout=upload_timeout,
            upload_dir=upload_dir,
            viewer_tokens=viewer_tokens,
            admin_tokens=admin_tokens,
            worker_tokens=worker_tokens,
            operator_modules=operator_modules,
            upload_min_free_bytes=upload_min_free_bytes,
            ownership=ownership,
        )
    except BaseException:
        ownership.close()
        raise
    import signal

    def stop(*_):
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        ownership.close()
