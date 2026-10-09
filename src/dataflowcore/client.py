"""Dependency-free API client, also used by executors."""

import json
import time
import urllib.error
import urllib.request
import uuid
from urllib.parse import urlencode

from .contracts import TERMINAL, Invalid, canonical, integer


class APIError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class Client:
    def __init__(self, url, token="", timeout=5):
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout

    def request(self, method, path, payload=None, key=None, timeout=None):
        data = canonical(payload).encode() if payload is not None else None
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.token}"}
        if key:
            headers["Idempotency-Key"] = key
        request = urllib.request.Request(self.url + path, data, headers, method=method)
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout if timeout is None else timeout
            ) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                message = json.load(exc).get("error", str(exc))
            except ValueError, AttributeError:
                message = str(exc)
            raise APIError(exc.code, message) from exc

    def submit(self, spec, key=None):
        return self.request("POST", "/v1/tasks", spec, key or str(uuid.uuid4()))

    def get(self, task_id):
        return self.request("GET", f"/v1/tasks/{task_id}")

    def cancel(self, task_id):
        return self.request("POST", f"/v1/tasks/{task_id}/cancel", {})

    def retry(self, task_id, key=None):
        return self.request("POST", f"/v1/tasks/{task_id}/retry", {}, key or str(uuid.uuid4()))

    def statuses(self, task_ids, *, timeout=None):
        return self.request(
            "POST", "/v1/tasks/status", {"task_ids": list(task_ids)}, timeout=timeout
        )

    def iter_tasks(self, *, state=None, pool=None, page_size=100, summary=True):
        integer(page_size, "page_size", 1, 100)
        cursor = None
        while True:
            params = {"limit": page_size, "summary": int(summary)}
            params.update(
                {
                    k: v
                    for k, v in {"state": state, "pool": pool, "cursor": cursor}.items()
                    if v is not None
                }
            )
            page = self.request("GET", "/v1/tasks?" + urlencode(params))
            yield from page["tasks"]
            cursor = page["next_cursor"]
            if cursor is None:
                break

    def wait_many(self, task_ids, *, timeout=3600, interval=0.5):
        ids = list(dict.fromkeys(task_ids))
        if timeout <= 0 or interval <= 0:
            raise Invalid("timeout and interval must be positive")
        deadline, completed = time.monotonic() + timeout, {}
        while len(completed) < len(ids):
            pending = [t for t in ids if t not in completed]
            for start in range(0, len(pending), 100):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("waiting for tasks timed out")
                try:
                    page = self.statuses(
                        pending[start : start + 100], timeout=min(self.timeout, remaining)
                    )
                except APIError as exc:
                    if exc.status not in (429, 500, 502, 503, 504):
                        raise
                    break
                except OSError, TimeoutError:
                    break
                if page["missing"]:
                    raise APIError(404, f"unknown tasks: {page['missing']}")
                for task in page["tasks"]:
                    if task["state"] in TERMINAL:
                        completed[task["id"]] = task
            if len(completed) == len(ids):
                return [completed[t] for t in ids]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("waiting for tasks timed out")
            time.sleep(min(interval, remaining))
        return []

    def drain(self, session_id):
        return self.request("POST", f"/v1/workers/{session_id}/drain", {})
