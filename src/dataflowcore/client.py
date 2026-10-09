"""Dependency-free API client, also used by executors."""

import json
import urllib.error
import urllib.request
import uuid

from .contracts import canonical


class APIError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class Client:
    def __init__(self, url, token="", timeout=5):
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout

    def request(self, method, path, payload=None, key=None):
        data = canonical(payload).encode() if payload is not None else None
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.token}"}
        if key:
            headers["Idempotency-Key"] = key
        request = urllib.request.Request(self.url + path, data, headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
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
