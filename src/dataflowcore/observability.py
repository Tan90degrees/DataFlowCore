"""Process-local HTTP metrics with fixed labels and no task/file identifiers."""

import json
import logging
import threading
from collections import defaultdict

BUCKETS = (0.01, 0.05, 0.1, 0.5, 1, 5, 10)


def route_label(path):
    if path in ("/healthz", "/readyz", "/version", "/metrics"):
        return path[1:]
    parts = path.strip("/").split("/")
    if parts[:2] == ["v1", "worker"]:
        return (
            "worker_" + parts[2]
            if len(parts) == 3
            and parts[2] in {"register", "heartbeat", "claim", "renew", "complete"}
            else "unknown"
        )
    if parts[:2] == ["v1", "tasks"]:
        if len(parts) == 2:
            return "tasks"
        if len(parts) == 3:
            return "task_status" if parts[2] == "status" else "task"
        if len(parts) == 4 and parts[3] in ("events", "cancel", "retry"):
            return "task_" + parts[3]
    return {
        "/v1/overview": "overview",
        "/v1/control": "control",
        "/v1/audit": "audit",
        "/v1/files": "files",
        "/v1/files/limits": "file_limits",
        "/v1/workers": "workers",
        "/v1/dags/validate": "validate",
    }.get(
        path,
        "worker_drain"
        if len(parts) == 4 and parts[:2] == ["v1", "workers"] and parts[3] == "drain"
        else "unknown",
    )


class HTTPMetrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.counts = defaultdict(int)
        self.duration = defaultdict(lambda: [0, 0.0, [0] * len(BUCKETS)])

    def observe(self, method, path, status, duration):
        method = method if method in ("GET", "POST", "OPTIONS") else "OTHER"
        route = route_label(path)
        with self.lock:
            self.counts[(method, route, str(status // 100) + "xx")] += 1
            value = self.duration[(method, route)]
            value[0] += 1
            value[1] += duration
            for index, boundary in enumerate(BUCKETS):
                value[2][index] += duration <= boundary

    def render(self):
        lines = [
            "# TYPE dataflow_http_requests_total counter",
            "# TYPE dataflow_http_request_duration_seconds histogram",
        ]
        with self.lock:
            for (method, route, status), count in sorted(self.counts.items()):
                lines.append(
                    f'dataflow_http_requests_total{{method="{method}",route="{route}",'
                    f'status="{status}"}} {count}'
                )
            for (method, route), (count, total, buckets) in sorted(self.duration.items()):
                labels = f'method="{method}",route="{route}"'
                for boundary, value in zip(BUCKETS, buckets, strict=True):
                    lines.append(
                        f"dataflow_http_request_duration_seconds_bucket{{{labels},"
                        f'le="{boundary}"}} {value}'
                    )
                lines.extend(
                    [
                        f"dataflow_http_request_duration_seconds_bucket"
                        f'{{{labels},le="+Inf"}} {count}',
                        f"dataflow_http_request_duration_seconds_count{{{labels}}} {count}",
                        f"dataflow_http_request_duration_seconds_sum{{{labels}}} {total}",
                    ]
                )
        return "\n".join(lines) + "\n"


class JSONFormatter(logging.Formatter):
    def format(self, record):
        value = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            value["exception"] = self.formatException(record.exc_info)
        return json.dumps(value, ensure_ascii=False)
