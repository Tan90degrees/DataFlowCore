"""JSON-only contracts: business code is installed in an immutable worker image."""

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import __version__

TERMINAL = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})
ACTIVE = frozenset({"RUNNING", "STOPPING"})
SYMBOL = re.compile(
    r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+:[A-Za-z_]\w*$|"
    r"^[A-Za-z_]\w*:[A-Za-z_]\w*$"
)


class Invalid(ValueError):
    pass


class Conflict(Exception):
    pass


class Missing(Exception):
    pass


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def integer(value: Any, name: str, low: int, high: int) -> int:
    if type(value) is not int or not low <= value <= high:
        raise Invalid(f"{name} must be an integer in [{low}, {high}]")
    return value


def number(value: Any, name: str, low: float, high: float) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or not low <= value <= high:
        raise Invalid(f"{name} must be a finite number in [{low}, {high}]")
    return float(value)


def keys(value: dict, allowed: set[str]) -> None:
    if not isinstance(value, dict):
        raise Invalid("expected a JSON object")
    unknown = set(value) - allowed
    if unknown:
        raise Invalid(f"unknown fields: {sorted(unknown)}")


@dataclass(frozen=True)
class TaskSpec:
    input_path: str
    steps: list[dict]
    name: str = "file-task"
    parameters: dict = field(default_factory=dict)
    pool: str = "default"
    dag_workers: int = 2
    cpu: int = 1
    memory_mb: int = 256
    max_attempts: int = 3
    retry_delay: float = 2.0
    timeout: float = 3600.0
    runtime_version: str = __version__
    input_sha256: str | None = None

    @classmethod
    def parse(cls, data: dict) -> TaskSpec:
        keys(data, set(cls.__dataclass_fields__))
        try:
            spec = cls(**data)
        except TypeError as exc:
            raise Invalid(str(exc)) from exc
        for name in ("name", "pool", "runtime_version", "input_path"):
            value = getattr(spec, name)
            if not isinstance(value, str) or not value or len(value) > 4096:
                raise Invalid(f"invalid {name}")
        if not Path(spec.input_path).is_absolute():
            raise Invalid("input_path must be an absolute shared-filesystem path")
        if spec.input_sha256 is not None and not re.fullmatch(r"[0-9a-f]{64}", spec.input_sha256):
            raise Invalid("input_sha256 must be a lowercase SHA256")
        if not isinstance(spec.parameters, dict):
            raise Invalid("parameters must be an object")
        integer(spec.dag_workers, "dag_workers", 1, 256)
        integer(spec.cpu, "cpu", 1, 1024)
        integer(spec.memory_mb, "memory_mb", 1, 1048576)
        integer(spec.max_attempts, "max_attempts", 1, 100)
        number(spec.retry_delay, "retry_delay", 0, 86400)
        number(spec.timeout, "timeout", 0.1, 604800)
        if not isinstance(spec.steps, list) or not 1 <= len(spec.steps) <= 1000:
            raise Invalid("steps must contain 1..1000 operators")
        ids = set()
        for step in spec.steps:
            keys(step, {"id", "callable", "depends_on", "parameters"})
            sid = step.get("id")
            if not isinstance(sid, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", sid):
                raise Invalid("invalid step id")
            if sid in ids:
                raise Invalid("duplicate step id")
            ids.add(sid)
            if not isinstance(step.get("callable"), str) or not SYMBOL.fullmatch(step["callable"]):
                raise Invalid("callable must be an importable module:symbol")
            deps = step.get("depends_on", [])
            if not isinstance(deps, list) or any(not isinstance(d, str) for d in deps):
                raise Invalid("depends_on must be a list of step ids")
            if len(set(deps)) != len(deps) or sid in deps:
                raise Invalid("duplicate/self dependency")
            if not isinstance(step.get("parameters", {}), dict):
                raise Invalid("step parameters must be an object")
        resolved = set()
        while len(resolved) < len(ids):
            ready = {
                s["id"]
                for s in spec.steps
                if s["id"] not in resolved and set(s.get("depends_on", [])) <= resolved
            }
            if not ready:
                raise Invalid("DAG has a cycle or unknown dependency")
            resolved |= ready
        if len(canonical(data)) > 1_000_000:
            raise Invalid("task specification too large")
        return spec

    def json(self) -> dict:
        from dataclasses import asdict

        return asdict(self)
