"""Bounded uploads with atomic file + metadata publication on the shared volume."""

import errno
import hashlib
import json
import os
import re
import shutil
import threading
import time
import uuid
from pathlib import Path

from .contracts import Conflict, Invalid, Missing, integer, number
from .runtime import atomic_json, under_root

DEFAULT_MAX_BYTES = 256 * 1024 * 1024
CHUNK_BYTES = 1024 * 1024


class UploadError(Invalid):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def filename(value):
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > 255
        or value in (".", "..")
        or "/" in value
        or "\\" in value
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise Invalid(
            "filename must be a basename of 1..255 UTF-8 bytes without control characters"
        )
    return value


def sync_directory(path):
    if os.name == "posix":
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class Uploads:
    """Single-controller writer; workers only read fully published inputs.

    File metadata is published in the same rename as its source bytes. It therefore
    survives controller restart without a database/filesystem dual-write window.
    """

    def __init__(self, data_root, max_bytes=DEFAULT_MAX_BYTES, concurrency=4, timeout=300):
        integer(max_bytes, "upload_max_bytes", 0, 1024**4)
        integer(concurrency, "upload_concurrency", 1, 32)
        number(timeout, "upload_timeout", 1, 86400)
        self.data_root = Path(data_root).resolve()
        self.root = under_root(self.data_root / "uploads", self.data_root)
        self.staging = under_root(self.root / ".staging", self.root)
        self.max_bytes, self.concurrency, self.timeout = max_bytes, concurrency, timeout
        self.slots = threading.BoundedSemaphore(concurrency)
        self.lock = threading.Lock()
        self.active = set()
        if max_bytes:
            self.staging.mkdir(parents=True, mode=0o700, exist_ok=True)
            # A single controller owns this directory. No task can see staging inputs.
            for entry in self.staging.iterdir():
                if re.fullmatch(r"[0-9a-f]{32}", entry.name):
                    if entry.is_symlink() or entry.is_file():
                        entry.unlink()
                    else:
                        shutil.rmtree(entry)

    def limits(self):
        return {
            "enabled": bool(self.max_bytes),
            "max_bytes": self.max_bytes,
            "max_concurrent": self.concurrency,
            "timeout_seconds": self.timeout,
        }

    @staticmethod
    def identity(key):
        return hashlib.sha256(key.encode()).hexdigest()

    def get(self, key):
        folder = under_root(self.root / self.identity(key), self.root)
        try:
            metadata = json.loads((folder / "metadata.json").read_text())
        except FileNotFoundError as exc:
            raise Missing("upload has not completed") from exc
        # Construct the path locally instead of trusting a path stored in metadata.
        source = under_root(folder / "source" / filename(metadata["filename"]), folder)
        if not source.is_file() or source.stat().st_size != metadata["size_bytes"]:
            raise UploadError(503, "uploaded source is missing or changed")
        return {**metadata, "input_path": str(source)}

    def receive(self, key, name, size, stream, connection):
        filename(name)
        if not self.max_bytes:
            raise UploadError(403, "file uploads are disabled")
        if size < 0:
            raise Invalid("invalid content length")
        if size > self.max_bytes:
            raise UploadError(413, f"file exceeds upload limit of {self.max_bytes} bytes")
        with self.lock:
            if key in self.active:
                raise Conflict("upload with this key is still in progress; retry after it finishes")
            if not self.slots.acquire(blocking=False):
                raise UploadError(429, "upload slots are busy; retry later")
            self.active.add(key)
        stage = None
        original_timeout = connection.gettimeout()
        try:
            try:
                existing = self.get(key)
            except Missing:
                existing = None
            if existing and (existing["filename"] != name or existing["size_bytes"] != size):
                raise Conflict("idempotency key reused with a different file")
            if not existing:
                stage = under_root(self.staging / uuid.uuid4().hex, self.staging)
                source_dir = stage / "source"
                source_dir.mkdir(parents=True, mode=0o750)
                target = (source_dir / name).open("xb")
            else:
                target = None
            digest = hashlib.sha256()
            remaining, deadline = size, time.monotonic() + self.timeout
            try:
                while remaining:
                    budget = deadline - time.monotonic()
                    if budget <= 0:
                        raise UploadError(408, "upload timed out; retry the whole file")
                    connection.settimeout(min(30, budget))
                    block = stream.read1(min(CHUNK_BYTES, remaining))
                    if not block:
                        raise UploadError(400, "incomplete upload; retry the whole file")
                    if target:
                        target.write(block)
                    digest.update(block)
                    remaining -= len(block)
                if target:
                    target.flush()
                    os.fsync(target.fileno())
            finally:
                if target:
                    target.close()
            if time.monotonic() > deadline:
                raise UploadError(408, "upload timed out; retry the whole file")
            checksum = digest.hexdigest()
            if existing:
                if existing["input_sha256"] != checksum:
                    raise Conflict("idempotency key reused with different file bytes")
                return 200, existing
            result = {
                "id": self.identity(key),
                "filename": name,
                "size_bytes": size,
                "input_sha256": checksum,
                "created_at": time.time(),
            }
            (source_dir / name).chmod(0o440)
            atomic_json(stage / "metadata.json", result)
            sync_directory(source_dir)
            sync_directory(stage)
            if time.monotonic() > deadline:
                raise UploadError(408, "upload timed out; retry the whole file")
            final = under_root(self.root / result["id"], self.root)
            os.rename(stage, final)
            stage = None
            sync_directory(self.root)
            return 201, {**result, "input_path": str(final / "source" / name)}
        except TimeoutError as exc:
            raise UploadError(408, "upload timed out; retry the whole file") from exc
        except OSError as exc:
            if exc.errno in (errno.ENOSPC, errno.EDQUOT):
                raise UploadError(507, "shared upload storage is full") from exc
            raise UploadError(
                503, "upload storage or connection is unavailable; retry later"
            ) from exc
        finally:
            connection.settimeout(original_timeout)
            if stage:
                shutil.rmtree(stage, ignore_errors=True)
            with self.lock:
                self.active.remove(key)
                self.slots.release()
