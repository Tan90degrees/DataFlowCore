"""Explicit, bounded cleanup of old failed attempts; successful outputs and inputs stay."""

import os
import shutil
import time
import uuid
from pathlib import Path

from .contracts import Invalid, number


def clean_failed_outputs(store, data_root, days=7, limit=200, apply=False):
    number(days, "retention days", 1, 36500)
    root = Path(data_root).resolve()
    outputs = root / "outputs"
    if outputs.is_symlink():
        raise Invalid("output root must not be a symlink")
    report = {"dry_run": not apply, "retention_days": days, "candidates": []}
    for attempt in store.output_cleanup_candidates(max(0, time.time() - days * 86400), limit):
        tid, aid = attempt["task_id"], attempt["id"]
        if str(uuid.UUID(tid)) != tid or str(uuid.UUID(aid)) != aid:
            raise Invalid("non-canonical task/attempt identity in cleanup metadata")
        parent, path = outputs / tid, outputs / tid / aid
        item = {"task_id": tid, "attempt_id": aid, "path": str(path)}
        report["candidates"].append(item)
        if parent.is_symlink() or path.is_symlink():
            item["status"] = "protected_symlink"
        elif store.output_referenced(path):
            item["status"] = "protected_reference"
        elif not path.exists():
            item["status"] = "absent"
            if apply:
                store.mark_outputs_cleaned(aid, False)
        elif not path.is_dir():
            item["status"] = "protected_non_directory"
        elif not apply:
            item["status"] = "eligible"
        else:
            # Operate relative to an opened parent directory, never through a
            # changed symlink. shutil.rmtree itself uses fd-based traversal.
            root_fd = os.open(outputs, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                fd = os.open(tid, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
            finally:
                os.close(root_fd)
            try:
                try:
                    shutil.rmtree(aid, dir_fd=fd)
                except FileNotFoundError:
                    pass  # A concurrent cleanup is idempotent.
            finally:
                os.close(fd)
            store.mark_outputs_cleaned(aid, True)
            item["status"] = "removed"
    return report
