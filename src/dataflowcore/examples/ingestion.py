"""UTF-8 document -> chunk -> embedding -> immutable, idempotent version index.

The deterministic embedding is a reproducible test fixture, not a semantic model.
Set embedding_url to a service implementing POST {text} -> {vector: [numbers]}.
SQLite is a local acceptance sink; production should implement the same keys in its DB.
"""

import hashlib
import json
import math
import sqlite3
import urllib.error
import urllib.request
from pathlib import Path

from dataflowcore.contracts import canonical, fingerprint, integer
from dataflowcore.runtime import PermanentError, file_hash, sleep_cooperatively, under_root
from dataflowcore.sdk import Pipeline


def pipeline(**defaults):
    return (
        Pipeline("document-ingestion", **defaults)
        .step("parse", parse)
        .step("chunk", chunk, depends_on=["parse"])
        .step("stats", statistics, depends_on=["parse"])
        .step("embed", embed, depends_on=["chunk"])
        .step("index", index, depends_on=["embed", "stats"])
        .step("receipt", receipt, depends_on=["index"])
    )


def records(path):
    with Path(path).open(encoding="utf-8") as stream:
        for line in stream:
            yield json.loads(line)


def parse(context, inputs):
    target = context.output_dir / "document.txt"
    chars = 0
    try:
        with (
            context.input_path.open(encoding="utf-8") as source,
            target.open("w", encoding="utf-8") as dest,
        ):
            while block := source.read(32768):
                context.check_cancelled()
                dest.write(block)
                chars += len(block)
                context.report(chars, message="parsed characters")
    except UnicodeError as exc:
        raise PermanentError("reference parser accepts UTF-8 text/Markdown only") from exc
    if chars == 0:
        raise PermanentError("empty document")
    return {"path": str(target), "characters": chars}


def chunk(context, inputs):
    size = integer(context.parameters.get("chunk_size", 512), "chunk_size", 16, 32768)
    overlap = integer(context.parameters.get("overlap", 32), "overlap", 0, size - 1)
    target = context.output_dir / "chunks.jsonl"
    count, offset, buffer = 0, 0, ""
    with (
        Path(inputs["parse"]["path"]).open(encoding="utf-8") as source,
        target.open("w", encoding="utf-8") as dest,
    ):
        while offset < inputs["parse"]["characters"]:
            while len(buffer) < size:
                block = source.read(32768)
                if not block:
                    break
                buffer += block
            context.check_cancelled()
            text = buffer[:size]
            dest.write(canonical({"index": count, "offset": offset, "text": text}) + "\n")
            count += 1
            context.report(count, message="document chunks")
            if offset + len(text) >= inputs["parse"]["characters"]:
                break
            buffer = buffer[size - overlap :]
            offset += size - overlap
    return {"path": str(target), "count": count}


def statistics(context, inputs):
    lines, words, in_word, last = 0, 0, False, ""
    with Path(inputs["parse"]["path"]).open(encoding="utf-8") as stream:
        while block := stream.read(32768):
            context.check_cancelled()
            lines += block.count("\n")
            for char in block:
                whitespace = char.isspace()
                if not whitespace and not in_word:
                    words += 1
                in_word = not whitespace
            last = block[-1]
    lines += last != "\n"
    return {"lines": lines, "words": words, "characters": inputs["parse"]["characters"]}


def fixture_vector(text, dimensions=32, rounds=0):
    # rounds supplies deterministic pure-Python CPU work for thread benchmarks.
    values = list(hashlib.sha256(text.encode()).digest())
    for _ in range(rounds):
        values = [((x * 1664525 + i * 1013904223) & 0xFFFFFFFF) for i, x in enumerate(values)]
    vector = [(values[i % len(values)] % 257 - 128) / 128 for i in range(dimensions)]
    norm = math.sqrt(sum(v * v for v in vector)) or 1
    return [v / norm for v in vector]


def embed(context, inputs):
    workers = integer(context.parameters.get("embedding_workers", 4), "embedding_workers", 1, 64)
    rounds = integer(context.parameters.get("cpu_rounds", 0), "cpu_rounds", 0, 1000000)
    url = context.parameters.get("embedding_url")

    def encode(row):
        context.check_cancelled()
        if url:
            request = urllib.request.Request(
                url,
                canonical({"text": row["text"]}).encode(),
                {"Content-Type": "application/json"},
            )
            try:
                with urllib.request.urlopen(request, timeout=5) as response:
                    payload = response.read(1_000_001)
                    if len(payload) > 1_000_000:
                        raise PermanentError("embedding response too large")
            except urllib.error.HTTPError as exc:
                if 400 <= exc.code < 500 and exc.code != 429:
                    raise PermanentError(
                        f"embedding service rejected input: HTTP {exc.code}"
                    ) from exc
                raise
            try:
                vector = json.loads(payload)["vector"]
            except (KeyError, ValueError, TypeError) as exc:
                raise PermanentError("invalid embedding response") from exc
        else:
            vector = fixture_vector(row["text"], rounds=rounds)
        if (
            not isinstance(vector, list)
            or not 1 <= len(vector) <= 4096
            or any(type(v) not in (int, float) or not math.isfinite(v) for v in vector)
        ):
            raise PermanentError("embedding must be a finite numeric vector")
        return {**row, "vector": vector}

    target = context.output_dir / "vectors.jsonl"
    with target.open("w", encoding="utf-8") as out:
        for count, row in enumerate(
            context.map(encode, records(inputs["chunk"]["path"]), max_workers=workers), 1
        ):
            out.write(canonical(row) + "\n")
            context.report(count, inputs["chunk"]["count"], "embedded chunks")
    return {"path": str(target), "count": inputs["chunk"]["count"]}


def index(context, inputs):
    root = context.output_dir.parents[2]
    target = under_root(context.parameters.get("index_path", root / "index.sqlite"), root)
    document = context.parameters.get("document_id", str(context.input_path))
    if not isinstance(document, str) or not document or len(document) > 4096:
        raise PermanentError("invalid document_id")
    input_sha256 = file_hash(context.input_path)
    version = fingerprint(
        {
            "input_sha256": input_sha256,
            "chunk_size": context.parameters.get("chunk_size", 512),
            "overlap": context.parameters.get("overlap", 32),
            "embedding_profile": context.parameters.get("embedding_profile", "fixture-sha256-v1"),
            "cpu_rounds": context.parameters.get("cpu_rounds", 0),
        }
    )
    conn = sqlite3.connect(target, timeout=15)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS chunks (document TEXT, version TEXT, ordinal INTEGER, "
            "payload TEXT NOT NULL, PRIMARY KEY(document, version, ordinal))"
        )
        conn.execute("BEGIN IMMEDIATE")
        for row in records(inputs["embed"]["path"]):
            context.check_cancelled()
            cursor = conn.execute(
                "INSERT INTO chunks VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (document, version, row["index"], canonical(row)),
            )
            if cursor.rowcount == 0:
                old = conn.execute(
                    "SELECT payload FROM chunks WHERE document = ? AND version = ? AND ordinal = ?",
                    (document, version, row["index"]),
                ).fetchone()
                if old[0] != canonical(row):
                    raise PermanentError(
                        "index version conflict; use a new embedding_profile for a new model"
                    )
        context.check_cancelled()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "document_id": document,
        "version": version,
        "input_sha256": input_sha256,
        "chunks": inputs["embed"]["count"],
        "statistics": inputs["stats"],
    }


def receipt(context, inputs):
    # Allows fault injection after the external side effect committed, before task completion.
    sleep_cooperatively(context, context.parameters.get("receipt_delay", 0))
    target = context.output_dir / "receipt.json"
    target.write_text(canonical(inputs["index"]), encoding="utf-8")
    return {"path": str(target), **inputs["index"]}
