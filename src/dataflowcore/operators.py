"""Small reproducible example operators; add business operators in your worker image."""

import json

from .runtime import PermanentError, sleep_cooperatively


def read_text(context, inputs):
    context.report(0, 1, "reading input")
    text = context.input_path.read_text(encoding=context.parameters.get("encoding", "utf-8"))
    context.report(1, 1)
    return {"text": text}


def word_count(context, inputs):
    text = next(iter(inputs.values()))["text"]
    counts = {}
    words = text.split()
    for i, word in enumerate(words):
        context.check_cancelled()
        counts[word] = counts.get(word, 0) + 1
        if i % 1000 == 0:
            context.report(i, len(words))
    context.report(len(words), len(words))
    return {"words": len(words), "counts": counts}


def write_json(context, inputs):
    path = context.output_dir / f"{context.step_id}.json"
    path.write_text(json.dumps(dict(inputs), ensure_ascii=False), encoding="utf-8")
    return {"path": str(path)}


def delay(context, inputs):
    sleep_cooperatively(context, float(context.parameters.get("seconds", 1)))
    return {"delayed": True}


def fail(context, inputs):
    raise PermanentError("example permanent failure")
