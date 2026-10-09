"""Python DAG authoring generates JSON; it never starts tasks or imports Ray."""

import inspect

from .client import Client
from .contracts import Invalid, TaskSpec


class Pipeline:
    def __init__(self, name, **defaults):
        self.name, self.defaults, self.steps = name, defaults, []

    def step(self, step_id, fn, *, depends_on=(), parameters=None):
        if isinstance(fn, str):
            reference = fn
        elif inspect.isfunction(fn) or inspect.isclass(fn):
            if fn.__module__ == "__main__" or fn.__qualname__ != fn.__name__:
                raise Invalid("operators must be top-level importable symbols")
            reference = f"{fn.__module__}:{fn.__name__}"
        else:
            raise Invalid("use an importable function/class or module:symbol string")
        self.steps.append(
            {
                "id": step_id,
                "callable": reference,
                "depends_on": list(depends_on),
                "parameters": {} if parameters is None else parameters,
            }
        )
        return self

    def spec(self, input_path, **overrides):
        return TaskSpec.parse(
            {
                "name": self.name,
                **self.defaults,
                **overrides,
                "input_path": str(input_path),
                "steps": self.steps,
            }
        ).json()


__all__ = ["Client", "Pipeline"]
