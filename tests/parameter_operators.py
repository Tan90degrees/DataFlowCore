"""Importable operators for real subprocess configuration isolation acceptance."""


def snapshot(context, inputs):
    if context.parameters.get("mutate"):
        context.parameters["nested"]["flags"].append("changed")
    return {"parameters": context.parameters, "inputs": dict(inputs)}
