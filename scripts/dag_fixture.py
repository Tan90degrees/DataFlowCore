"""Small operators for measuring DAG bookkeeping, rather than model performance."""


def visit(context, inputs):
    context.check_cancelled()
    for index in range(context.parameters.get("reports", 0)):
        context.report(index + 1, context.parameters["reports"])
    return {"step": context.step_id, "parents": sorted(inputs)}
