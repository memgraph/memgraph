import time

import mgp


@mgp.read_proc
def components(
    context: mgp.ProcCtx,
) -> mgp.Record(versions=list, edition=str, name=str):
    return [
        mgp.Record(versions=["5.9.0"], edition="community", name="Memgraph"),
        mgp.Record(versions=["5.9.0"], edition="community", name="Neo4j Kernel"),
    ]


@mgp.read_proc
def await_indexes(context: mgp.ProcCtx, seconds: int):
    time.sleep(1)


@mgp.function
def version() -> str:
    return "5.9.0"


@mgp.function
def validate_predicate(predicate: bool, message: str, params: list):
    if predicate:
        raise Exception(message % tuple(params))
    else:
        return True


@mgp.read_proc
def validate(ctx: mgp.ProcCtx, predicate: bool, message: str, params: mgp.List[mgp.Any]):
    if predicate:
        raise Exception(message % tuple(params))
