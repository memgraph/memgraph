import mgp


@mgp.read_proc
def components(
    context: mgp.ProcCtx,
) -> mgp.Record(versions=list, edition=str, name=str):
    return mgp.Record(versions=["5.9.0"], edition="community", name="Memgraph")


@mgp.function
def version() -> str:
    return "5.9.0"


@mgp.function
def validate_predicate(predicate: bool, message: str, params: list):
    if predicate:
        raise Exception(message % tuple(params))
    else:
        return True


@mgp.write_proc
def validate(ctx: mgp.ProcCtx, predicate: bool, message: str, params: mgp.List[mgp.Any]):
    if predicate:
        raise Exception(message % tuple(params))
