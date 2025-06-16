import typing

import mgp


@mgp.read_proc
def components(context: mgp.ProcCtx) -> mgp.Record(versions=list, edition=str):
    return mgp.Record(versions=["4.3"], edition="4.3.2")


@mgp.function
def validate_predicate(predicate: bool, message: str, params: list):
    if predicate:
        raise Exception(message % tuple(params))
    else:
        return True
