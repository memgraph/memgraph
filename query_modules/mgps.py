import mgp


@mgp.read_proc
def components(
    context: mgp.ProcCtx,
) -> mgp.Record(versions=list, edition=str, name=str):
    return mgp.Record(versions=["5.9.0"], edition="community", name="Memgraph")
