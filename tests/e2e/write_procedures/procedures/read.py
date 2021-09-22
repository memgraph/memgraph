import mgp


@mgp.read_proc
def underlying_graph_is_mutable(ctx: mgp.ProcCtx,
                                object: mgp.Any) -> mgp.Record(mutable=bool):
    return mgp.Record(mutable=object.underlying_graph_is_mutable())


@mgp.read_proc
def graph_is_mutable(ctx: mgp.ProcCtx) -> mgp.Record(mutable=bool):
    return mgp.Record(mutable=ctx.graph.is_mutable())
