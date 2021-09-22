import mgp


@mgp.write_proc
def create_vertex(ctx: mgp.ProcCtx) -> mgp.Record(v=mgp.Any):
    v = None
    try:
        v = ctx.graph.create_vertex()
    except RuntimeError as e:
        return mgp.Record(v=str(e))
    return mgp.Record(v=v)


@mgp.write_proc
def delete_vertex(ctx: mgp.ProcCtx, v: mgp.Any) -> mgp.Record():
    ctx.graph.delete_vertex(v)
    return mgp.Record()


@mgp.write_proc
def detach_delete_vertex(ctx: mgp.ProcCtx, v: mgp.Any) -> mgp.Record():
    ctx.graph.detach_delete_vertex(v)
    return mgp.Record()


@mgp.write_proc
def create_edge(ctx: mgp.ProcCtx, from_vertex: mgp.Vertex,
                to_vertex: mgp.Vertex,
                edge_type: str) -> mgp.Record(e=mgp.Any):
    e = None
    try:
        e = ctx.graph.create_edge(
            from_vertex, to_vertex, mgp.EdgeType(edge_type))
    except RuntimeError as ex:
        return mgp.Record(e=str(ex))
    return mgp.Record(e=e)


@mgp.write_proc
def delete_edge(ctx: mgp.ProcCtx, edge: mgp.Edge) -> mgp.Record():
    ctx.graph.delete_edge(edge)
    return mgp.Record()


@mgp.write_proc
def set_property(ctx: mgp.ProcCtx, object: mgp.Any,
                 name: str, value: mgp.Nullable[mgp.Any]) -> mgp.Record():
    object.properties.set(name, value)
    return mgp.Record()


@mgp.write_proc
def add_label(ctx: mgp.ProcCtx, object: mgp.Any,
              name: str) -> mgp.Record(o=mgp.Any):
    object.add_label(name)
    return mgp.Record(o=object)


@mgp.write_proc
def remove_label(ctx: mgp.ProcCtx, object: mgp.Any,
                 name: str) -> mgp.Record(o=mgp.Any):
    object.remove_label(name)
    return mgp.Record(o=object)


@mgp.write_proc
def underlying_graph_is_mutable(ctx: mgp.ProcCtx,
                                object: mgp.Any) -> mgp.Record(mutable=bool):
    return mgp.Record(mutable=object.underlying_graph_is_mutable())


@mgp.write_proc
def graph_is_mutable(ctx: mgp.ProcCtx) -> mgp.Record(mutable=bool):
    return mgp.Record(mutable=ctx.graph.is_mutable())
