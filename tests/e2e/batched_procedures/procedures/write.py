import mgp

# isort: off
from read import InitializationGraphMutable, InitializationUnderlyingGraphMutable

write_init_underlying_graph_mutable = InitializationUnderlyingGraphMutable()


def cleanup_underlying():
    write_init_underlying_graph_mutable.reset()


def init_underlying_graph_is_mutable(ctx: mgp.ProcCtx, object: mgp.Any):
    write_init_underlying_graph_mutable.set()


def underlying_graph_is_mutable(ctx: mgp.ProcCtx, object: mgp.Any) -> mgp.Record(mutable=bool, init_called=bool):
    if write_init_underlying_graph_mutable.get_to_return() == 0:
        return []
    write_init_underlying_graph_mutable.increment_returned(1)
    return mgp.Record(
        mutable=object.underlying_graph_is_mutable(), init_called=write_init_underlying_graph_mutable.get()
    )


# Register batched
mgp.add_batch_write_proc(underlying_graph_is_mutable, init_underlying_graph_is_mutable, cleanup_underlying)


write_init_graph_mutable = InitializationGraphMutable()


def init_graph_is_mutable(ctx: mgp.ProcCtx):
    write_init_graph_mutable.set()


def graph_is_mutable(ctx: mgp.ProcCtx) -> mgp.Record(mutable=bool, init_called=bool):
    if write_init_graph_mutable.get_to_return() > 0:
        write_init_graph_mutable.increment_returned(1)
        return mgp.Record(mutable=ctx.graph.is_mutable(), init_called=write_init_graph_mutable.get())
    return []


def cleanup_graph():
    write_init_graph_mutable.reset()


# Register batched
mgp.add_batch_write_proc(graph_is_mutable, init_graph_is_mutable, cleanup_graph)
