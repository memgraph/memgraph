# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import mgp

# isort: off
from common.shared import InitializationGraphMutable, InitializationUnderlyingGraphMutable

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
