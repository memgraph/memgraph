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


class BaseClass:
    def __init__(self, num_to_return=1) -> None:
        self._init_is_called = False
        self._num_to_return = num_to_return
        self._num_returned = 0

    def reset(self):
        self._init_is_called = False
        self._num_returned = 0

    def set(self):
        self._init_is_called = True

    def get(self):
        return self._init_is_called

    def increment_returned(self, returned: int):
        self._num_returned += returned

    def get_to_return(self) -> int:
        return self._num_to_return - self._num_returned


class InitializationUnderlyingGraphMutable(BaseClass):
    def __init__(self):
        super().__init__()


initialization_underlying_graph_mutable = InitializationUnderlyingGraphMutable()


def cleanup_underlying():
    initialization_underlying_graph_mutable.reset()


def init_underlying_graph_is_mutable(ctx: mgp.ProcCtx, object: mgp.Any):
    initialization_underlying_graph_mutable.set()


def underlying_graph_is_mutable(ctx: mgp.ProcCtx, object: mgp.Any) -> mgp.Record(mutable=bool, init_called=bool):
    if initialization_underlying_graph_mutable.get_to_return() > 0:
        initialization_underlying_graph_mutable.increment_returned(1)
        return mgp.Record(
            mutable=object.underlying_graph_is_mutable(), init_called=initialization_underlying_graph_mutable.get()
        )
    return []


# Register batched
mgp.add_batch_read_proc(underlying_graph_is_mutable, init_underlying_graph_is_mutable, cleanup_underlying)


class InitializationGraphMutable(BaseClass):
    def __init__(self):
        super().__init__()


initialization_graph_mutable = InitializationGraphMutable()


def init_graph_is_mutable(ctx: mgp.ProcCtx):
    initialization_graph_mutable.set()


def graph_is_mutable(ctx: mgp.ProcCtx) -> mgp.Record(mutable=bool, init_called=bool):
    if initialization_graph_mutable.get_to_return() > 0:
        initialization_graph_mutable.increment_returned(1)
        return mgp.Record(mutable=ctx.graph.is_mutable(), init_called=initialization_graph_mutable.get())
    return []


def cleanup_graph():
    initialization_graph_mutable.reset()


# Register batched
mgp.add_batch_read_proc(graph_is_mutable, init_graph_is_mutable, cleanup_graph)
