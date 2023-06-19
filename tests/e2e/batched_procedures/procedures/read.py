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


class BatchingNums(BaseClass):
    def __init__(self, nums_to_return):
        super().__init__(nums_to_return)
        self._nums = []
        self._i = 0


batching_nums = BatchingNums(10)


def init_batching_nums(ctx: mgp.ProcCtx):
    batching_nums.set()
    batching_nums._nums = [i for i in range(1, 11)]
    batching_nums._i = 0


def batch_nums(ctx: mgp.ProcCtx) -> mgp.Record(num=int, init_called=bool, is_valid=bool):
    if batching_nums.get_to_return() > 0:
        batching_nums.increment_returned(1)
        batching_nums._i += 1
        return mgp.Record(
            num=batching_nums._nums[batching_nums._i - 1],
            init_called=batching_nums.get(),
            is_valid=ctx.graph.is_valid(),
        )
    return []


def cleanup_batching_nums():
    batching_nums.reset()
    batching_nums._i = 0


# Register batched
mgp.add_batch_read_proc(batch_nums, init_batching_nums, cleanup_batching_nums)


class BatchingVertices(BaseClass):
    def __init__(self):
        super().__init__()
        self._vertices = []
        self._i = 0


batching_vertices = BatchingVertices()


def init_batching_vertices(ctx: mgp.ProcCtx):
    print("init called")
    print("graph is mutable", ctx.graph.is_mutable())
    batching_vertices.set()
    batching_vertices._vertices = list(ctx.graph.vertices)
    batching_vertices._i = 0
    batching_vertices._num_to_return = len(batching_vertices._vertices)


def batch_vertices(ctx: mgp.ProcCtx) -> mgp.Record(vertex_id=int, init_called=bool):
    if batching_vertices.get_to_return() == 0:
        return []
    batching_vertices.increment_returned(1)
    return mgp.Record(vertex=batching_vertices._vertices[batching_vertices._i].id, init_called=batching_vertices.get())


def cleanup_batching_vertices():
    batching_vertices.reset()
    batching_vertices._vertices = []
    batching_vertices._i = 0
    batching_vertices._num_to_return = 0


# Register batched
mgp.add_batch_read_proc(batch_vertices, init_batching_vertices, cleanup_batching_vertices)
