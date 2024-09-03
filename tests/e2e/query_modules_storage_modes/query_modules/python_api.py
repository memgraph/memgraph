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

import multiprocessing
from enum import IntEnum

import mgp


class State(IntEnum):
    BEGIN = 0
    READER_READY = 1
    WRITER_READY = 2
    AT_LEAST_ONE_WRITE_DONE = 3


def update_global_state():
    global global_state

    if global_state.value == State.BEGIN:
        global_state.value = State.READER_READY
    elif global_state.value == State.READER_READY:
        global_state.value = State.WRITER_READY
    elif global_state.value == State.WRITER_READY:
        global_state.value = State.AT_LEAST_ONE_WRITE_DONE
    elif global_state.value == State.AT_LEAST_ONE_WRITE_DONE:
        pass


condition = multiprocessing.Condition()
global_state = multiprocessing.Value("i", State.BEGIN)


def wait_for_state(func):
    global condition
    global global_state

    with condition:
        condition.wait_for(lambda: func(State(global_state.value)))
        update_global_state()
        condition.notify_all()


@mgp.read_proc
def reset(ctx: mgp.ProcCtx, arg: mgp.Nullable[str] = None) -> mgp.Record():
    global global_state
    global_state.value = State.BEGIN
    return mgp.Record()


@mgp.write_proc
def delete_vertex(ctx: mgp.ProcCtx, node: mgp.Vertex) -> mgp.Record():
    wait_for_state(lambda state: state == State.READER_READY)
    ctx.graph.detach_delete_vertex(node)
    wait_for_state(lambda state: state == state == State.WRITER_READY)
    return mgp.Record()


@mgp.write_proc
def delete_edge(ctx: mgp.ProcCtx, edge: mgp.Edge) -> mgp.Record():
    wait_for_state(lambda state: state == State.READER_READY)
    ctx.graph.delete_edge(edge)
    wait_for_state(lambda state: state == State.WRITER_READY)
    return mgp.Record()


@mgp.read_proc
def pass_node_with_id(ctx: mgp.ProcCtx, node: mgp.Vertex) -> mgp.Record(node=mgp.Vertex, id=int):
    wait_for_state(lambda state: state == State.BEGIN or state == State.AT_LEAST_ONE_WRITE_DONE)
    wait_for_state(lambda state: state == State.AT_LEAST_ONE_WRITE_DONE)
    return mgp.Record(node=node, id=node.id)


@mgp.function
def pass_node(ctx: mgp.FuncCtx, node: mgp.Vertex):
    wait_for_state(lambda state: state == State.BEGIN or state == State.AT_LEAST_ONE_WRITE_DONE)
    wait_for_state(lambda state: state == State.AT_LEAST_ONE_WRITE_DONE)
    return node


@mgp.function
def pass_relationship(ctx: mgp.FuncCtx, relationship: mgp.Edge):
    wait_for_state(lambda state: state == State.BEGIN or state == State.AT_LEAST_ONE_WRITE_DONE)
    wait_for_state(lambda state: state == State.AT_LEAST_ONE_WRITE_DONE)
    return relationship


@mgp.function
def pass_path(ctx: mgp.FuncCtx, path: mgp.Path):
    wait_for_state(lambda state: state == State.BEGIN or state == State.AT_LEAST_ONE_WRITE_DONE)
    wait_for_state(lambda state: state == State.AT_LEAST_ONE_WRITE_DONE)
    return path


@mgp.function
def pass_list(ctx: mgp.FuncCtx, list_: mgp.List[mgp.Any]):
    wait_for_state(lambda state: state == State.BEGIN or state == State.AT_LEAST_ONE_WRITE_DONE)
    wait_for_state(lambda state: state == State.AT_LEAST_ONE_WRITE_DONE)
    return list_


@mgp.function
def pass_map(ctx: mgp.FuncCtx, map_: mgp.Map):
    wait_for_state(lambda state: state == State.BEGIN or state == State.AT_LEAST_ONE_WRITE_DONE)
    wait_for_state(lambda state: state == State.AT_LEAST_ONE_WRITE_DONE)
    return map_
