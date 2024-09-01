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

import mgp

condition = multiprocessing.Condition()
turn = multiprocessing.Value("i", 0)


def wait_turn(func):
    global condition
    global turn

    with condition:
        condition.wait_for(lambda: func(turn.value))
        turn.value += 1
        condition.notify_all()


@mgp.read_proc
def reset(ctx: mgp.ProcCtx, arg: mgp.Nullable[str] = None) -> mgp.Record():
    global turn
    turn.value = 0
    return mgp.Record()


@mgp.write_proc
def delete_vertex(ctx: mgp.ProcCtx, node: mgp.Vertex) -> mgp.Record():
    wait_turn(lambda cnt: cnt == 1 or cnt == 2)
    ctx.graph.detach_delete_vertex(node)
    wait_turn(lambda cnt: cnt == 1 or cnt == 2)
    return mgp.Record()


@mgp.write_proc
def delete_edge(ctx: mgp.ProcCtx, edge: mgp.Edge) -> mgp.Record():
    wait_turn(lambda cnt: cnt == 1 or cnt == 2)
    ctx.graph.delete_edge(edge)
    wait_turn(lambda cnt: cnt == 1 or cnt == 2)
    return mgp.Record()


@mgp.read_proc
def pass_node_with_id(ctx: mgp.ProcCtx, node: mgp.Vertex) -> mgp.Record(node=mgp.Vertex, id=int):
    wait_turn(lambda cnt: cnt == 0 or cnt > 2)
    wait_turn(lambda cnt: cnt == 0 or cnt > 2)
    return mgp.Record(node=node, id=node.id)


@mgp.function
def pass_node(ctx: mgp.FuncCtx, node: mgp.Vertex):
    wait_turn(lambda cnt: cnt == 0 or cnt > 2)
    wait_turn(lambda cnt: cnt == 0 or cnt > 2)
    return node


@mgp.function
def pass_relationship(ctx: mgp.FuncCtx, relationship: mgp.Edge):
    wait_turn(lambda cnt: cnt == 0 or cnt > 2)
    wait_turn(lambda cnt: cnt == 0 or cnt > 2)
    return relationship


@mgp.function
def pass_path(ctx: mgp.FuncCtx, path: mgp.Path):
    wait_turn(lambda cnt: cnt == 0 or cnt > 2)
    wait_turn(lambda cnt: cnt == 0 or cnt > 2)
    return path


@mgp.function
def pass_list(ctx: mgp.FuncCtx, list_: mgp.List[mgp.Any]):
    wait_turn(lambda cnt: cnt == 0 or cnt > 2)
    wait_turn(lambda cnt: cnt == 0 or cnt > 2)
    return list_


@mgp.function
def pass_map(ctx: mgp.FuncCtx, map_: mgp.Map):
    wait_turn(lambda cnt: cnt == 0 or cnt > 2)
    wait_turn(lambda cnt: cnt == 0 or cnt > 2)
    return map_
