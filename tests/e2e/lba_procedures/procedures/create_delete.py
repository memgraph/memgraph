# Copyright 2021 Memgraph Ltd.
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


@mgp.write_proc
def create_vertex(ctx: mgp.ProcCtx) -> mgp.Record(created_node=mgp.Vertex):
    vertex = ctx.graph.create_vertex()
    return mgp.Record(created_node=vertex)


@mgp.write_proc
def remove_label(ctx: mgp.ProcCtx, label: str) -> mgp.Record(node=mgp.Vertex):
    v.remove_label(label)
    return mgp.Record(node=v)


@mgp.write_proc
def set_label(ctx: mgp.ProcCtx, new_label: str) -> mgp.Record(node=mgp.Vertex):
    v.add_label(new_label)
    return mgp.Record(node=v)


@mgp.write_proc
def create_edge(ctx: mgp.ProcCtx, v1: mgp.Vertex, v2: mgp.Vertex) -> mgp.Record(nr_of_edges=int):
    ctx.graph.create_edge(v1, v2, mgp.EdgeType("new_create_delete_edge_type"))

    count = 0
    for vertex in ctx.graph.vertices:
        for _ in vertex.out_edges:
            count += 1

    return mgp.Record(nr_of_edges=count)


@mgp.write_proc
def delete_edge(ctx: mgp.ProcCtx) -> mgp.Record():
    for vertex in ctx.graph.vertices:
        for edge in vertex.out_edges:
            if edge.labels in "create_delete_edge_type":
                try:
                    ctx.graph.delete_edge(vertex)
                except mgp.PermissionDeniedError:
                    pass
                break

    return mgp.Record()
