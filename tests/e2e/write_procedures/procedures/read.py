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


@mgp.read_proc
def underlying_graph_is_mutable(ctx: mgp.ProcCtx, object: mgp.Any) -> mgp.Record(mutable=bool):
    return mgp.Record(mutable=object.underlying_graph_is_mutable())


@mgp.read_proc
def graph_is_mutable(ctx: mgp.ProcCtx) -> mgp.Record(mutable=bool):
    return mgp.Record(mutable=ctx.graph.is_mutable())


@mgp.read_proc
def subgraph_get_vertices(ctx: mgp.ProcCtx) -> mgp.Record(node=mgp.Vertex):
    return [mgp.Record(node=vertex) for vertex in ctx.graph.vertices]


@mgp.read_proc
def subgraph_get_out_edges(ctx: mgp.ProcCtx, vertex: mgp.Vertex) -> mgp.Record(edge=mgp.Edge):
    return [mgp.Record(edge=edge) for edge in vertex.out_edges]


@mgp.read_proc
def subgraph_get_in_edges(ctx: mgp.ProcCtx, vertex: mgp.Vertex) -> mgp.Record(edge=mgp.Edge):
    return [mgp.Record(edge=edge) for edge in vertex.in_edges]


@mgp.read_proc
def subgraph_get_2_hop_edges(ctx: mgp.ProcCtx, vertex: mgp.Vertex) -> mgp.Record(edge=mgp.Edge):
    out_edges = vertex.out_edges
    records = []
    for edge in out_edges:
        vertex = edge.to_vertex
        properties = vertex.properties
        print(properties)
        records.extend([mgp.Record(edge=edge) for edge in edge.to_vertex.out_edges])
    return records


@mgp.read_proc
def subgraph_get_out_edges_vertex_id(ctx: mgp.ProcCtx, vertex: mgp.Vertex) -> mgp.Record(edge=mgp.Edge):
    vertex = ctx.graph.get_vertex_by_id(vertex.id)
    return [mgp.Record(edge=edge) for edge in vertex.out_edges]


@mgp.read_proc
def subgraph_get_path_vertices(ctx: mgp.ProcCtx, path: mgp.Path) -> mgp.Record(node=mgp.Vertex):
    return [mgp.Record(node=node) for node in path.vertices]


@mgp.read_proc
def subgraph_get_path_edges(ctx: mgp.ProcCtx, path: mgp.Path) -> mgp.Record(edge=mgp.Edge):
    return [mgp.Record(edge=edge) for edge in path.edges]


@mgp.read_proc
def subgraph_get_path_vertices_in_subgraph(ctx: mgp.ProcCtx, path: mgp.Path) -> mgp.Record(node=mgp.Vertex):
    path_vertices = path.vertices
    graph_vertices = ctx.graph.vertices
    records = []
    for path_vertex in path_vertices:
        if path_vertex in graph_vertices:
            records.append(mgp.Record(node=path_vertex))
    return records
