# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""
Read procedures for parallel execution testing.

These procedures are designed to test various scenarios when called
within queries that use USING PARALLEL EXECUTION hint.
"""

import mgp


@mgp.read_proc
def get_node_count(ctx: mgp.ProcCtx) -> mgp.Record(count=int):
    """Return the count of all vertices in the graph."""
    count = 0
    for _ in ctx.graph.vertices:
        count += 1
    return mgp.Record(count=count)


@mgp.read_proc
def get_node_properties(ctx: mgp.ProcCtx, node: mgp.Vertex) -> mgp.Record(id=int, props=mgp.Map):
    """Return node ID and all properties."""
    props = dict(node.properties.items())
    props["_large_data"] = "x" * 120000
    return mgp.Record(id=node.id, props=props)


@mgp.read_proc
def get_node_labels(ctx: mgp.ProcCtx, node: mgp.Vertex) -> mgp.Record(id=int, labels=mgp.List[str]):
    """Return node ID and its labels."""
    labels = [str(label) for label in node.labels]
    return mgp.Record(id=id(node), labels=labels)


@mgp.read_proc
def compute_value(ctx: mgp.ProcCtx, x: int, y: int) -> mgp.Record(result=int):
    """Simple computation procedure."""
    return mgp.Record(result=x * y + x + y)


@mgp.read_proc
def generate_numbers(ctx: mgp.ProcCtx, start: int, end: int) -> mgp.Record(num=int):
    """Generate a sequence of numbers."""
    return [mgp.Record(num=i) for i in range(start, end + 1)]


@mgp.read_proc
def echo_values(
    ctx: mgp.ProcCtx, int_val: int, str_val: str, float_val: float
) -> mgp.Record(int_out=int, str_out=str, float_out=float):
    """Echo back the input values - useful for testing parameter passing."""
    return mgp.Record(int_out=int_val, str_out=str_val, float_out=float_val)


@mgp.read_proc
def sum_property(ctx: mgp.ProcCtx, prop_name: str) -> mgp.Record(total=mgp.Any):
    """Sum a numeric property across all vertices."""
    total = 0
    for vertex in ctx.graph.vertices:
        val = vertex.properties.get(prop_name)
        if val is not None and isinstance(val, (int, float)):
            total += val
    return mgp.Record(total=total)


@mgp.read_proc
def filter_nodes_by_property(ctx: mgp.ProcCtx, prop_name: str, min_val: int) -> mgp.Record(node=mgp.Vertex):
    """Return nodes where property value >= min_val."""
    results = []
    for vertex in ctx.graph.vertices:
        val = vertex.properties.get(prop_name)
        if val is not None and isinstance(val, (int, float)) and val >= min_val:
            results.append(mgp.Record(node=vertex))
    return results


@mgp.read_proc
def get_neighbors(ctx: mgp.ProcCtx, node: mgp.Vertex) -> mgp.Record(neighbor=mgp.Vertex, edge_type=str):
    """Return all neighbors of a node with edge types."""
    results = []
    for edge in node.out_edges:
        results.append(mgp.Record(neighbor=edge.to_vertex, edge_type=edge.type.name))
    for edge in node.in_edges:
        results.append(mgp.Record(neighbor=edge.from_vertex, edge_type=edge.type.name))
    return results


@mgp.read_proc
def aggregate_by_label(ctx: mgp.ProcCtx) -> mgp.Record(label=str, count=int):
    """Count nodes by their first label."""
    label_counts = {}
    for vertex in ctx.graph.vertices:
        labels = list(vertex.labels)
        if labels:
            label = str(labels[0])
            label_counts[label] = label_counts.get(label, 0) + 1
        else:
            label_counts["_unlabeled"] = label_counts.get("_unlabeled", 0) + 1
    return [mgp.Record(label=k, count=v) for k, v in label_counts.items()]


@mgp.read_proc
def graph_is_mutable(ctx: mgp.ProcCtx) -> mgp.Record(mutable=bool):
    """Check if graph is mutable (should be False for read procedures)."""
    return mgp.Record(mutable=ctx.graph.is_mutable())
