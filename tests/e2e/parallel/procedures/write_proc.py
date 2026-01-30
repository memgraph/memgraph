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
Write procedures for parallel execution testing.

These procedures are designed to test write operations when called
within queries that use USING PARALLEL EXECUTION hint.

Note: Write operations typically inhibit parallelization of the scan,
but the post-write aggregation/processing can still be parallel.
"""

import mgp


@mgp.write_proc
def create_node(ctx: mgp.ProcCtx) -> mgp.Record(node=mgp.Vertex):
    """Create a new vertex and return it."""
    vertex = ctx.graph.create_vertex()
    return mgp.Record(node=vertex)


@mgp.write_proc
def create_node_with_props(ctx: mgp.ProcCtx, label: str, props: mgp.Map) -> mgp.Record(node=mgp.Vertex):
    """Create a new vertex with label and properties."""
    vertex = ctx.graph.create_vertex()
    vertex.add_label(label)
    for key, value in props.items():
        vertex.properties.set(key, value)
    return mgp.Record(node=vertex)


@mgp.write_proc
def set_property(ctx: mgp.ProcCtx, node: mgp.Vertex, prop_name: str, prop_value: mgp.Any) -> mgp.Record(success=bool):
    """Set a property on a node."""
    try:
        node.properties.set(prop_name, prop_value)
        return mgp.Record(success=True)
    except Exception:
        return mgp.Record(success=False)


@mgp.write_proc
def increment_property(
    ctx: mgp.ProcCtx, node: mgp.Vertex, prop_name: str, increment: int
) -> mgp.Record(old_value=mgp.Any, new_value=int):
    """Increment a numeric property on a node."""
    old_value = node.properties.get(prop_name)
    if old_value is None:
        old_value = 0
    new_value = int(old_value) + increment
    node.properties.set(prop_name, new_value)
    return mgp.Record(old_value=old_value, new_value=new_value)


@mgp.write_proc
def add_label(ctx: mgp.ProcCtx, node: mgp.Vertex, label: str) -> mgp.Record(success=bool):
    """Add a label to a node."""
    try:
        node.add_label(label)
        return mgp.Record(success=True)
    except Exception:
        return mgp.Record(success=False)


@mgp.write_proc
def remove_label(ctx: mgp.ProcCtx, node: mgp.Vertex, label: str) -> mgp.Record(success=bool):
    """Remove a label from a node."""
    try:
        node.remove_label(label)
        return mgp.Record(success=True)
    except Exception:
        return mgp.Record(success=False)


@mgp.write_proc
def create_edge(
    ctx: mgp.ProcCtx, from_node: mgp.Vertex, to_node: mgp.Vertex, edge_type: str
) -> mgp.Record(edge=mgp.Edge):
    """Create an edge between two nodes."""
    edge = ctx.graph.create_edge(from_node, to_node, mgp.EdgeType(edge_type))
    return mgp.Record(edge=edge)


@mgp.write_proc
def delete_node(ctx: mgp.ProcCtx, node: mgp.Vertex) -> mgp.Record(deleted=bool):
    """Delete a node (must have no edges)."""
    try:
        ctx.graph.delete_vertex(node)
        return mgp.Record(deleted=True)
    except Exception:
        return mgp.Record(deleted=False)


@mgp.write_proc
def detach_delete_node(ctx: mgp.ProcCtx, node: mgp.Vertex) -> mgp.Record(deleted=bool):
    """Delete a node and all its edges."""
    try:
        ctx.graph.detach_delete_vertex(node)
        return mgp.Record(deleted=True)
    except Exception:
        return mgp.Record(deleted=False)


@mgp.write_proc
def batch_set_property(ctx: mgp.ProcCtx, prop_name: str, prop_value: mgp.Any) -> mgp.Record(updated_count=int):
    """Set a property on all vertices."""
    count = 0
    for vertex in ctx.graph.vertices:
        vertex.properties.set(prop_name, prop_value)
        count += 1
    return mgp.Record(updated_count=count)


@mgp.write_proc
def graph_is_mutable(ctx: mgp.ProcCtx) -> mgp.Record(mutable=bool):
    """Check if graph is mutable (should be True for write procedures)."""
    return mgp.Record(mutable=ctx.graph.is_mutable())


@mgp.write_proc
def conditional_update(
    ctx: mgp.ProcCtx, node: mgp.Vertex, check_prop: str, check_value: mgp.Any, set_prop: str, set_value: mgp.Any
) -> mgp.Record(updated=bool):
    """Update a property only if another property matches a value."""
    current = node.properties.get(check_prop)
    if current == check_value:
        node.properties.set(set_prop, set_value)
        return mgp.Record(updated=True)
    return mgp.Record(updated=False)
