# Copyright 2026 Memgraph Ltd.
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
GNN integration module for Memgraph.

This module provides procedures to export and import graph data for
PyTorch Geometric (PyG) and TensorFlow GNN (TF-GNN).

Example usage:
    # Export entire graph to PyG format
    CALL gnn.pyg_export(['feature1', 'feature2'], ['weight']) YIELD *;

    # Import PyG data (from JSON string)
    CALL gnn.pyg_import_from_dict('{...}', 'PyGNode', 'EDGE') YIELD *;

    # Export entire graph to TF-GNN format
    CALL gnn.tf_export(['feature1'], ['weight']) YIELD schema, graph;

    # Import TF-GNN data (from JSON string)
    CALL gnn.tf_import_from_dict('{...}', 'TfGnnNode', 'CONNECTS') YIELD *;
"""

import json
from typing import Dict, List, Tuple

import mgp
from mage.pyg.converter import MemgraphPyGConverter, pyg_dict_to_graph_data
from mage.tfgnn.converter import MemgraphTfGnnConverter, tfgnn_dict_to_graph_data


def _collect_vertices_and_edges(ctx: mgp.ProcCtx) -> Tuple[List[mgp.Vertex], List[mgp.Edge]]:
    vertices = list(ctx.graph.vertices)
    edges = []
    for vertex in vertices:
        for edge in vertex.out_edges:
            edges.append(edge)
    return vertices, edges


@mgp.read_proc
def pyg_export(
    ctx: mgp.ProcCtx,
    node_property_names: mgp.Nullable[mgp.List[str]] = None,
    edge_property_names: mgp.Nullable[mgp.List[str]] = None,
    node_label_property: mgp.Nullable[str] = None,
) -> mgp.Record(
    edge_index=mgp.List[mgp.List[int]],
    x=mgp.Nullable[mgp.List[mgp.List[mgp.Number]]],
    edge_attr=mgp.Nullable[mgp.List[mgp.List[mgp.Number]]],
    y=mgp.Nullable[mgp.List[mgp.Any]],
    num_nodes=int,
    node_id_mapping=mgp.Map,
    labels=mgp.List[mgp.List[str]],
    edge_types=mgp.List[str],
):
    """Export the entire graph to PyG format."""
    vertices, edges = _collect_vertices_and_edges(ctx)

    converter = MemgraphPyGConverter()
    result = converter.export_to_pyg_dict(
        vertices=vertices,
        edges=edges,
        node_property_names=list(node_property_names) if node_property_names else None,
        edge_property_names=list(edge_property_names) if edge_property_names else None,
        node_label_property=node_label_property if node_label_property else None,
        include_node_id=True,
        include_labels=True,
    )

    return mgp.Record(
        edge_index=result["edge_index"],
        x=result.get("x"),
        edge_attr=result.get("edge_attr"),
        y=result.get("y"),
        num_nodes=result["num_nodes"],
        node_id_mapping=result.get("node_id_mapping", {}),
        labels=result.get("labels", []),
        edge_types=result.get("edge_types", []),
    )


@mgp.read_proc
def pyg_export_subgraph(
    ctx: mgp.ProcCtx,
    vertices: mgp.List[mgp.Vertex],
    edges: mgp.List[mgp.Edge],
    node_property_names: mgp.Nullable[mgp.List[str]] = None,
    edge_property_names: mgp.Nullable[mgp.List[str]] = None,
    node_label_property: mgp.Nullable[str] = None,
) -> mgp.Record(
    edge_index=mgp.List[mgp.List[int]],
    x=mgp.Nullable[mgp.List[mgp.List[mgp.Number]]],
    edge_attr=mgp.Nullable[mgp.List[mgp.List[mgp.Number]]],
    y=mgp.Nullable[mgp.List[mgp.Any]],
    num_nodes=int,
    node_id_mapping=mgp.Map,
    labels=mgp.List[mgp.List[str]],
    edge_types=mgp.List[str],
):
    """Export a subgraph to PyG format."""
    converter = MemgraphPyGConverter()
    result = converter.export_to_pyg_dict(
        vertices=list(vertices),
        edges=list(edges),
        node_property_names=list(node_property_names) if node_property_names else None,
        edge_property_names=list(edge_property_names) if edge_property_names else None,
        node_label_property=node_label_property if node_label_property else None,
        include_node_id=True,
        include_labels=True,
    )

    return mgp.Record(
        edge_index=result["edge_index"],
        x=result.get("x"),
        edge_attr=result.get("edge_attr"),
        y=result.get("y"),
        num_nodes=result["num_nodes"],
        node_id_mapping=result.get("node_id_mapping", {}),
        labels=result.get("labels", []),
        edge_types=result.get("edge_types", []),
    )


@mgp.read_proc
def pyg_export_to_json(
    ctx: mgp.ProcCtx,
    node_property_names: mgp.Nullable[mgp.List[str]] = None,
    edge_property_names: mgp.Nullable[mgp.List[str]] = None,
    node_label_property: mgp.Nullable[str] = None,
) -> mgp.Record(json_data=str):
    """Export the entire graph to a JSON string in PyG format."""
    vertices, edges = _collect_vertices_and_edges(ctx)

    converter = MemgraphPyGConverter()
    result = converter.export_to_pyg_dict(
        vertices=vertices,
        edges=edges,
        node_property_names=list(node_property_names) if node_property_names else None,
        edge_property_names=list(edge_property_names) if edge_property_names else None,
        node_label_property=node_label_property if node_label_property else None,
        include_node_id=True,
        include_labels=True,
    )

    return mgp.Record(json_data=json.dumps(result))


@mgp.write_proc
def pyg_import_from_dict(
    ctx: mgp.ProcCtx,
    json_data: str,
    default_node_label: str = "PyGNode",
    default_edge_type: str = "CONNECTS",
    node_property_names: mgp.Nullable[mgp.List[str]] = None,
    edge_property_names: mgp.Nullable[mgp.List[str]] = None,
) -> mgp.Record(nodes_created=int, edges_created=int):
    """Import PyG data from a JSON string into Memgraph."""
    pyg_dict = json.loads(json_data)

    nodes_data, edges_data = pyg_dict_to_graph_data(
        pyg_dict=pyg_dict,
        default_node_label=default_node_label,
        default_edge_type=default_edge_type,
        node_property_names=list(node_property_names) if node_property_names else None,
        edge_property_names=list(edge_property_names) if edge_property_names else None,
    )

    idx_to_vertex: Dict[int, mgp.Vertex] = {}
    nodes_created = 0

    for node_data in nodes_data:
        vertex = ctx.graph.create_vertex()
        idx = node_data["idx"]

        for label in node_data.get("labels", [default_node_label]):
            vertex.add_label(label)

        for prop_name, prop_value in node_data.get("properties", {}).items():
            vertex.properties.set(prop_name, prop_value)

        vertex.properties.set("_pyg_idx", idx)

        idx_to_vertex[idx] = vertex
        nodes_created += 1

    edges_created = 0

    for edge_data in edges_data:
        src_idx = edge_data["source_idx"]
        dst_idx = edge_data["target_idx"]

        if src_idx in idx_to_vertex and dst_idx in idx_to_vertex:
            src_vertex = idx_to_vertex[src_idx]
            dst_vertex = idx_to_vertex[dst_idx]
            edge_type = mgp.EdgeType(edge_data.get("type", default_edge_type))

            edge = ctx.graph.create_edge(src_vertex, dst_vertex, edge_type)

            for prop_name, prop_value in edge_data.get("properties", {}).items():
                edge.properties.set(prop_name, prop_value)

            edges_created += 1

    return mgp.Record(nodes_created=nodes_created, edges_created=edges_created)


@mgp.read_proc
def pyg_get_node_features(
    ctx: mgp.ProcCtx,
    vertices: mgp.List[mgp.Vertex],
    property_names: mgp.List[str],
) -> mgp.Record(node_id=int, features=mgp.List[mgp.Number],):
    """Get node features for a list of vertices."""
    results = []
    for vertex in vertices:
        props = vertex.properties
        features = []

        for prop_name in property_names:
            value = props.get(prop_name, None)

            if isinstance(value, (list, tuple)):
                features.extend([float(v) if isinstance(v, (int, float)) else 0.0 for v in value])
            elif isinstance(value, (int, float)):
                features.append(float(value))
            elif isinstance(value, bool):
                features.append(1.0 if value else 0.0)
            else:
                features.append(0.0)

        results.append(mgp.Record(node_id=vertex.id, features=features))

    return results


@mgp.read_proc
def pyg_help(
    ctx: mgp.ProcCtx,
) -> mgp.Record(name=str, description=str):
    """Show help information for PyG procedures."""
    procedures = [
        ("gnn.pyg_export", "Export entire graph to PyG format with node/edge features"),
        ("gnn.pyg_export_subgraph", "Export specific vertices and edges to PyG format"),
        ("gnn.pyg_export_to_json", "Export graph to JSON string in PyG format"),
        ("gnn.pyg_import_from_dict", "Import PyG data from JSON string into Memgraph"),
        ("gnn.pyg_get_node_features", "Extract node features from vertices"),
        ("gnn.pyg_help", "Show this help information"),
    ]

    return [mgp.Record(name=name, description=desc) for name, desc in procedures]


@mgp.read_proc
def tf_export(
    ctx: mgp.ProcCtx,
    node_property_names: mgp.Nullable[mgp.List[str]] = None,
    edge_property_names: mgp.Nullable[mgp.List[str]] = None,
    node_set_name: str = "node",
    edge_set_name: str = "edge",
) -> mgp.Record(schema=mgp.Map, graph=mgp.Map):
    """Export the entire graph to TF-GNN format."""
    vertices, edges = _collect_vertices_and_edges(ctx)

    converter = MemgraphTfGnnConverter()
    result = converter.export_to_tfgnn_dict(
        vertices=vertices,
        edges=edges,
        node_property_names=list(node_property_names) if node_property_names else None,
        edge_property_names=list(edge_property_names) if edge_property_names else None,
        node_set_name=node_set_name,
        edge_set_name=edge_set_name,
    )

    return mgp.Record(schema=result["schema"], graph=result["graph"])


@mgp.read_proc
def tf_export_to_json(
    ctx: mgp.ProcCtx,
    node_property_names: mgp.Nullable[mgp.List[str]] = None,
    edge_property_names: mgp.Nullable[mgp.List[str]] = None,
    node_set_name: str = "node",
    edge_set_name: str = "edge",
) -> mgp.Record(json_data=str):
    """Export the entire graph to a JSON string in TF-GNN format."""
    vertices, edges = _collect_vertices_and_edges(ctx)

    converter = MemgraphTfGnnConverter()
    result = converter.export_to_tfgnn_dict(
        vertices=vertices,
        edges=edges,
        node_property_names=list(node_property_names) if node_property_names else None,
        edge_property_names=list(edge_property_names) if edge_property_names else None,
        node_set_name=node_set_name,
        edge_set_name=edge_set_name,
    )

    return mgp.Record(json_data=json.dumps(result))


@mgp.write_proc
def tf_import_from_dict(
    ctx: mgp.ProcCtx,
    json_data: str,
    default_node_label: str = "TfGnnNode",
    default_edge_type: str = "CONNECTS",
) -> mgp.Record(nodes_created=int, edges_created=int):
    """Import TF-GNN data from a JSON string into Memgraph."""
    tfgnn_dict = json.loads(json_data)

    nodes_data, edges_data = tfgnn_dict_to_graph_data(
        tfgnn_dict=tfgnn_dict,
        default_node_label=default_node_label,
        default_edge_type=default_edge_type,
    )

    nodes_created = 0
    node_lookup: Dict[tuple, mgp.Vertex] = {}

    for node_data in nodes_data:
        vertex = ctx.graph.create_vertex()
        idx = node_data["idx"]
        node_set = node_data.get("node_set", "")

        for label in node_data.get("labels", [default_node_label]):
            vertex.add_label(label)

        for prop_name, prop_value in node_data.get("properties", {}).items():
            vertex.properties.set(prop_name, prop_value)

        vertex.properties.set("_tfgnn_idx", idx)
        if node_set:
            vertex.properties.set("_tfgnn_node_set", node_set)

        node_lookup[(node_set, idx)] = vertex
        nodes_created += 1

    edges_created = 0

    for edge_data in edges_data:
        src_idx = edge_data["source_idx"]
        dst_idx = edge_data["target_idx"]
        src_set = edge_data.get("source_set", "")
        dst_set = edge_data.get("target_set", "")

        src_vertex = node_lookup.get((src_set, src_idx))
        dst_vertex = node_lookup.get((dst_set, dst_idx))
        if src_vertex is None or dst_vertex is None:
            continue

        edge_type = mgp.EdgeType(edge_data.get("type", default_edge_type))
        edge = ctx.graph.create_edge(src_vertex, dst_vertex, edge_type)

        for prop_name, prop_value in edge_data.get("properties", {}).items():
            edge.properties.set(prop_name, prop_value)

        edges_created += 1

    return mgp.Record(nodes_created=nodes_created, edges_created=edges_created)


@mgp.read_proc
def tf_help(
    ctx: mgp.ProcCtx,
) -> mgp.Record(name=str, description=str):
    """Show help information for TF-GNN procedures."""
    procedures = [
        ("gnn.tf_export", "Export entire graph to TF-GNN format"),
        ("gnn.tf_export_to_json", "Export graph to JSON string in TF-GNN format"),
        ("gnn.tf_import_from_dict", "Import TF-GNN data from JSON string into Memgraph"),
        ("gnn.tf_help", "Show this help information"),
    ]

    return [mgp.Record(name=name, description=desc) for name, desc in procedures]
