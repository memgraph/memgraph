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
PyTorch Geometric (PyG) integration module for Memgraph.

This module provides procedures to export and import graph data in a format
compatible with PyTorch Geometric.

The module supports:
- Exporting Memgraph graph data to PyG-compatible format
- Importing PyG data into Memgraph
- Handling node/edge features and labels

Example usage:
    # Export entire graph to PyG format
    CALL pyg.export(['feature1', 'feature2'], ['weight']) YIELD *;
    
    # Export with node labels for classification
    CALL pyg.export(['features'], [], 'class') YIELD *;
    
    # Import PyG data (from JSON string)
    CALL pyg.import_from_dict('{...}', 'PyGNode', 'EDGE') YIELD *;
"""

import json
from typing import Dict

import mgp
from mage.pyg.converter import (
    MemgraphPyGConverter,
    graph_to_pyg_dict,
    pyg_dict_to_graph_data,
)


@mgp.read_proc
def export(
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
    """
    Export the entire graph to PyTorch Geometric compatible format.
    
    This procedure exports all vertices and edges from the current graph
    into a format that can be used directly with PyTorch Geometric.
    
    Args:
        node_property_names: List of vertex property names to include as node features.
            Properties should contain numeric values or lists of numeric values.
        edge_property_names: List of edge property names to include as edge features.
            Properties should contain numeric values or lists of numeric values.
        node_label_property: Property name to use as classification labels (y).
            If empty, y will be NULL.
    
    Returns:
        edge_index: Edge index tensor as [[source_indices], [target_indices]]
        x: Node feature matrix (list of feature vectors per node), or NULL if no properties specified
        edge_attr: Edge feature matrix (list of feature vectors per edge), or NULL if no properties specified
        y: Node labels for classification, or NULL if no label property specified
        num_nodes: Total number of nodes
        node_id_mapping: Mapping from original Memgraph node IDs to PyG indices
        labels: List of Memgraph labels for each node
        edge_types: List of edge types for each edge
    
    Example:
        ```cypher
        CALL pyg.export(['embedding', 'age'], ['weight'], 'class') YIELD *;
        ```
    """
    # Collect all vertices and edges
    vertices = list(ctx.graph.vertices)
    edges = []
    for vertex in vertices:
        for edge in vertex.out_edges:
            edges.append(edge)
    
    # Convert to PyG format
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
        edge_index=result['edge_index'],
        x=result.get('x'),
        edge_attr=result.get('edge_attr'),
        y=result.get('y'),
        num_nodes=result['num_nodes'],
        node_id_mapping=result.get('node_id_mapping', {}),
        labels=result.get('labels', []),
        edge_types=result.get('edge_types', []),
    )


@mgp.read_proc
def export_subgraph(
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
    """
    Export a specific subgraph to PyTorch Geometric compatible format.
    
    This procedure allows exporting a specific set of vertices and edges
    rather than the entire graph.
    
    Args:
        vertices: List of vertices to include in the export
        edges: List of edges to include in the export
        node_property_names: List of vertex property names to include as node features
        edge_property_names: List of edge property names to include as edge features
        node_label_property: Property name to use as classification labels (y)
    
    Returns:
        Same as export() procedure
    
    Example:
        ```cypher
        MATCH (n:Person)-[r:KNOWS]->(m:Person)
        WITH collect(n) + collect(m) AS nodes, collect(r) AS rels
        CALL pyg.export_subgraph(nodes, rels, ['age', 'income'], ['strength']) YIELD *
        RETURN *;
        ```
    """
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
        edge_index=result['edge_index'],
        x=result.get('x'),
        edge_attr=result.get('edge_attr'),
        y=result.get('y'),
        num_nodes=result['num_nodes'],
        node_id_mapping=result.get('node_id_mapping', {}),
        labels=result.get('labels', []),
        edge_types=result.get('edge_types', []),
    )


@mgp.read_proc
def export_to_json(
    ctx: mgp.ProcCtx,
    node_property_names: mgp.Nullable[mgp.List[str]] = None,
    edge_property_names: mgp.Nullable[mgp.List[str]] = None,
    node_label_property: mgp.Nullable[str] = None,
) -> mgp.Record(json_data=str):
    """
    Export the entire graph to a JSON string in PyTorch Geometric compatible format.
    
    This is useful for serializing the graph data for external processing.
    
    Args:
        node_property_names: List of vertex property names to include as node features
        edge_property_names: List of edge property names to include as edge features
        node_label_property: Property name to use as classification labels
    
    Returns:
        json_data: JSON string containing the PyG-compatible graph data
    
    Example:
        ```cypher
        CALL pyg.export_to_json(['features'], ['weight']) YIELD json_data
        RETURN json_data;
        ```
    """
    vertices = list(ctx.graph.vertices)
    edges = []
    for vertex in vertices:
        for edge in vertex.out_edges:
            edges.append(edge)
    
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
def import_from_dict(
    ctx: mgp.ProcCtx,
    json_data: str,
    default_node_label: str = "PyGNode",
    default_edge_type: str = "CONNECTS",
    node_property_names: mgp.Nullable[mgp.List[str]] = None,
    edge_property_names: mgp.Nullable[mgp.List[str]] = None,
) -> mgp.Record(nodes_created=int, edges_created=int):
    """
    Import PyTorch Geometric data from a JSON string into Memgraph.
    
    This procedure creates vertices and edges in Memgraph based on
    PyG-formatted data provided as a JSON string.
    
    Args:
        json_data: JSON string containing PyG-formatted data with at minimum
            'edge_index' and 'num_nodes' fields
        default_node_label: Label to assign to created nodes (default: "PyGNode")
        default_edge_type: Edge type for created relationships (default: "CONNECTS")
        node_property_names: Property names to use for node features in 'x'
        edge_property_names: Property names to use for edge features in 'edge_attr'
    
    Returns:
        nodes_created: Number of nodes created
        edges_created: Number of edges created
    
    Example:
        ```cypher
        CALL pyg.import_from_dict(
            '{"edge_index": [[0,1], [1,2]], "num_nodes": 3, "x": [[1.0], [2.0], [3.0]]}',
            'Node', 'EDGE', ['feature']) YIELD *;
        ```
    """
    pyg_dict = json.loads(json_data)
    
    nodes_data, edges_data = pyg_dict_to_graph_data(
        pyg_dict=pyg_dict,
        default_node_label=default_node_label,
        default_edge_type=default_edge_type,
        node_property_names=list(node_property_names) if node_property_names else None,
        edge_property_names=list(edge_property_names) if edge_property_names else None,
    )
    
    # Create nodes
    idx_to_vertex: Dict[int, mgp.Vertex] = {}
    nodes_created = 0
    
    for node_data in nodes_data:
        vertex = ctx.graph.create_vertex()
        idx = node_data['idx']
        
        # Add labels
        for label in node_data.get('labels', [default_node_label]):
            vertex.add_label(label)
        
        # Add properties
        for prop_name, prop_value in node_data.get('properties', {}).items():
            vertex.properties.set(prop_name, prop_value)
        
        # Add index as a property for reference
        vertex.properties.set('_pyg_idx', idx)
        
        idx_to_vertex[idx] = vertex
        nodes_created += 1
    
    # Create edges
    edges_created = 0
    
    for edge_data in edges_data:
        src_idx = edge_data['source_idx']
        dst_idx = edge_data['target_idx']
        
        if src_idx in idx_to_vertex and dst_idx in idx_to_vertex:
            src_vertex = idx_to_vertex[src_idx]
            dst_vertex = idx_to_vertex[dst_idx]
            edge_type = mgp.EdgeType(edge_data.get('type', default_edge_type))
            
            edge = ctx.graph.create_edge(src_vertex, dst_vertex, edge_type)
            
            # Add properties
            for prop_name, prop_value in edge_data.get('properties', {}).items():
                edge.properties.set(prop_name, prop_value)
            
            edges_created += 1
    
    return mgp.Record(nodes_created=nodes_created, edges_created=edges_created)


@mgp.read_proc
def get_node_features(
    ctx: mgp.ProcCtx,
    vertices: mgp.List[mgp.Vertex],
    property_names: mgp.List[str],
) -> mgp.Record(
    node_id=int,
    features=mgp.List[mgp.Number],
):
    """
    Get node features for a list of vertices.
    
    This procedure extracts numeric features from specified properties
    for each vertex, useful for preparing data for GNN training.
    
    Args:
        vertices: List of vertices to extract features from
        property_names: List of property names to include as features
    
    Yields:
        node_id: Original Memgraph node ID
        features: List of numeric feature values
    
    Example:
        ```cypher
        MATCH (n:Person)
        WITH collect(n) AS nodes
        CALL pyg.get_node_features(nodes, ['age', 'income']) YIELD node_id, features
        RETURN node_id, features;
        ```
    """
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
def help(
    ctx: mgp.ProcCtx,
) -> mgp.Record(name=str, description=str):
    """
    Show help information for PyG module procedures.
    
    Returns:
        name: Procedure name
        description: Brief description of the procedure
    """
    procedures = [
        ("pyg.export", "Export entire graph to PyG format with node/edge features"),
        ("pyg.export_subgraph", "Export specific vertices and edges to PyG format"),
        ("pyg.export_to_json", "Export graph to JSON string in PyG format"),
        ("pyg.import_from_dict", "Import PyG data from JSON string into Memgraph"),
        ("pyg.get_node_features", "Extract node features from vertices"),
        ("pyg.help", "Show this help information"),
    ]
    
    return [mgp.Record(name=name, description=desc) for name, desc in procedures]
