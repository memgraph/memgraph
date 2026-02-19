# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the
# Business Source License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""
Converter utilities for Memgraph <-> PyTorch Geometric data format.

This module provides functions to convert between Memgraph graph data and
PyTorch Geometric Data format.
"""

from typing import Any, Dict, List, Optional, Tuple


def _property_to_numeric(value: Any) -> Optional[float]:
    """Convert a property value to a numeric value if possible."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    if isinstance(value, (list, tuple)):
        # Return as-is for list/tuple, they'll be handled separately
        return None
    return None


def _is_numeric_list(value: Any) -> bool:
    """Check if value is a list/tuple of numeric values."""
    if not isinstance(value, (list, tuple)):
        return False
    return all(isinstance(v, (int, float)) for v in value)


class MemgraphPyGConverter:
    """
    Converter class for Memgraph <-> PyTorch Geometric data.

    This class handles conversion between Memgraph graph representation
    and PyTorch Geometric Data format.
    """

    def __init__(self):
        self._node_id_to_idx: Dict[int, int] = {}
        self._idx_to_node_id: Dict[int, int] = {}

    def export_to_pyg_dict(
        self,
        vertices: List[Any],
        edges: List[Any],
        node_property_names: Optional[List[str]] = None,
        edge_property_names: Optional[List[str]] = None,
        node_label_property: Optional[str] = None,
        include_node_id: bool = True,
        include_labels: bool = True,
    ) -> Dict[str, Any]:
        """
        Export Memgraph graph data to a dictionary format compatible with PyTorch Geometric.

        The returned dictionary contains:
        - 'edge_index': List of [source_indices, target_indices] for edges
        - 'x': Node feature matrix (list of lists) if node_property_names provided
        - 'edge_attr': Edge feature matrix (list of lists) if edge_property_names provided
        - 'y': Node labels if node_label_property provided
        - 'num_nodes': Number of nodes
        - 'node_id_mapping': Mapping from original node IDs to indices
        - 'labels': List of label sets per node if include_labels is True
        Args:
            vertices: List of Memgraph Vertex objects
            edges: List of Memgraph Edge objects
            node_property_names: List of property names to include as node features
            edge_property_names: List of property names to include as edge features
            node_label_property: Property name to use as node labels (for classification)
            include_node_id: Whether to include original node IDs in the output
            include_labels: Whether to include vertex labels in the output

        Returns:
            Dictionary with PyG-compatible data structure
        """
        self._node_id_to_idx = {}
        self._idx_to_node_id = {}

        # Build node ID mapping
        for idx, vertex in enumerate(vertices):
            node_id = vertex.id
            self._node_id_to_idx[node_id] = idx
            self._idx_to_node_id[idx] = node_id

        num_nodes = len(vertices)

        # Build edge index
        source_indices = []
        target_indices = []

        included_edges = []
        for edge in edges:
            src_id = edge.from_vertex.id
            dst_id = edge.to_vertex.id

            if src_id in self._node_id_to_idx and dst_id in self._node_id_to_idx:
                source_indices.append(self._node_id_to_idx[src_id])
                target_indices.append(self._node_id_to_idx[dst_id])
                included_edges.append(edge)

        edge_index = [source_indices, target_indices]

        result: Dict[str, Any] = {
            "edge_index": edge_index,
            "num_nodes": num_nodes,
        }

        # Build node features
        if node_property_names:
            x = self._extract_node_features(vertices, node_property_names)
            result["x"] = x

        # Build edge features
        if edge_property_names:
            edge_attr = self._extract_edge_features(included_edges, edge_property_names)
            result["edge_attr"] = edge_attr

        # Build node labels
        if node_label_property:
            y = self._extract_node_labels(vertices, node_label_property)
            result["y"] = y

        # Include node ID mapping
        if include_node_id:
            result["node_id_mapping"] = dict(self._node_id_to_idx)
            result["idx_to_node_id"] = dict(self._idx_to_node_id)

        # Include vertex labels
        if include_labels:
            labels_list = []
            for vertex in vertices:
                vertex_labels = [label.name for label in vertex.labels]
                labels_list.append(vertex_labels)
            result["labels"] = labels_list

        # Include edge types
        edge_types = [edge.type.name for edge in included_edges]
        result["edge_types"] = edge_types

        return result

    def _extract_node_features(
        self,
        vertices: List[Any],
        property_names: List[str],
    ) -> List[List[float]]:
        """Extract node features from vertices based on property names."""
        features = []

        for vertex in vertices:
            node_features = []
            props = vertex.properties

            for prop_name in property_names:
                value = props.get(prop_name, None)

                if _is_numeric_list(value):
                    # Flatten numeric lists into features
                    node_features.extend([float(v) for v in value])
                else:
                    numeric_value = _property_to_numeric(value)
                    node_features.append(numeric_value if numeric_value is not None else 0.0)

            features.append(node_features)

        return features

    def _extract_edge_features(
        self,
        edges: List[Any],
        property_names: List[str],
    ) -> List[List[float]]:
        """Extract edge features from edges based on property names."""
        features = []

        for edge in edges:
            # Only include edges that are in the valid mapping
            src_id = edge.from_vertex.id
            dst_id = edge.to_vertex.id

            if src_id not in self._node_id_to_idx or dst_id not in self._node_id_to_idx:
                continue

            edge_features = []
            props = edge.properties

            for prop_name in property_names:
                value = props.get(prop_name, None)

                if _is_numeric_list(value):
                    edge_features.extend([float(v) for v in value])
                else:
                    numeric_value = _property_to_numeric(value)
                    edge_features.append(numeric_value if numeric_value is not None else 0.0)

            features.append(edge_features)

        return features

    def _extract_node_labels(
        self,
        vertices: List[Any],
        label_property: str,
    ) -> List[Any]:
        """Extract node labels from vertices."""
        labels = []

        for vertex in vertices:
            value = vertex.properties.get(label_property, None)
            labels.append(value)

        return labels

    @staticmethod
    def prepare_import_data(
        pyg_dict: Dict[str, Any],
        default_node_label: str = "Node",
        default_edge_type: str = "CONNECTS",
        node_property_names: Optional[List[str]] = None,
        edge_property_names: Optional[List[str]] = None,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Prepare PyG data dictionary for import into Memgraph.

        This converts the PyG-style data back to a format suitable for
        creating vertices and edges in Memgraph.

        Args:
            pyg_dict: Dictionary with PyG-compatible data (edge_index, x, edge_attr, etc.)
            default_node_label: Default label to use for created nodes
            default_edge_type: Default edge type if not specified in data
            node_property_names: Property names for node features
            edge_property_names: Property names for edge features

        Returns:
            Tuple of (nodes_data, edges_data) where each is a list of dicts
        """
        # Validate required fields
        if "edge_index" not in pyg_dict:
            raise ValueError("Missing required field: 'edge_index'")
        if "num_nodes" not in pyg_dict:
            raise ValueError("Missing required field: 'num_nodes'")

        edge_index = pyg_dict.get("edge_index", [[], []])
        num_nodes = pyg_dict.get("num_nodes", 0)
        x = pyg_dict.get("x", None)
        edge_attr = pyg_dict.get("edge_attr", None)
        y = pyg_dict.get("y", None)
        labels = pyg_dict.get("labels", None)
        edge_types = pyg_dict.get("edge_types", None)
        idx_to_node_id = pyg_dict.get("idx_to_node_id", None)

        # Prepare nodes data
        nodes_data = []
        for idx in range(num_nodes):
            node_data: Dict[str, Any] = {
                "idx": idx,
            }

            # Set labels
            if labels and idx < len(labels) and labels[idx]:
                node_data["labels"] = labels[idx]
            else:
                node_data["labels"] = [default_node_label]

            # Set properties from features
            if x is not None and idx < len(x):
                node_features = x[idx]
                if node_property_names:
                    props = {}
                    for i, prop_name in enumerate(node_property_names):
                        if i < len(node_features):
                            props[prop_name] = node_features[i]
                    node_data["properties"] = props
                else:
                    node_data["properties"] = {"features": node_features}
            else:
                node_data["properties"] = {}

            # Set label property
            if y is not None and idx < len(y) and y[idx] is not None:
                node_data["properties"]["y"] = y[idx]

            # Set original ID if available
            if idx_to_node_id and idx in idx_to_node_id:
                node_data["original_id"] = idx_to_node_id[idx]

            nodes_data.append(node_data)

        # Prepare edges data
        edges_data = []
        source_indices = edge_index[0] if len(edge_index) > 0 else []
        target_indices = edge_index[1] if len(edge_index) > 1 else []

        for edge_idx, (src_idx, dst_idx) in enumerate(zip(source_indices, target_indices)):
            edge_data: Dict[str, Any] = {
                "source_idx": src_idx,
                "target_idx": dst_idx,
            }

            # Set edge type
            if edge_types and edge_idx < len(edge_types):
                edge_data["type"] = edge_types[edge_idx]
            else:
                edge_data["type"] = default_edge_type

            # Set properties from edge features
            if edge_attr is not None and edge_idx < len(edge_attr):
                edge_features = edge_attr[edge_idx]
                if edge_property_names:
                    props = {}
                    for i, prop_name in enumerate(edge_property_names):
                        if i < len(edge_features):
                            props[prop_name] = edge_features[i]
                    edge_data["properties"] = props
                else:
                    edge_data["properties"] = {"features": edge_features}
            else:
                edge_data["properties"] = {}

            edges_data.append(edge_data)

        return nodes_data, edges_data


def graph_to_pyg_dict(
    vertices: List[Any],
    edges: List[Any],
    node_property_names: Optional[List[str]] = None,
    edge_property_names: Optional[List[str]] = None,
    node_label_property: Optional[str] = None,
    include_node_id: bool = True,
    include_labels: bool = True,
) -> Dict[str, Any]:
    """
    Convert Memgraph graph data to PyTorch Geometric compatible dictionary.

    This is a convenience function that wraps MemgraphPyGConverter.

    Args:
        vertices: List of Memgraph Vertex objects
        edges: List of Memgraph Edge objects
        node_property_names: List of property names to include as node features
        edge_property_names: List of property names to include as edge features
        node_label_property: Property name to use as node labels (for classification)
        include_node_id: Whether to include original node IDs in the output
        include_labels: Whether to include vertex labels in the output

    Returns:
        Dictionary with PyG-compatible data structure
    """
    converter = MemgraphPyGConverter()
    return converter.export_to_pyg_dict(
        vertices=vertices,
        edges=edges,
        node_property_names=node_property_names,
        edge_property_names=edge_property_names,
        node_label_property=node_label_property,
        include_node_id=include_node_id,
        include_labels=include_labels,
    )


def pyg_dict_to_graph_data(
    pyg_dict: Dict[str, Any],
    default_node_label: str = "Node",
    default_edge_type: str = "CONNECTS",
    node_property_names: Optional[List[str]] = None,
    edge_property_names: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Prepare PyG data dictionary for import into Memgraph.

    This is a convenience function that wraps MemgraphPyGConverter.prepare_import_data.

    Args:
        pyg_dict: Dictionary with PyG-compatible data
        default_node_label: Default label to use for created nodes
        default_edge_type: Default edge type if not specified in data
        node_property_names: Property names for node features
        edge_property_names: Property names for edge features

    Returns:
        Tuple of (nodes_data, edges_data)
    """
    return MemgraphPyGConverter.prepare_import_data(
        pyg_dict=pyg_dict,
        default_node_label=default_node_label,
        default_edge_type=default_edge_type,
        node_property_names=node_property_names,
        edge_property_names=edge_property_names,
    )
