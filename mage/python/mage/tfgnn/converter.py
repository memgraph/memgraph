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
Converter utilities for Memgraph <-> TF-GNN data format.

This module provides functions to convert between Memgraph graph data and a
TF-GNN-inspired dictionary format that contains both schema and graph data.
"""

from typing import Any, Dict, List, Optional, Tuple


def _infer_dtype(values: List[Any]) -> str:
    """Infer TF-GNN schema dtype from a list of feature values."""
    has_string = False
    has_float = False
    has_int = False
    has_bool = False

    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            has_bool = True
            continue
        if isinstance(value, int):
            has_int = True
            continue
        if isinstance(value, float):
            has_float = True
            continue
        if isinstance(value, str):
            has_string = True
            continue
        if isinstance(value, (list, tuple)):
            for item in value:
                if item is None:
                    continue
                if isinstance(item, bool):
                    has_bool = True
                elif isinstance(item, int):
                    has_int = True
                elif isinstance(item, float):
                    has_float = True
                elif isinstance(item, str):
                    has_string = True
        else:
            has_string = True

    if has_string:
        return "DT_STRING"
    if has_float:
        return "DT_FLOAT"
    if has_bool and not has_int:
        return "DT_BOOL"
    if has_int or has_bool:
        return "DT_INT64"
    return "DT_STRING"


def _infer_shape(values: List[Any]) -> List[int]:
    """Infer TF-GNN schema feature shape from a list of feature values."""
    lengths = []
    for value in values:
        if isinstance(value, (list, tuple)):
            if any(isinstance(item, (list, tuple)) for item in value):
                return [-1]
            lengths.append(len(value))

    if not lengths:
        return []
    if all(length == lengths[0] for length in lengths):
        return [lengths[0]]
    return [-1]


def _collect_feature_values(items: List[Any], property_names: List[str]) -> Dict[str, List[Any]]:
    """Collect feature values for each property name from graph items."""
    features: Dict[str, List[Any]] = {name: [] for name in property_names}
    for item in items:
        props = item.properties
        for name in property_names:
            features[name].append(props.get(name, None))
    return features


def _build_feature_schema(features: Dict[str, List[Any]]) -> Dict[str, Dict[str, Any]]:
    """Build TF-GNN schema feature entries from collected feature values."""
    schema: Dict[str, Dict[str, Any]] = {}
    for name, values in features.items():
        schema[name] = {
            "dtype": _infer_dtype(values),
            "shape": _infer_shape(values),
        }
    return schema


class MemgraphTfGnnConverter:
    """Converter class for Memgraph <-> TF-GNN data."""

    def __init__(self) -> None:
        self._node_id_to_idx: Dict[int, int] = {}

    def export_to_tfgnn_dict(
        self,
        vertices: List[Any],
        edges: List[Any],
        node_property_names: Optional[List[str]] = None,
        edge_property_names: Optional[List[str]] = None,
        node_set_name: str = "node",
        edge_set_name: str = "edge",
    ) -> Dict[str, Any]:
        """Export Memgraph graph data to a TF-GNN-inspired dictionary."""
        self._node_id_to_idx = {vertex.id: idx for idx, vertex in enumerate(vertices)}

        source_indices: List[int] = []
        target_indices: List[int] = []
        included_edges: List[Any] = []

        for edge in edges:
            src_id = edge.from_vertex.id
            dst_id = edge.to_vertex.id
            if src_id in self._node_id_to_idx and dst_id in self._node_id_to_idx:
                source_indices.append(self._node_id_to_idx[src_id])
                target_indices.append(self._node_id_to_idx[dst_id])
                included_edges.append(edge)

        node_features: Dict[str, List[Any]] = {}
        edge_features: Dict[str, List[Any]] = {}

        if node_property_names:
            node_features = _collect_feature_values(vertices, node_property_names)

        if edge_property_names:
            edge_features = _collect_feature_values(included_edges, edge_property_names)

        schema = {
            "node_sets": {
                node_set_name: {
                    "features": _build_feature_schema(node_features),
                },
            },
            "edge_sets": {
                edge_set_name: {
                    "source": node_set_name,
                    "target": node_set_name,
                    "features": _build_feature_schema(edge_features),
                },
            },
            "context": {
                "features": {},
            },
        }

        graph = {
            "node_sets": {
                node_set_name: {
                    "features": node_features,
                    "sizes": [len(vertices)],
                },
            },
            "edge_sets": {
                edge_set_name: {
                    "features": edge_features,
                    "sizes": [len(source_indices)],
                    "adjacency": {
                        "source": {
                            "node_set_name": node_set_name,
                            "indices": source_indices,
                        },
                        "target": {
                            "node_set_name": node_set_name,
                            "indices": target_indices,
                        },
                    },
                },
            },
            "context": {
                "features": {},
            },
        }

        return {
            "schema": schema,
            "graph": graph,
        }

    @staticmethod
    def prepare_import_data(
        tfgnn_dict: Dict[str, Any],
        default_node_label: str = "TfGnnNode",
        default_edge_type: str = "CONNECTS",
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Prepare TF-GNN data dictionary for import into Memgraph."""
        graph = tfgnn_dict.get("graph", tfgnn_dict)
        node_sets = graph.get("node_sets", {})
        edge_sets = graph.get("edge_sets", {})

        nodes_data: List[Dict[str, Any]] = []
        for node_set_name, node_set_data in node_sets.items():
            sizes = node_set_data.get("sizes", [])
            size = sizes[0] if sizes else 0
            features = node_set_data.get("features", {})
            if size == 0 and features:
                size = max((len(values) for values in features.values()), default=0)

            for idx in range(size):
                props: Dict[str, Any] = {}
                for feature_name, values in features.items():
                    if idx < len(values) and values[idx] is not None:
                        props[feature_name] = values[idx]

                nodes_data.append(
                    {
                        "idx": idx,
                        "node_set": node_set_name,
                        "labels": [node_set_name or default_node_label],
                        "properties": props,
                    }
                )

        edges_data: List[Dict[str, Any]] = []
        for edge_set_name, edge_set_data in edge_sets.items():
            adjacency = edge_set_data.get("adjacency", {})
            source = adjacency.get("source", {})
            target = adjacency.get("target", {})
            source_set = source.get("node_set_name", default_node_label)
            target_set = target.get("node_set_name", default_node_label)
            source_indices = source.get("indices", [])
            target_indices = target.get("indices", [])
            features = edge_set_data.get("features", {})

            for edge_idx, (src_idx, dst_idx) in enumerate(zip(source_indices, target_indices)):
                props: Dict[str, Any] = {}
                for feature_name, values in features.items():
                    if edge_idx < len(values) and values[edge_idx] is not None:
                        props[feature_name] = values[edge_idx]

                edges_data.append(
                    {
                        "source_idx": src_idx,
                        "target_idx": dst_idx,
                        "source_set": source_set,
                        "target_set": target_set,
                        "type": edge_set_name or default_edge_type,
                        "properties": props,
                    }
                )

        return nodes_data, edges_data


def tfgnn_dict_to_graph_data(
    tfgnn_dict: Dict[str, Any],
    default_node_label: str = "TfGnnNode",
    default_edge_type: str = "CONNECTS",
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Prepare TF-GNN data dictionary for import into Memgraph."""
    return MemgraphTfGnnConverter.prepare_import_data(
        tfgnn_dict=tfgnn_dict,
        default_node_label=default_node_label,
        default_edge_type=default_edge_type,
    )
