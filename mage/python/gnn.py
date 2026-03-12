# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this
# file, you agree to be bound by the terms of the Business
# Source License, and you may not use this file except in
# compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""
GNN integration module for Memgraph.

Provides export/import procedures for PyTorch Geometric (PyG) and
TensorFlow GNN (TF-GNN) formats. All exports produce a single JSON
string that can be deserialized on the client side and fed into the
respective framework.

Example Cypher usage::

    CALL gnn.pyg_export(['feat'], ['weight'], 'class')
    YIELD json_data;

    CALL gnn.pyg_import($json_data, 'Node', 'EDGE')
    YIELD nodes_created, edges_created;

    CALL gnn.tf_export(['score'], ['weight'])
    YIELD json_data;

    CALL gnn.tf_import($json_data, 'Node', 'EDGE')
    YIELD nodes_created, edges_created;
"""

import json
from typing import Any, Dict, List, Optional, Tuple

import mgp

# ---- shared helpers ------------------------------------------------


def _collect_vertices_and_edges(
    ctx: mgp.ProcCtx,
) -> Tuple[List[mgp.Vertex], List[mgp.Edge]]:
    vertices = list(ctx.graph.vertices)
    edges = []
    for vertex in vertices:
        for edge in vertex.out_edges:
            edges.append(edge)
    return vertices, edges


def _property_to_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _is_numeric_list(value: Any) -> bool:
    if not isinstance(value, (list, tuple)):
        return False
    return all(isinstance(v, (int, float)) for v in value)


def _extract_features(
    items: List[Any],
    property_names: List[str],
) -> List[List[float]]:
    features = []
    for item in items:
        row: List[float] = []
        props = item.properties
        for name in property_names:
            value = props.get(name, None)
            if _is_numeric_list(value):
                row.extend(float(v) for v in value)
            else:
                num = _property_to_numeric(value)
                row.append(num if num is not None else 0.0)
        features.append(row)
    return features


def _collect_feature_values(
    items: List[Any],
    property_names: List[str],
) -> Dict[str, List[Any]]:
    features: Dict[str, List[Any]] = {n: [] for n in property_names}
    for item in items:
        props = item.properties
        for name in property_names:
            features[name].append(props.get(name, None))
    return features


# ---- PyG helpers ---------------------------------------------------


def _pyg_export_dict(
    vertices: List[Any],
    edges: List[Any],
    node_property_names: Optional[List[str]],
    edge_property_names: Optional[List[str]],
    node_label_property: Optional[str],
) -> Dict[str, Any]:
    id_to_idx: Dict[int, int] = {}
    for idx, v in enumerate(vertices):
        id_to_idx[v.id] = idx

    src_indices: List[int] = []
    dst_indices: List[int] = []
    included_edges: List[Any] = []
    for edge in edges:
        s = edge.from_vertex.id
        d = edge.to_vertex.id
        if s in id_to_idx and d in id_to_idx:
            src_indices.append(id_to_idx[s])
            dst_indices.append(id_to_idx[d])
            included_edges.append(edge)

    result: Dict[str, Any] = {
        "edge_index": [src_indices, dst_indices],
        "num_nodes": len(vertices),
    }

    if node_property_names:
        result["x"] = _extract_features(vertices, node_property_names)

    if edge_property_names:
        result["edge_attr"] = _extract_features(included_edges, edge_property_names)

    if node_label_property:
        result["y"] = [v.properties.get(node_label_property, None) for v in vertices]

    result["node_id_mapping"] = {str(k): v for k, v in id_to_idx.items()}
    result["idx_to_node_id"] = {str(v): k for k, v in id_to_idx.items()}
    result["labels"] = [[lb.name for lb in v.labels] for v in vertices]
    result["edge_types"] = [e.type.name for e in included_edges]
    return result


def _pyg_import_data(
    pyg_dict: Dict[str, Any],
    default_node_label: str,
    default_edge_type: str,
    node_property_names: Optional[List[str]],
    edge_property_names: Optional[List[str]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    edge_index = pyg_dict.get("edge_index", [[], []])
    num_nodes = pyg_dict.get("num_nodes", 0)
    x = pyg_dict.get("x")
    edge_attr = pyg_dict.get("edge_attr")
    y = pyg_dict.get("y")
    labels = pyg_dict.get("labels")
    edge_types = pyg_dict.get("edge_types")

    nodes_data: List[Dict[str, Any]] = []
    for idx in range(num_nodes):
        nd: Dict[str, Any] = {"idx": idx}
        if labels and idx < len(labels) and labels[idx]:
            nd["labels"] = labels[idx]
        else:
            nd["labels"] = [default_node_label]

        props: Dict[str, Any] = {}
        if x is not None and idx < len(x):
            feats = x[idx]
            if node_property_names:
                for i, n in enumerate(node_property_names):
                    if i < len(feats):
                        props[n] = feats[i]
            else:
                props["features"] = feats
        if y is not None and idx < len(y) and y[idx] is not None:
            props["y"] = y[idx]
        nd["properties"] = props
        nodes_data.append(nd)

    src = edge_index[0] if edge_index else []
    dst = edge_index[1] if len(edge_index) > 1 else []
    edges_data: List[Dict[str, Any]] = []
    for ei, (si, di) in enumerate(zip(src, dst)):
        ed: Dict[str, Any] = {
            "source_idx": si,
            "target_idx": di,
            "type": (edge_types[ei] if edge_types and ei < len(edge_types) else default_edge_type),
        }
        props = {}
        if edge_attr is not None and ei < len(edge_attr):
            feats = edge_attr[ei]
            if edge_property_names:
                for i, n in enumerate(edge_property_names):
                    if i < len(feats):
                        props[n] = feats[i]
            else:
                props["features"] = feats
        ed["properties"] = props
        edges_data.append(ed)

    return nodes_data, edges_data


# ---- TF-GNN helpers -----------------------------------------------


def _tf_infer_dtype(values: List[Any]) -> str:
    has_string = has_float = has_int = has_bool = False
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            has_bool = True
        elif isinstance(value, int):
            has_int = True
        elif isinstance(value, float):
            has_float = True
        elif isinstance(value, str):
            has_string = True
        elif isinstance(value, (list, tuple)):
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


def _tf_infer_shape(values: List[Any]) -> List[int]:
    lengths: List[int] = []
    for value in values:
        if isinstance(value, (list, tuple)):
            if any(isinstance(i, (list, tuple)) for i in value):
                return [-1]
            lengths.append(len(value))
    if not lengths:
        return []
    if all(ln == lengths[0] for ln in lengths):
        return [lengths[0]]
    return [-1]


def _tf_build_feature_schema(
    features: Dict[str, List[Any]],
) -> Dict[str, Dict[str, Any]]:
    return {
        name: {
            "dtype": _tf_infer_dtype(vals),
            "shape": _tf_infer_shape(vals),
        }
        for name, vals in features.items()
    }


def _tf_export_dict(
    vertices: List[Any],
    edges: List[Any],
    node_property_names: Optional[List[str]],
    edge_property_names: Optional[List[str]],
    node_set_name: str,
    edge_set_name: str,
) -> Dict[str, Any]:
    id_to_idx = {v.id: idx for idx, v in enumerate(vertices)}

    src_indices: List[int] = []
    dst_indices: List[int] = []
    included_edges: List[Any] = []
    for edge in edges:
        s = edge.from_vertex.id
        d = edge.to_vertex.id
        if s in id_to_idx and d in id_to_idx:
            src_indices.append(id_to_idx[s])
            dst_indices.append(id_to_idx[d])
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
                "features": _tf_build_feature_schema(node_features),
            },
        },
        "edge_sets": {
            edge_set_name: {
                "source": node_set_name,
                "target": node_set_name,
                "features": _tf_build_feature_schema(edge_features),
            },
        },
        "context": {"features": {}},
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
                "sizes": [len(src_indices)],
                "adjacency": {
                    "source": {
                        "node_set_name": node_set_name,
                        "indices": src_indices,
                    },
                    "target": {
                        "node_set_name": node_set_name,
                        "indices": dst_indices,
                    },
                },
            },
        },
        "context": {"features": {}},
    }

    return {"schema": schema, "graph": graph}


def _tf_import_data(
    tfgnn_dict: Dict[str, Any],
    default_node_label: str,
    default_edge_type: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    graph = tfgnn_dict.get("graph", tfgnn_dict)
    node_sets = graph.get("node_sets", {})
    edge_sets = graph.get("edge_sets", {})

    nodes_data: List[Dict[str, Any]] = []
    for ns_name, ns_data in node_sets.items():
        sizes = ns_data.get("sizes", [])
        size = sizes[0] if sizes else 0
        feats = ns_data.get("features", {})
        if size == 0 and feats:
            size = max(
                (len(v) for v in feats.values()),
                default=0,
            )
        for idx in range(size):
            props: Dict[str, Any] = {}
            for fname, vals in feats.items():
                if idx < len(vals) and vals[idx] is not None:
                    props[fname] = vals[idx]
            nodes_data.append(
                {
                    "idx": idx,
                    "node_set": ns_name,
                    "labels": [ns_name or default_node_label],
                    "properties": props,
                }
            )

    edges_data: List[Dict[str, Any]] = []
    for es_name, es_data in edge_sets.items():
        adj = es_data.get("adjacency", {})
        src_info = adj.get("source", {})
        dst_info = adj.get("target", {})
        src_set = src_info.get("node_set_name", default_node_label)
        dst_set = dst_info.get("node_set_name", default_node_label)
        src_idx = src_info.get("indices", [])
        dst_idx = dst_info.get("indices", [])
        feats = es_data.get("features", {})
        for ei, (si, di) in enumerate(zip(src_idx, dst_idx)):
            props: Dict[str, Any] = {}
            for fname, vals in feats.items():
                if ei < len(vals) and vals[ei] is not None:
                    props[fname] = vals[ei]
            edges_data.append(
                {
                    "source_idx": si,
                    "target_idx": di,
                    "source_set": src_set,
                    "target_set": dst_set,
                    "type": es_name or default_edge_type,
                    "properties": props,
                }
            )

    return nodes_data, edges_data


# ---- Procedures ----------------------------------------------------


@mgp.read_proc
def pyg_export(
    ctx: mgp.ProcCtx,
    node_property_names: mgp.Nullable[mgp.List[str]] = None,
    edge_property_names: mgp.Nullable[mgp.List[str]] = None,
    node_label_property: mgp.Nullable[str] = None,
) -> mgp.Record(json_data=str):
    """Export the graph to a JSON string in PyG format."""
    vertices, edges = _collect_vertices_and_edges(ctx)
    result = _pyg_export_dict(
        vertices,
        edges,
        (list(node_property_names) if node_property_names else None),
        (list(edge_property_names) if edge_property_names else None),
        node_label_property or None,
    )
    return mgp.Record(json_data=json.dumps(result))


@mgp.write_proc
def pyg_import(
    ctx: mgp.ProcCtx,
    json_data: str,
    default_node_label: str = "PyGNode",
    default_edge_type: str = "CONNECTS",
    node_property_names: mgp.Nullable[mgp.List[str]] = None,
    edge_property_names: mgp.Nullable[mgp.List[str]] = None,
) -> mgp.Record(nodes_created=int, edges_created=int):
    """Import PyG data from a JSON string."""
    pyg_dict = json.loads(json_data)
    nodes_data, edges_data = _pyg_import_data(
        pyg_dict,
        default_node_label,
        default_edge_type,
        (list(node_property_names) if node_property_names else None),
        (list(edge_property_names) if edge_property_names else None),
    )

    idx_to_vertex: Dict[int, mgp.Vertex] = {}
    nodes_created = 0
    for nd in nodes_data:
        vertex = ctx.graph.create_vertex()
        idx = nd["idx"]
        for label in nd.get("labels", [default_node_label]):
            vertex.add_label(label)
        for k, v in nd.get("properties", {}).items():
            vertex.properties.set(k, v)
        vertex.properties.set("_pyg_idx", idx)
        idx_to_vertex[idx] = vertex
        nodes_created += 1

    edges_created = 0
    for ed in edges_data:
        si, di = ed["source_idx"], ed["target_idx"]
        if si in idx_to_vertex and di in idx_to_vertex:
            e = ctx.graph.create_edge(
                idx_to_vertex[si],
                idx_to_vertex[di],
                mgp.EdgeType(ed.get("type", default_edge_type)),
            )
            for k, v in ed.get("properties", {}).items():
                e.properties.set(k, v)
            edges_created += 1

    return mgp.Record(
        nodes_created=nodes_created,
        edges_created=edges_created,
    )


@mgp.read_proc
def tf_export(
    ctx: mgp.ProcCtx,
    node_property_names: mgp.Nullable[mgp.List[str]] = None,
    edge_property_names: mgp.Nullable[mgp.List[str]] = None,
    node_set_name: str = "node",
    edge_set_name: str = "edge",
) -> mgp.Record(json_data=str):
    """Export the graph to a JSON string in TF-GNN format."""
    vertices, edges = _collect_vertices_and_edges(ctx)
    result = _tf_export_dict(
        vertices,
        edges,
        (list(node_property_names) if node_property_names else None),
        (list(edge_property_names) if edge_property_names else None),
        node_set_name,
        edge_set_name,
    )
    return mgp.Record(json_data=json.dumps(result))


@mgp.write_proc
def tf_import(
    ctx: mgp.ProcCtx,
    json_data: str,
    default_node_label: str = "TfGnnNode",
    default_edge_type: str = "CONNECTS",
) -> mgp.Record(nodes_created=int, edges_created=int):
    """Import TF-GNN data from a JSON string."""
    tfgnn_dict = json.loads(json_data)
    nodes_data, edges_data = _tf_import_data(tfgnn_dict, default_node_label, default_edge_type)

    nodes_created = 0
    node_lookup: Dict[tuple, mgp.Vertex] = {}
    for nd in nodes_data:
        vertex = ctx.graph.create_vertex()
        idx = nd["idx"]
        ns = nd.get("node_set", "")
        for label in nd.get("labels", [default_node_label]):
            vertex.add_label(label)
        for k, v in nd.get("properties", {}).items():
            vertex.properties.set(k, v)
        vertex.properties.set("_tfgnn_idx", idx)
        if ns:
            vertex.properties.set("_tfgnn_node_set", ns)
        node_lookup[(ns, idx)] = vertex
        nodes_created += 1

    edges_created = 0
    for ed in edges_data:
        si, di = ed["source_idx"], ed["target_idx"]
        ss = ed.get("source_set", "")
        ds = ed.get("target_set", "")
        sv = node_lookup.get((ss, si))
        dv = node_lookup.get((ds, di))
        if sv is None or dv is None:
            continue
        e = ctx.graph.create_edge(
            sv,
            dv,
            mgp.EdgeType(ed.get("type", default_edge_type)),
        )
        for k, v in ed.get("properties", {}).items():
            e.properties.set(k, v)
        edges_created += 1

    return mgp.Record(
        nodes_created=nodes_created,
        edges_created=edges_created,
    )
