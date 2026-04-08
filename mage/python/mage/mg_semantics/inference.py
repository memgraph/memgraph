from typing import Any, Dict, List, Optional, Set

import mgp


class InferenceEngine:
    @staticmethod
    def nodes_labelled(
        ctx: mgp.ProcCtx,
        label: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[mgp.Vertex]:
        sub_rel = (params or {}).get("subLabelRel", "SCO")
        all_labels = _collect_sublabels(ctx, label, sub_rel)
        all_labels.add(label)

        results = []
        for vertex in ctx.graph.vertices:
            vertex_labels = {l.name for l in vertex.labels}
            if vertex_labels & all_labels:
                results.append(vertex)
        return results

    @staticmethod
    def nodes_in_category(
        ctx: mgp.ProcCtx,
        category_node: mgp.Vertex,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[mgp.Vertex]:
        cat_rel = (params or {}).get("catRel", "IN_CATEGORY")
        sub_cat_rel = (params or {}).get("subCatRel", "SCO")

        all_categories = _collect_subcategories(category_node, sub_cat_rel)
        all_categories.add(category_node)

        results = []
        for vertex in ctx.graph.vertices:
            for edge in vertex.out_edges:
                if edge.type.name == cat_rel and edge.to_vertex in all_categories:
                    results.append(vertex)
                    break
        return results

    @staticmethod
    def get_rels(
        ctx: mgp.ProcCtx,
        start_node: mgp.Vertex,
        rel_type: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        sub_rel = (params or {}).get("subRelRel", "SPO")
        all_rel_types = _collect_subrel_types(ctx, rel_type, sub_rel)
        all_rel_types.add(rel_type)

        results = []
        for edge in start_node.out_edges:
            if edge.type.name in all_rel_types:
                results.append(
                    {
                        "rel": edge,
                        "node": edge.to_vertex,
                    }
                )
        return results

    @staticmethod
    def has_label(
        ctx: mgp.ProcCtx,
        node: mgp.Vertex,
        label: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> bool:
        sub_rel = (params or {}).get("subLabelRel", "SCO")
        all_labels = _collect_sublabels(ctx, label, sub_rel)
        all_labels.add(label)

        vertex_labels = {l.name for l in node.labels}
        return bool(vertex_labels & all_labels)

    @staticmethod
    def in_category(
        ctx: mgp.ProcCtx,
        node: mgp.Vertex,
        category_node: mgp.Vertex,
        params: Optional[Dict[str, Any]] = None,
    ) -> bool:
        cat_rel = (params or {}).get("catRel", "IN_CATEGORY")
        sub_cat_rel = (params or {}).get("subCatRel", "SCO")

        all_categories = _collect_subcategories(category_node, sub_cat_rel)
        all_categories.add(category_node)

        for edge in node.out_edges:
            if edge.type.name == cat_rel and edge.to_vertex in all_categories:
                return True
        return False


def _collect_sublabels(ctx: mgp.ProcCtx, label: str, sub_rel: str) -> Set[str]:
    sublabels = set()
    visited_uris = set()
    queue = [label]

    uri_to_vertex = {}
    label_to_uris = {}

    for vertex in ctx.graph.vertices:
        uri = vertex.properties.get("uri")
        if uri:
            uri_to_vertex[uri] = vertex
        for lbl in vertex.labels:
            if lbl.name not in label_to_uris:
                label_to_uris[lbl.name] = set()
            label_to_uris[lbl.name].add(vertex)

    while queue:
        current_label = queue.pop(0)
        if current_label in visited_uris:
            continue
        visited_uris.add(current_label)

        for vertex in ctx.graph.vertices:
            for in_edge in vertex.in_edges:
                if in_edge.type.name == sub_rel:
                    parent_labels = {l.name for l in in_edge.to_vertex.labels}
                    child_labels = {l.name for l in vertex.labels}
                    if current_label in parent_labels:
                        sublabels.update(child_labels - {"Resource"})
                        for cl in child_labels:
                            if cl not in visited_uris and cl != "Resource":
                                queue.append(cl)

    return sublabels


def _collect_subcategories(category_node: mgp.Vertex, sub_cat_rel: str) -> Set[mgp.Vertex]:
    subcategories = set()
    queue = [category_node]
    visited = set()

    while queue:
        current = queue.pop(0)
        current_id = current.id
        if current_id in visited:
            continue
        visited.add(current_id)

        for edge in current.in_edges:
            if edge.type.name == sub_cat_rel:
                child = edge.from_vertex
                if child.id not in visited:
                    subcategories.add(child)
                    queue.append(child)

    return subcategories


def _collect_subrel_types(ctx: mgp.ProcCtx, rel_type: str, sub_rel: str) -> Set[str]:
    sub_types = set()
    visited = set()
    queue = [rel_type]

    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)

        for vertex in ctx.graph.vertices:
            vertex_name = vertex.properties.get("uri", "")
            if vertex_name.endswith(current) or current in {l.name for l in vertex.labels}:
                for in_edge in vertex.in_edges:
                    if in_edge.type.name == sub_rel:
                        child_uri = in_edge.from_vertex.properties.get("uri", "")
                        child_local = child_uri.rsplit("/", 1)[-1].rsplit("#", 1)[-1]
                        if child_local and child_local not in visited:
                            sub_types.add(child_local)
                            queue.append(child_local)

    return sub_types
