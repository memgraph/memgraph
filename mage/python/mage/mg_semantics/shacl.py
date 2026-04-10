from typing import Any, Dict, List, Optional

import mgp
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, XSD

SH = Namespace("http://www.w3.org/ns/shacl#")

FORMAT_MAP = {
    "Turtle": "turtle",
    "N-Triples": "nt",
    "JSON-LD": "json-ld",
    "TriG": "trig",
    "RDF/XML": "xml",
    "N3": "n3",
}


def _resolve_format(fmt: str) -> str:
    if fmt in FORMAT_MAP:
        return FORMAT_MAP[fmt]
    lower = fmt.lower()
    for key, val in FORMAT_MAP.items():
        if key.lower() == lower or val == lower:
            return val
    raise ValueError(f"Unsupported RDF format: {fmt}")


class ShaclValidator:
    _shapes_graph: Optional[Graph] = None

    @classmethod
    def reset(cls) -> None:
        cls._shapes_graph = None

    @classmethod
    def import_shapes_from_url(cls, url: str, fmt: str) -> int:
        from urllib.request import Request, urlopen

        rdf_format = _resolve_format(fmt)
        req = Request(url)
        req.add_header("User-Agent", "mg_semantics/1.0")
        with urlopen(req) as response:
            data = response.read().decode("utf-8")

        if cls._shapes_graph is None:
            cls._shapes_graph = Graph()
        cls._shapes_graph.parse(data=data, format=rdf_format)
        return len(list(cls._shapes_graph.subjects(RDF.type, SH.NodeShape)))

    @classmethod
    def import_shapes_from_text(cls, rdf_text: str, fmt: str) -> int:
        rdf_format = _resolve_format(fmt)
        if cls._shapes_graph is None:
            cls._shapes_graph = Graph()
        cls._shapes_graph.parse(data=rdf_text, format=rdf_format)
        return len(list(cls._shapes_graph.subjects(RDF.type, SH.NodeShape)))

    @classmethod
    def list_shapes(cls) -> List[Dict[str, Any]]:
        if cls._shapes_graph is None:
            return []

        shapes = []
        for shape in cls._shapes_graph.subjects(RDF.type, SH.NodeShape):
            shape_info = {"shape": str(shape), "properties": []}

            target_class = cls._shapes_graph.value(shape, SH.targetClass)
            if target_class:
                shape_info["targetClass"] = str(target_class)

            target_node = cls._shapes_graph.value(shape, SH.targetNode)
            if target_node:
                shape_info["targetNode"] = str(target_node)

            for prop_shape in cls._shapes_graph.objects(shape, SH.property):
                prop_info = {}
                path = cls._shapes_graph.value(prop_shape, SH.path)
                if path:
                    prop_info["path"] = str(path)
                datatype = cls._shapes_graph.value(prop_shape, SH.datatype)
                if datatype:
                    prop_info["datatype"] = str(datatype)
                min_count = cls._shapes_graph.value(prop_shape, SH.minCount)
                if min_count:
                    prop_info["minCount"] = int(min_count)
                max_count = cls._shapes_graph.value(prop_shape, SH.maxCount)
                if max_count:
                    prop_info["maxCount"] = int(max_count)
                node_kind = cls._shapes_graph.value(prop_shape, SH.nodeKind)
                if node_kind:
                    prop_info["nodeKind"] = str(node_kind)
                pattern = cls._shapes_graph.value(prop_shape, SH.pattern)
                if pattern:
                    prop_info["pattern"] = str(pattern)
                shape_info["properties"].append(prop_info)

            shapes.append(shape_info)
        return shapes

    @classmethod
    def validate(cls, ctx: mgp.ProcCtx) -> List[Dict[str, Any]]:
        if cls._shapes_graph is None:
            raise RuntimeError("No SHACL shapes loaded. Import shapes first.")

        data_graph = _memgraph_to_rdflib(ctx)
        return _run_validation(data_graph, cls._shapes_graph)

    @classmethod
    def validate_set(
        cls,
        ctx: mgp.ProcCtx,
        node_uris: List[str],
    ) -> List[Dict[str, Any]]:
        if cls._shapes_graph is None:
            raise RuntimeError("No SHACL shapes loaded. Import shapes first.")

        data_graph = _memgraph_to_rdflib(ctx, filter_uris=set(node_uris))
        return _run_validation(data_graph, cls._shapes_graph)


def _memgraph_to_rdflib(
    ctx: mgp.ProcCtx,
    filter_uris: Optional[set] = None,
) -> Graph:
    g = Graph()

    for vertex in ctx.graph.vertices:
        uri = vertex.properties.get("uri")
        if not uri:
            continue
        if filter_uris and uri not in filter_uris:
            continue

        subj = URIRef(uri)

        for label in vertex.labels:
            label_name = label.name
            if label_name == "Resource":
                continue
            if "__" in label_name:
                prefix, local = label_name.split("__", 1)
                from mage.mg_semantics.namespaces import NamespaceManager

                prefixes = NamespaceManager.get_prefixes()
                if prefix in prefixes:
                    type_uri = URIRef(prefixes[prefix] + local)
                else:
                    type_uri = URIRef(label_name)
            else:
                type_uri = URIRef(label_name)
            g.add((subj, RDF.type, type_uri))

        for key in vertex.properties.keys():
            if key == "uri":
                continue
            value = vertex.properties.get(key)
            if value is not None:
                if isinstance(value, bool):
                    g.add((subj, URIRef(key), Literal(value, datatype=XSD.boolean)))
                elif isinstance(value, int):
                    g.add((subj, URIRef(key), Literal(value, datatype=XSD.integer)))
                elif isinstance(value, float):
                    g.add((subj, URIRef(key), Literal(value, datatype=XSD.double)))
                else:
                    g.add((subj, URIRef(key), Literal(str(value))))

        for edge in vertex.out_edges:
            target_uri = edge.to_vertex.properties.get("uri")
            if target_uri:
                g.add((subj, URIRef(edge.type.name), URIRef(target_uri)))

    return g


def _run_validation(data_graph: Graph, shapes_graph: Graph) -> List[Dict[str, Any]]:
    try:
        from pyshacl import validate as shacl_validate
    except ImportError:
        raise RuntimeError("pyshacl is required for SHACL validation. " "Install it with: pip install pyshacl")

    conforms, results_graph, results_text = shacl_validate(
        data_graph,
        shacl_graph=shapes_graph,
        inference="none",
        abort_on_first=False,
    )

    violations = []
    for result in results_graph.subjects(RDF.type, SH.ValidationResult):
        violation = {
            "focusNode": str(results_graph.value(result, SH.focusNode) or ""),
            "resultPath": str(results_graph.value(result, SH.resultPath) or ""),
            "value": str(results_graph.value(result, SH.value) or ""),
            "sourceConstraintComponent": str(results_graph.value(result, SH.sourceConstraintComponent) or ""),
            "resultMessage": str(results_graph.value(result, SH.resultMessage) or ""),
            "resultSeverity": str(results_graph.value(result, SH.resultSeverity) or ""),
            "sourceShape": str(results_graph.value(result, SH.sourceShape) or ""),
        }
        violations.append(violation)

    return violations
