from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

import mgp
from mage.mg_semantics.config import GraphConfig
from mage.mg_semantics.mapping import MappingManager
from mage.mg_semantics.namespaces import NamespaceManager, _get_local_name
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS, XSD

FORMAT_MAP = {
    "Turtle": "turtle",
    "N-Triples": "nt",
    "JSON-LD": "json-ld",
    "TriG": "trig",
    "RDF/XML": "xml",
    "N3": "n3",
}

ONTOLOGY_CLASSES = {RDF.type, RDFS.Class, OWL.Class}
ONTOLOGY_PROPERTIES = {
    RDFS.subClassOf,
    RDFS.subPropertyOf,
    RDFS.domain,
    RDFS.range,
    OWL.equivalentClass,
    OWL.equivalentProperty,
    OWL.inverseOf,
}
ONTOLOGY_TYPES = {
    OWL.Class,
    RDFS.Class,
    OWL.ObjectProperty,
    OWL.DatatypeProperty,
    OWL.AnnotationProperty,
    RDF.Property,
}


def _resolve_format(fmt: str) -> str:
    if fmt in FORMAT_MAP:
        return FORMAT_MAP[fmt]
    lower = fmt.lower()
    for key, val in FORMAT_MAP.items():
        if key.lower() == lower or val == lower:
            return val
    raise ValueError(f"Unsupported RDF format: {fmt}. " f"Supported: {', '.join(FORMAT_MAP.keys())}")


def _transform_uri(uri: str) -> str:
    config = GraphConfig.show()
    mode = config.get("handleVocabUris", "SHORTEN")
    if mode == "KEEP":
        return uri
    if mode == "MAP":
        mapped = MappingManager.db_name_for_uri(uri)
        if mapped:
            return mapped
        return _get_local_name(uri)
    if mode == "SHORTEN":
        return NamespaceManager.shorten_uri(uri)
    # IGNORE
    return _get_local_name(uri)


def _sanitize_label(name: str) -> str:
    sanitized = name.replace("-", "_").replace(".", "_").replace(":", "__")
    if sanitized and sanitized[0].isdigit():
        sanitized = "_" + sanitized
    return sanitized


def _literal_to_python(lit: Literal, keep_lang: bool, keep_custom_dt: bool) -> Any:
    if lit.language and keep_lang:
        return f"{lit.value}@{lit.language}"
    if lit.datatype:
        if lit.datatype == XSD.integer or lit.datatype == XSD.int:
            return int(lit.value)
        if lit.datatype == XSD.float or lit.datatype == XSD.double or lit.datatype == XSD.decimal:
            return float(lit.value)
        if lit.datatype == XSD.boolean:
            return lit.value.lower() in ("true", "1")
        if keep_custom_dt and lit.datatype not in (
            XSD.string,
            XSD.integer,
            XSD.int,
            XSD.float,
            XSD.double,
            XSD.decimal,
            XSD.boolean,
            XSD.date,
            XSD.dateTime,
        ):
            return f"{lit.value}^^{lit.datatype}"
    if lit.value is not None:
        return lit.toPython()
    return str(lit)


def _parse_rdf(source: str, fmt: str, is_url: bool = False) -> Graph:
    rdf_format = _resolve_format(fmt)
    g = Graph()
    if is_url:
        req = Request(source)
        req.add_header("Accept", "text/turtle, application/rdf+xml, application/ld+json, application/n-triples")
        req.add_header("User-Agent", "mg_semantics/1.0")
        with urlopen(req) as response:
            data = response.read().decode("utf-8")
        g.parse(data=data, format=rdf_format)
    else:
        g.parse(data=source, format=rdf_format)
    return g


def _find_vertex_by_uri(ctx: mgp.ProcCtx, uri: str):
    for vertex in ctx.graph.vertices:
        if vertex.properties.get("uri") == uri:
            return vertex
    return None


class RdfProcessor:
    @staticmethod
    def import_rdf(
        ctx: mgp.ProcCtx,
        source: str,
        fmt: str,
        is_url: bool = False,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        config = GraphConfig.show()
        keep_lang = config.get("keepLangTag", False)
        keep_custom_dt = config.get("keepCustomDataTypes", False)
        handle_multival = config.get("handleMultival", "OVERWRITE")
        handle_rdf_types = config.get("handleRDFTypes", "LABELS")

        g = _parse_rdf(source, fmt, is_url)

        uri_to_vertex = {}
        triples_loaded = 0
        nodes_created = 0
        rels_created = 0

        # Build index of existing vertices with uri property
        for vertex in ctx.graph.vertices:
            uri_val = vertex.properties.get("uri")
            if uri_val:
                uri_to_vertex[uri_val] = vertex

        subjects = set(g.subjects())
        for subj in subjects:
            if isinstance(subj, BNode):
                subj_uri = f"_:{subj}"
            else:
                subj_uri = str(subj)

            if subj_uri not in uri_to_vertex:
                vertex = ctx.graph.create_vertex()
                vertex.properties.set("uri", subj_uri)
                uri_to_vertex[subj_uri] = vertex
                nodes_created += 1

        for subj, pred, obj in g:
            triples_loaded += 1
            if isinstance(subj, BNode):
                subj_uri = f"_:{subj}"
            else:
                subj_uri = str(subj)

            vertex = uri_to_vertex[subj_uri]
            pred_uri = str(pred)

            # Handle rdf:type
            if pred == RDF.type and not isinstance(obj, Literal):
                type_uri = str(obj)
                type_name = _transform_uri(type_uri)
                label = _sanitize_label(type_name)

                if handle_rdf_types in ("LABELS", "LABELS_AND_NODES"):
                    if label:
                        vertex.add_label(label)

                if handle_rdf_types in ("NODES", "LABELS_AND_NODES"):
                    if type_uri not in uri_to_vertex:
                        type_vertex = ctx.graph.create_vertex()
                        type_vertex.properties.set("uri", type_uri)
                        uri_to_vertex[type_uri] = type_vertex
                        nodes_created += 1
                    type_vertex = uri_to_vertex[type_uri]
                    ctx.graph.create_edge(vertex, type_vertex, mgp.EdgeType("HAS_TYPE"))
                    rels_created += 1
                continue

            if isinstance(obj, Literal):
                prop_name = _transform_uri(pred_uri)
                value = _literal_to_python(obj, keep_lang, keep_custom_dt)

                if handle_multival == "ARRAY":
                    existing = vertex.properties.get(prop_name)
                    if existing is not None:
                        if isinstance(existing, list):
                            existing.append(value)
                            vertex.properties.set(prop_name, existing)
                        else:
                            vertex.properties.set(prop_name, [existing, value])
                    else:
                        vertex.properties.set(prop_name, value)
                else:
                    vertex.properties.set(prop_name, value)
            else:
                # Object is a resource - create relationship
                if isinstance(obj, BNode):
                    obj_uri = f"_:{obj}"
                else:
                    obj_uri = str(obj)

                if obj_uri not in uri_to_vertex:
                    obj_vertex = ctx.graph.create_vertex()
                    obj_vertex.properties.set("uri", obj_uri)
                    uri_to_vertex[obj_uri] = obj_vertex
                    nodes_created += 1

                rel_name = _transform_uri(pred_uri)
                rel_type = _sanitize_label(rel_name)
                ctx.graph.create_edge(vertex, uri_to_vertex[obj_uri], mgp.EdgeType(rel_type))
                rels_created += 1

        # Add Resource label to all created vertices
        for vertex in uri_to_vertex.values():
            vertex.add_label("Resource")

        return {
            "triplesLoaded": triples_loaded,
            "triplesParsed": triples_loaded,
            "nodesCreated": nodes_created,
            "relationshipsCreated": rels_created,
        }

    @staticmethod
    def import_ontology(
        ctx: mgp.ProcCtx,
        source: str,
        fmt: str,
        is_url: bool = False,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        config = GraphConfig.show()
        g = _parse_rdf(source, fmt, is_url)

        uri_to_vertex = {}
        nodes_created = 0
        rels_created = 0
        triples_loaded = 0

        for vertex in ctx.graph.vertices:
            uri_val = vertex.properties.get("uri")
            if uri_val:
                uri_to_vertex[uri_val] = vertex

        def _get_or_create(uri_str: str, label: str) -> mgp.Vertex:
            nonlocal nodes_created
            if uri_str not in uri_to_vertex:
                v = ctx.graph.create_vertex()
                v.properties.set("uri", uri_str)
                v.add_label("Resource")
                uri_to_vertex[uri_str] = v
                nodes_created += 1
            vertex = uri_to_vertex[uri_str]
            vertex.add_label(_sanitize_label(label))
            return vertex

        # Import classes
        for subj in g.subjects(RDF.type, OWL.Class):
            _get_or_create(str(subj), "Class")
            triples_loaded += 1
        for subj in g.subjects(RDF.type, RDFS.Class):
            _get_or_create(str(subj), "Class")
            triples_loaded += 1

        # Import properties
        for subj in g.subjects(RDF.type, OWL.ObjectProperty):
            v = _get_or_create(str(subj), "ObjectProperty")
            v.add_label("Property")
            triples_loaded += 1
        for subj in g.subjects(RDF.type, OWL.DatatypeProperty):
            v = _get_or_create(str(subj), "DatatypeProperty")
            v.add_label("Property")
            triples_loaded += 1
        for subj in g.subjects(RDF.type, RDF.Property):
            _get_or_create(str(subj), "Property")
            triples_loaded += 1

        # Import rdfs:subClassOf
        for subj, _, obj in g.triples((None, RDFS.subClassOf, None)):
            if isinstance(obj, URIRef):
                sub_v = _get_or_create(str(subj), "Class")
                sup_v = _get_or_create(str(obj), "Class")
                ctx.graph.create_edge(sub_v, sup_v, mgp.EdgeType("SCO"))
                rels_created += 1
                triples_loaded += 1

        # Import rdfs:subPropertyOf
        for subj, _, obj in g.triples((None, RDFS.subPropertyOf, None)):
            if isinstance(obj, URIRef):
                sub_v = _get_or_create(str(subj), "Property")
                sup_v = _get_or_create(str(obj), "Property")
                ctx.graph.create_edge(sub_v, sup_v, mgp.EdgeType("SPO"))
                rels_created += 1
                triples_loaded += 1

        # Import rdfs:domain
        for subj, _, obj in g.triples((None, RDFS.domain, None)):
            if isinstance(obj, URIRef):
                prop_v = _get_or_create(str(subj), "Property")
                class_v = _get_or_create(str(obj), "Class")
                ctx.graph.create_edge(prop_v, class_v, mgp.EdgeType("DOMAIN"))
                rels_created += 1
                triples_loaded += 1

        # Import rdfs:range
        for subj, _, obj in g.triples((None, RDFS.range, None)):
            if isinstance(obj, URIRef):
                prop_v = _get_or_create(str(subj), "Property")
                class_v = _get_or_create(str(obj), "Class")
                ctx.graph.create_edge(prop_v, class_v, mgp.EdgeType("RANGE"))
                rels_created += 1
                triples_loaded += 1

        # Import rdfs:label as name property
        for subj, _, obj in g.triples((None, RDFS.label, None)):
            subj_uri = str(subj)
            if subj_uri in uri_to_vertex:
                uri_to_vertex[subj_uri].properties.set("name", str(obj))
                triples_loaded += 1

        # Import rdfs:comment as comment property
        for subj, _, obj in g.triples((None, RDFS.comment, None)):
            subj_uri = str(subj)
            if subj_uri in uri_to_vertex:
                uri_to_vertex[subj_uri].properties.set("comment", str(obj))
                triples_loaded += 1

        return {
            "triplesLoaded": triples_loaded,
            "triplesParsed": triples_loaded,
            "nodesCreated": nodes_created,
            "relationshipsCreated": rels_created,
        }

    @staticmethod
    def stream_rdf(
        source: str,
        fmt: str,
        is_url: bool = False,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, str]]:
        g = _parse_rdf(source, fmt, is_url)
        results = []
        for subj, pred, obj in g:
            results.append(
                {
                    "subject": str(subj),
                    "predicate": str(pred),
                    "object": str(obj),
                    "isLiteral": isinstance(obj, Literal),
                    "literalType": str(obj.datatype) if isinstance(obj, Literal) and obj.datatype else None,
                    "literalLang": obj.language if isinstance(obj, Literal) else None,
                }
            )
        return results

    @staticmethod
    def preview_rdf(
        source: str,
        fmt: str,
        is_url: bool = False,
        params: Optional[Dict[str, Any]] = None,
        limit: int = 1000,
    ) -> Dict[str, List]:
        g = _parse_rdf(source, fmt, is_url)

        nodes = {}
        relationships = []
        triple_count = 0

        for subj, pred, obj in g:
            if triple_count >= limit:
                break
            triple_count += 1

            subj_uri = str(subj)
            if subj_uri not in nodes:
                nodes[subj_uri] = {
                    "uri": subj_uri,
                    "labels": ["Resource"],
                    "properties": {"uri": subj_uri},
                }

            if pred == RDF.type and not isinstance(obj, Literal):
                type_name = _transform_uri(str(obj))
                label = _sanitize_label(type_name)
                if label and label not in nodes[subj_uri]["labels"]:
                    nodes[subj_uri]["labels"].append(label)
            elif isinstance(obj, Literal):
                prop_name = _transform_uri(str(pred))
                nodes[subj_uri]["properties"][prop_name] = str(obj)
            else:
                obj_uri = str(obj)
                if obj_uri not in nodes:
                    nodes[obj_uri] = {
                        "uri": obj_uri,
                        "labels": ["Resource"],
                        "properties": {"uri": obj_uri},
                    }
                rel_name = _transform_uri(str(pred))
                relationships.append(
                    {
                        "start": subj_uri,
                        "end": obj_uri,
                        "type": _sanitize_label(rel_name),
                        "properties": {},
                    }
                )

        return {
            "nodes": list(nodes.values()),
            "relationships": relationships,
        }

    @staticmethod
    def delete_rdf(
        ctx: mgp.ProcCtx,
        source: str,
        fmt: str,
        is_url: bool = False,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        g = _parse_rdf(source, fmt, is_url)

        triples_deleted = 0
        nodes_deleted = 0
        rels_deleted = 0

        uri_to_vertex = {}
        for vertex in ctx.graph.vertices:
            uri_val = vertex.properties.get("uri")
            if uri_val:
                uri_to_vertex[uri_val] = vertex

        for subj, pred, obj in g:
            subj_uri = str(subj)
            if subj_uri not in uri_to_vertex:
                continue

            vertex = uri_to_vertex[subj_uri]

            if pred == RDF.type and not isinstance(obj, Literal):
                type_name = _transform_uri(str(obj))
                label = _sanitize_label(type_name)
                try:
                    vertex.remove_label(label)
                    triples_deleted += 1
                except Exception:
                    pass
            elif isinstance(obj, Literal):
                prop_name = _transform_uri(str(pred))
                try:
                    vertex.properties.set(prop_name, None)
                    triples_deleted += 1
                except Exception:
                    pass
            else:
                obj_uri = str(obj)
                if obj_uri in uri_to_vertex:
                    rel_name = _transform_uri(str(pred))
                    rel_type = _sanitize_label(rel_name)
                    for edge in vertex.out_edges:
                        if edge.type.name == rel_type and edge.to_vertex.properties.get("uri") == obj_uri:
                            ctx.graph.delete_edge(edge)
                            rels_deleted += 1
                            triples_deleted += 1
                            break

        # Remove vertices that have no properties (except uri) and no edges
        vertices_to_check = list(uri_to_vertex.values())
        for vertex in vertices_to_check:
            has_edges = False
            try:
                for _ in vertex.in_edges:
                    has_edges = True
                    break
                if not has_edges:
                    for _ in vertex.out_edges:
                        has_edges = True
                        break
            except Exception:
                continue

            if not has_edges:
                prop_keys = list(vertex.properties.keys())
                if prop_keys == ["uri"] or not prop_keys:
                    try:
                        ctx.graph.detach_delete_vertex(vertex)
                        nodes_deleted += 1
                    except Exception:
                        pass

        return {
            "triplesDeleted": triples_deleted,
            "nodesDeleted": nodes_deleted,
            "relationshipsDeleted": rels_deleted,
        }
