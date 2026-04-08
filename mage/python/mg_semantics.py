from typing import Any, Dict, List, Optional, Union

import mgp
from mage.mg_semantics.config import GraphConfig
from mage.mg_semantics.inference import InferenceEngine
from mage.mg_semantics.mapping import MappingManager
from mage.mg_semantics.namespaces import NamespaceManager
from mage.mg_semantics.rdf_processor import RdfProcessor
from mage.mg_semantics.rdf_utils import RdfUtils
from mage.mg_semantics.shacl import ShaclValidator

# ---------------------------------------------------------------------------
# Graph Config procedures
# ---------------------------------------------------------------------------


@mgp.write_proc
def graphconfig_init(
    ctx: mgp.ProcCtx,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(key=str, value=mgp.Any):
    config = GraphConfig.init(params)
    NamespaceManager.init_common_prefixes()
    return [mgp.Record(key=k, value=v) for k, v in config.items()]


@mgp.write_proc
def graphconfig_set(
    ctx: mgp.ProcCtx,
    params: mgp.Map,
) -> mgp.Record(key=str, value=mgp.Any):
    config = GraphConfig.set(params)
    return [mgp.Record(key=k, value=v) for k, v in config.items()]


@mgp.read_proc
def graphconfig_show(
    ctx: mgp.ProcCtx,
) -> mgp.Record(key=str, value=mgp.Any):
    config = GraphConfig.show()
    return [mgp.Record(key=k, value=v) for k, v in config.items()]


@mgp.write_proc
def graphconfig_drop(ctx: mgp.ProcCtx) -> mgp.Record(status=str):
    GraphConfig.drop()
    NamespaceManager.reset()
    MappingManager.reset()
    ShaclValidator.reset()
    return mgp.Record(status="Graph config dropped")


# ---------------------------------------------------------------------------
# RDF Import procedures
# ---------------------------------------------------------------------------


@mgp.write_proc
def rdf_import_fetch(
    ctx: mgp.ProcCtx,
    url: str,
    fmt: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(triplesLoaded=int, triplesParsed=int, nodesCreated=int, relationshipsCreated=int,):
    result = RdfProcessor.import_rdf(ctx, url, fmt, is_url=True, params=params)
    return mgp.Record(**result)


@mgp.write_proc
def rdf_import_inline(
    ctx: mgp.ProcCtx,
    rdf: str,
    fmt: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(triplesLoaded=int, triplesParsed=int, nodesCreated=int, relationshipsCreated=int,):
    result = RdfProcessor.import_rdf(ctx, rdf, fmt, is_url=False, params=params)
    return mgp.Record(**result)


@mgp.write_proc
def onto_import_fetch(
    ctx: mgp.ProcCtx,
    url: str,
    fmt: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(triplesLoaded=int, triplesParsed=int, nodesCreated=int, relationshipsCreated=int,):
    result = RdfProcessor.import_ontology(ctx, url, fmt, is_url=True, params=params)
    return mgp.Record(**result)


@mgp.write_proc
def onto_import_inline(
    ctx: mgp.ProcCtx,
    rdf: str,
    fmt: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(triplesLoaded=int, triplesParsed=int, nodesCreated=int, relationshipsCreated=int,):
    result = RdfProcessor.import_ontology(ctx, rdf, fmt, is_url=False, params=params)
    return mgp.Record(**result)


# ---------------------------------------------------------------------------
# RDF Stream procedures
# ---------------------------------------------------------------------------


@mgp.read_proc
def rdf_stream_fetch(
    ctx: mgp.ProcCtx,
    url: str,
    fmt: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(
    subject=str,
    predicate=str,
    object=str,
    isLiteral=bool,
    literalType=mgp.Nullable[str],
    literalLang=mgp.Nullable[str],
):
    triples = RdfProcessor.stream_rdf(url, fmt, is_url=True, params=params)
    return [mgp.Record(**t) for t in triples]


@mgp.read_proc
def rdf_stream_inline(
    ctx: mgp.ProcCtx,
    rdf: str,
    fmt: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(
    subject=str,
    predicate=str,
    object=str,
    isLiteral=bool,
    literalType=mgp.Nullable[str],
    literalLang=mgp.Nullable[str],
):
    triples = RdfProcessor.stream_rdf(rdf, fmt, is_url=False, params=params)
    return [mgp.Record(**t) for t in triples]


# ---------------------------------------------------------------------------
# RDF Preview procedures
# ---------------------------------------------------------------------------


@mgp.read_proc
def rdf_preview_fetch(
    ctx: mgp.ProcCtx,
    url: str,
    fmt: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(nodes=mgp.List[mgp.Map], relationships=mgp.List[mgp.Map]):
    result = RdfProcessor.preview_rdf(url, fmt, is_url=True, params=params)
    return mgp.Record(nodes=result["nodes"], relationships=result["relationships"])


@mgp.read_proc
def rdf_preview_inline(
    ctx: mgp.ProcCtx,
    rdf: str,
    fmt: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(nodes=mgp.List[mgp.Map], relationships=mgp.List[mgp.Map]):
    result = RdfProcessor.preview_rdf(rdf, fmt, is_url=False, params=params)
    return mgp.Record(nodes=result["nodes"], relationships=result["relationships"])


# ---------------------------------------------------------------------------
# RDF Delete procedures
# ---------------------------------------------------------------------------


@mgp.write_proc
def rdf_delete_fetch(
    ctx: mgp.ProcCtx,
    url: str,
    fmt: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(triplesDeleted=int, nodesDeleted=int, relationshipsDeleted=int):
    result = RdfProcessor.delete_rdf(ctx, url, fmt, is_url=True, params=params)
    return mgp.Record(**result)


@mgp.write_proc
def rdf_delete_inline(
    ctx: mgp.ProcCtx,
    rdf: str,
    fmt: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(triplesDeleted=int, nodesDeleted=int, relationshipsDeleted=int):
    result = RdfProcessor.delete_rdf(ctx, rdf, fmt, is_url=False, params=params)
    return mgp.Record(**result)


# ---------------------------------------------------------------------------
# Namespace Prefix procedures
# ---------------------------------------------------------------------------


@mgp.write_proc
def nsprefixes_add(
    ctx: mgp.ProcCtx,
    prefix: str,
    namespace: str,
) -> mgp.Record(prefix=str, namespace=str):
    p, ns = NamespaceManager.add(prefix, namespace)
    return mgp.Record(prefix=p, namespace=ns)


@mgp.read_proc
def nsprefixes_list(
    ctx: mgp.ProcCtx,
) -> mgp.Record(prefix=str, namespace=str):
    items = NamespaceManager.list_all()
    return [mgp.Record(prefix=p, namespace=ns) for p, ns in items]


@mgp.write_proc
def nsprefixes_remove(
    ctx: mgp.ProcCtx,
    prefix: str,
) -> mgp.Record(status=str):
    NamespaceManager.remove(prefix)
    return mgp.Record(status=f"Prefix '{prefix}' removed")


@mgp.write_proc
def nsprefixes_remove_all(ctx: mgp.ProcCtx) -> mgp.Record(status=str):
    NamespaceManager.remove_all()
    return mgp.Record(status="All namespace prefixes removed")


@mgp.write_proc
def nsprefixes_add_from_text(
    ctx: mgp.ProcCtx,
    text: str,
) -> mgp.Record(prefix=str, namespace=str):
    added = NamespaceManager.add_from_text(text)
    return [mgp.Record(prefix=p, namespace=ns) for p, ns in added]


# ---------------------------------------------------------------------------
# Model Mapping procedures
# ---------------------------------------------------------------------------


@mgp.write_proc
def mapping_add(
    ctx: mgp.ProcCtx,
    full_uri: str,
    db_name: str,
) -> mgp.Record(dbName=str, fullUri=str):
    name, uri = MappingManager.add(full_uri, db_name)
    return mgp.Record(dbName=name, fullUri=uri)


@mgp.write_proc
def mapping_drop(
    ctx: mgp.ProcCtx,
    db_name: str,
) -> mgp.Record(status=str):
    MappingManager.drop(db_name)
    return mgp.Record(status=f"Mapping for '{db_name}' dropped")


@mgp.write_proc
def mapping_drop_all(
    ctx: mgp.ProcCtx,
    namespace: mgp.Nullable[str] = None,
) -> mgp.Record(status=str):
    MappingManager.drop_all(namespace)
    msg = "All mappings dropped"
    if namespace:
        msg = f"All mappings for namespace '{namespace}' dropped"
    return mgp.Record(status=msg)


@mgp.read_proc
def mapping_list(
    ctx: mgp.ProcCtx,
    filter_str: mgp.Nullable[str] = None,
) -> mgp.Record(dbName=str, fullUri=str):
    items = MappingManager.list_all(filter_str)
    return [mgp.Record(dbName=name, fullUri=uri) for name, uri in items]


# ---------------------------------------------------------------------------
# SHACL Validation procedures
# ---------------------------------------------------------------------------


@mgp.write_proc
def shacl_import_fetch(
    ctx: mgp.ProcCtx,
    url: str,
    fmt: str,
) -> mgp.Record(shapesLoaded=int):
    count = ShaclValidator.import_shapes_from_url(url, fmt)
    return mgp.Record(shapesLoaded=count)


@mgp.write_proc
def shacl_import_inline(
    ctx: mgp.ProcCtx,
    rdf: str,
    fmt: str,
) -> mgp.Record(shapesLoaded=int):
    count = ShaclValidator.import_shapes_from_text(rdf, fmt)
    return mgp.Record(shapesLoaded=count)


@mgp.read_proc
def shacl_list_shapes(
    ctx: mgp.ProcCtx,
) -> mgp.Record(shape=str, targetClass=mgp.Nullable[str], targetNode=mgp.Nullable[str], properties=mgp.List[mgp.Map]):
    shapes = ShaclValidator.list_shapes()
    return [
        mgp.Record(
            shape=s["shape"],
            targetClass=s.get("targetClass"),
            targetNode=s.get("targetNode"),
            properties=s.get("properties", []),
        )
        for s in shapes
    ]


@mgp.read_proc
def shacl_validate(
    ctx: mgp.ProcCtx,
) -> mgp.Record(
    focusNode=str,
    resultPath=str,
    value=str,
    sourceConstraintComponent=str,
    resultMessage=str,
    resultSeverity=str,
    sourceShape=str,
):
    violations = ShaclValidator.validate(ctx)
    if not violations:
        return []
    return [mgp.Record(**v) for v in violations]


@mgp.read_proc
def shacl_validate_set(
    ctx: mgp.ProcCtx,
    node_uris: mgp.List[str],
) -> mgp.Record(
    focusNode=str,
    resultPath=str,
    value=str,
    sourceConstraintComponent=str,
    resultMessage=str,
    resultSeverity=str,
    sourceShape=str,
):
    violations = ShaclValidator.validate_set(ctx, list(node_uris))
    if not violations:
        return []
    return [mgp.Record(**v) for v in violations]


@mgp.read_proc
def shacl_validate_transaction(
    ctx: mgp.ProcCtx,
) -> mgp.Record(
    focusNode=str,
    resultPath=str,
    value=str,
    sourceConstraintComponent=str,
    resultMessage=str,
    resultSeverity=str,
    sourceShape=str,
):
    violations = ShaclValidator.validate(ctx)
    if violations:
        raise RuntimeError(
            f"SHACL validation failed with {len(violations)} violation(s). " "Transaction should be rolled back."
        )
    return []


# ---------------------------------------------------------------------------
# Inferencing procedures
# ---------------------------------------------------------------------------


@mgp.read_proc
def inference_nodes_labelled(
    ctx: mgp.ProcCtx,
    label: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(node=mgp.Vertex):
    nodes = InferenceEngine.nodes_labelled(ctx, label, params)
    return [mgp.Record(node=n) for n in nodes]


@mgp.read_proc
def inference_nodes_in_category(
    ctx: mgp.ProcCtx,
    category_node: mgp.Vertex,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(node=mgp.Vertex):
    nodes = InferenceEngine.nodes_in_category(ctx, category_node, params)
    return [mgp.Record(node=n) for n in nodes]


@mgp.read_proc
def inference_get_rels(
    ctx: mgp.ProcCtx,
    start_node: mgp.Vertex,
    rel_type: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(rel=mgp.Edge, node=mgp.Vertex):
    results = InferenceEngine.get_rels(ctx, start_node, rel_type, params)
    return [mgp.Record(rel=r["rel"], node=r["node"]) for r in results]


# ---------------------------------------------------------------------------
# Inferencing functions
# ---------------------------------------------------------------------------


@mgp.function
def inference_has_label(
    ctx: mgp.FuncCtx,
    node: mgp.Vertex,
    label: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> bool:
    # FuncCtx doesn't have .graph, so we check labels directly on the node
    # without sublabel traversal (requires ProcCtx for full graph access)
    node_labels = {l.name for l in node.labels}
    return label in node_labels


@mgp.function
def inference_in_category(
    ctx: mgp.FuncCtx,
    node: mgp.Vertex,
    category_node: mgp.Vertex,
    params: mgp.Nullable[mgp.Map] = None,
) -> bool:
    cat_rel = (params or {}).get("catRel", "IN_CATEGORY")
    for edge in node.out_edges:
        if edge.type.name == cat_rel and edge.to_vertex.id == category_node.id:
            return True
    return False


# ---------------------------------------------------------------------------
# RDF Utility functions
# ---------------------------------------------------------------------------


@mgp.function
def rdf_get_iri_local_name(ctx: mgp.FuncCtx, uri: str) -> str:
    return RdfUtils.get_iri_local_name(uri)


@mgp.function
def rdf_get_iri_namespace(ctx: mgp.FuncCtx, uri: str) -> str:
    return RdfUtils.get_iri_namespace(uri)


@mgp.function
def rdf_get_data_type(ctx: mgp.FuncCtx, value: str) -> str:
    return RdfUtils.get_data_type(value)


@mgp.function
def rdf_get_lang_value(ctx: mgp.FuncCtx, lang: str, value: str) -> mgp.Nullable[str]:
    return RdfUtils.get_lang_value(lang, value)


@mgp.function
def rdf_get_lang_tag(ctx: mgp.FuncCtx, value: str) -> mgp.Nullable[str]:
    return RdfUtils.get_lang_tag(value)


@mgp.function
def rdf_has_lang_tag(ctx: mgp.FuncCtx, lang: str, value: str) -> bool:
    return RdfUtils.has_lang_tag(lang, value)


@mgp.function
def rdf_get_value(ctx: mgp.FuncCtx, value: str) -> str:
    return RdfUtils.get_value(value)


@mgp.function
def rdf_short_form_from_full_uri(ctx: mgp.FuncCtx, uri: str) -> str:
    return RdfUtils.short_form_from_full_uri(uri)


@mgp.function
def rdf_full_uri_from_short_form(ctx: mgp.FuncCtx, short_form: str) -> str:
    return RdfUtils.full_uri_from_short_form(short_form)
