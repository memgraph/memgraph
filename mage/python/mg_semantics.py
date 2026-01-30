"""
Module: mg_semantics
Description: Memgraph Semantics - RDF data import and manipulation for Memgraph.

Translated from Neo4j Neosemantics (n10s): https://neo4j.com/labs/neosemantics/
Documentation: https://neo4j.com/labs/neosemantics/5.14/

This module provides functionality to:
- Configure graph settings for RDF data handling
- Import RDF data from URLs or inline snippets
- Manipulate URIs, language tags, and datatypes
- Manage namespace prefixes
"""
import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import mgp

import requests

# Try to import rdflib for RDF parsing
try:
    import rdflib
    from rdflib import BNode, Graph, Literal, Namespace, URIRef
    from rdflib.namespace import DC, DCTERMS, OWL, RDF, RDFS, SKOS, XSD

    HAS_RDFLIB = True
except ImportError:
    HAS_RDFLIB = False


# =============================================================================
# GRAPH CONFIGURATION
# =============================================================================

# Global configuration storage (simulating graph-level config)
# In production, this should be stored as graph properties or a special node
_GRAPH_CONFIG: Dict[str, Any] = {}
_NAMESPACE_PREFIXES: Dict[str, str] = {}

# Default configuration values
DEFAULT_CONFIG = {
    "handleVocabUris": "SHORTEN",  # SHORTEN | IGNORE | KEEP
    "handleMultival": "OVERWRITE",  # OVERWRITE | ARRAY
    "handleRDFTypes": "LABELS",  # LABELS | LABELS_AND_NODES | NODES
    "keepLangTag": False,
    "keepCustomDataTypes": False,
    "multivalPropList": [],
    "customDataTypePropList": [],
    "predicateExclusionList": [],
    "typesToLabels": True,
    "applyNeo4jNaming": False,
}

# Standard namespace prefixes
STANDARD_PREFIXES = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dct": "http://purl.org/dc/terms/",
    "sch": "http://schema.org/",
    "sh": "http://www.w3.org/ns/shacl#",
}

# RDF format mapping
FORMAT_MAP = {
    "Turtle": "turtle",
    "turtle": "turtle",
    "ttl": "turtle",
    "N-Triples": "nt",
    "N-TRIPLES": "nt",
    "nt": "nt",
    "ntriples": "nt",
    "JSON-LD": "json-ld",
    "json-ld": "json-ld",
    "jsonld": "json-ld",
    "RDF/XML": "xml",
    "rdf/xml": "xml",
    "xml": "xml",
    "rdfxml": "xml",
    "TriG": "trig",
    "trig": "trig",
    "N-Quads": "nquads",
    "nquads": "nquads",
    "nq": "nquads",
}


@dataclass
class GraphConfig:
    """Configuration for RDF graph handling."""

    handleVocabUris: str = "SHORTEN"
    handleMultival: str = "OVERWRITE"
    handleRDFTypes: str = "LABELS"
    keepLangTag: bool = False
    keepCustomDataTypes: bool = False
    multivalPropList: List[str] = field(default_factory=list)
    customDataTypePropList: List[str] = field(default_factory=list)
    predicateExclusionList: List[str] = field(default_factory=list)
    typesToLabels: bool = True
    applyNeo4jNaming: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "handleVocabUris": self.handleVocabUris,
            "handleMultival": self.handleMultival,
            "handleRDFTypes": self.handleRDFTypes,
            "keepLangTag": self.keepLangTag,
            "keepCustomDataTypes": self.keepCustomDataTypes,
            "multivalPropList": self.multivalPropList,
            "customDataTypePropList": self.customDataTypePropList,
            "predicateExclusionList": self.predicateExclusionList,
            "typesToLabels": self.typesToLabels,
            "applyNeo4jNaming": self.applyNeo4jNaming,
        }


def _get_config() -> GraphConfig:
    """Get current graph configuration."""
    global _GRAPH_CONFIG
    if not _GRAPH_CONFIG:
        return GraphConfig()
    return GraphConfig(**_GRAPH_CONFIG)


def _set_config(config: Dict[str, Any]) -> None:
    """Set graph configuration."""
    global _GRAPH_CONFIG
    _GRAPH_CONFIG.update(config)


def _init_prefixes() -> None:
    """Initialize standard namespace prefixes."""
    global _NAMESPACE_PREFIXES
    _NAMESPACE_PREFIXES = STANDARD_PREFIXES.copy()


# =============================================================================
# GRAPHCONFIG PROCEDURES
# =============================================================================


@mgp.read_proc
def graphconfig_init(
    ctx: mgp.ProcCtx,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(param=str, value=mgp.Any):
    """
    Initialize the graph configuration for RDF data handling.

    This procedure sets up the configuration parameters that control how RDF
    data is imported and stored in the graph. All settings are global and
    remain valid for the lifetime of the graph.

    Parameters
    ----------
    params : Map, optional
        Configuration parameters to override defaults:
        - handleVocabUris: "SHORTEN" | "IGNORE" | "KEEP" (default: "SHORTEN")
        - handleMultival: "OVERWRITE" | "ARRAY" (default: "OVERWRITE")
        - handleRDFTypes: "LABELS" | "LABELS_AND_NODES" | "NODES" (default: "LABELS")
        - keepLangTag: bool (default: false)
        - keepCustomDataTypes: bool (default: false)
        - multivalPropList: List[str] (default: [])
        - customDataTypePropList: List[str] (default: [])
        - predicateExclusionList: List[str] (default: [])

    Returns
    -------
    mgp.Record
        param: Configuration parameter name
        value: Configuration parameter value

    Example
    -------
    CALL mg_semantics.graphconfig_init();
    CALL mg_semantics.graphconfig_init({handleVocabUris: "IGNORE"});
    """
    global _GRAPH_CONFIG, _NAMESPACE_PREFIXES

    # Reset configuration to defaults
    _GRAPH_CONFIG = DEFAULT_CONFIG.copy()

    # Initialize namespace prefixes
    _init_prefixes()

    # Apply custom parameters
    if params:
        for key, value in params.items():
            if key in _GRAPH_CONFIG:
                _GRAPH_CONFIG[key] = value

    # Return all configuration parameters
    return [mgp.Record(param=k, value=v) for k, v in _GRAPH_CONFIG.items()]


@mgp.read_proc
def graphconfig_set(
    ctx: mgp.ProcCtx,
    params: mgp.Map,
) -> mgp.Record(param=str, value=mgp.Any):
    """
    Update specific graph configuration parameters.

    Use this procedure to modify individual configuration items after
    the initial configuration has been created.

    Parameters
    ----------
    params : Map
        Configuration parameters to update.

    Returns
    -------
    mgp.Record
        param: Updated parameter name
        value: Updated parameter value

    Example
    -------
    CALL mg_semantics.graphconfig_set({keepLangTag: true, handleRDFTypes: "LABELS_AND_NODES"});
    """
    global _GRAPH_CONFIG

    if not _GRAPH_CONFIG:
        raise ValueError("Graph configuration not initialized. Call graphconfig_init() first.")

    updated = []
    for key, value in params.items():
        if key in DEFAULT_CONFIG:
            _GRAPH_CONFIG[key] = value
            updated.append(mgp.Record(param=key, value=value))
        else:
            raise ValueError(f"Unknown configuration parameter: {key}")

    return updated


@mgp.read_proc
def graphconfig_show(
    ctx: mgp.ProcCtx,
) -> mgp.Record(param=str, value=mgp.Any):
    """
    Show the current graph configuration.

    Returns
    -------
    mgp.Record
        param: Configuration parameter name
        value: Configuration parameter value

    Example
    -------
    CALL mg_semantics.graphconfig_show();
    """
    global _GRAPH_CONFIG

    if not _GRAPH_CONFIG:
        return [mgp.Record(param="status", value="Not configured")]

    return [mgp.Record(param=k, value=v) for k, v in _GRAPH_CONFIG.items()]


@mgp.read_proc
def graphconfig_drop(
    ctx: mgp.ProcCtx,
) -> mgp.Record(status=str):
    """
    Remove the graph configuration.

    This clears all configuration settings, requiring a new call to
    graphconfig_init() before importing RDF data.

    Returns
    -------
    mgp.Record
        status: "OK" if configuration was dropped

    Example
    -------
    CALL mg_semantics.graphconfig_drop();
    """
    global _GRAPH_CONFIG, _NAMESPACE_PREFIXES

    _GRAPH_CONFIG = {}
    _NAMESPACE_PREFIXES = {}

    return mgp.Record(status="OK")


# =============================================================================
# NAMESPACE PREFIX PROCEDURES
# =============================================================================


@mgp.read_proc
def nsprefixes_list(
    ctx: mgp.ProcCtx,
) -> mgp.Record(prefix=str, namespace=str):
    """
    List all registered namespace prefixes.

    Returns
    -------
    mgp.Record
        prefix: The short prefix (e.g., "rdf")
        namespace: The full namespace URI (e.g., "http://www.w3.org/1999/02/22-rdf-syntax-ns#")

    Example
    -------
    CALL mg_semantics.nsprefixes_list();
    """
    global _NAMESPACE_PREFIXES

    if not _NAMESPACE_PREFIXES:
        _init_prefixes()

    return [mgp.Record(prefix=p, namespace=ns) for p, ns in _NAMESPACE_PREFIXES.items()]


@mgp.read_proc
def nsprefixes_add(
    ctx: mgp.ProcCtx,
    prefix: str,
    namespace: str,
) -> mgp.Record(prefix=str, namespace=str):
    """
    Add a namespace prefix mapping.

    Parameters
    ----------
    prefix : str
        The short prefix to use (e.g., "neo")
    namespace : str
        The full namespace URI (e.g., "http://neo4j.org/vocab/sw#")

    Returns
    -------
    mgp.Record
        prefix: The added prefix
        namespace: The namespace URI

    Example
    -------
    CALL mg_semantics.nsprefixes_add("neo", "http://neo4j.org/vocab/sw#");
    """
    global _NAMESPACE_PREFIXES

    if not _NAMESPACE_PREFIXES:
        _init_prefixes()

    _NAMESPACE_PREFIXES[prefix] = namespace

    return mgp.Record(prefix=prefix, namespace=namespace)


@mgp.read_proc
def nsprefixes_add_from_text(
    ctx: mgp.ProcCtx,
    text: str,
) -> mgp.Record(prefix=str, namespace=str):
    """
    Extract and add namespace prefixes from RDF/XML or Turtle header text.

    Parameters
    ----------
    text : str
        Text containing namespace declarations (RDF/XML, Turtle, or SPARQL format)

    Returns
    -------
    mgp.Record
        prefix: Extracted prefix
        namespace: Extracted namespace URI

    Example
    -------
    WITH '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
             xmlns:foaf="http://xmlns.com/foaf/0.1/">' as txt
    CALL mg_semantics.nsprefixes_add_from_text(txt) YIELD prefix, namespace
    RETURN prefix, namespace;
    """
    global _NAMESPACE_PREFIXES

    if not _NAMESPACE_PREFIXES:
        _init_prefixes()

    results = []

    # Pattern for XML namespace declarations: xmlns:prefix="namespace"
    xml_pattern = r'xmlns:(\w+)\s*=\s*["\']([^"\']+)["\']'
    for match in re.finditer(xml_pattern, text):
        prefix, namespace = match.groups()
        _NAMESPACE_PREFIXES[prefix] = namespace
        results.append(mgp.Record(prefix=prefix, namespace=namespace))

    # Pattern for Turtle/SPARQL prefix declarations: @prefix prefix: <namespace> .
    # or PREFIX prefix: <namespace>
    turtle_pattern = r"(?:@prefix|PREFIX)\s+(\w*):\s*<([^>]+)>"
    for match in re.finditer(turtle_pattern, text, re.IGNORECASE):
        prefix, namespace = match.groups()
        if prefix:  # Skip base URI (empty prefix)
            _NAMESPACE_PREFIXES[prefix] = namespace
            results.append(mgp.Record(prefix=prefix, namespace=namespace))

    return results


# =============================================================================
# RDF IMPORT PROCEDURES
# =============================================================================


def _get_namespace_and_local(uri: str) -> Tuple[str, str]:
    """Split a URI into namespace and local name."""
    # Try common delimiters
    for delimiter in ["#", "/", ":"]:
        if delimiter in uri:
            idx = uri.rfind(delimiter)
            return uri[: idx + 1], uri[idx + 1 :]
    return "", uri


def _shorten_uri(uri: str) -> str:
    """Shorten a URI using registered namespace prefixes."""
    global _NAMESPACE_PREFIXES

    if not _NAMESPACE_PREFIXES:
        _init_prefixes()

    namespace, local_name = _get_namespace_and_local(uri)

    # Check if namespace is registered
    for prefix, ns in _NAMESPACE_PREFIXES.items():
        if namespace == ns:
            return f"{prefix}__{local_name}"

    # Auto-generate prefix for unknown namespace
    prefix_num = len([p for p in _NAMESPACE_PREFIXES if p.startswith("ns")])
    new_prefix = f"ns{prefix_num}"
    _NAMESPACE_PREFIXES[new_prefix] = namespace

    return f"{new_prefix}__{local_name}"


def _process_uri(uri: str, handle_vocab: str) -> str:
    """Process URI according to handleVocabUris setting."""
    if handle_vocab == "IGNORE":
        _, local_name = _get_namespace_and_local(uri)
        return local_name
    elif handle_vocab == "SHORTEN":
        return _shorten_uri(uri)
    else:  # KEEP
        return uri


def _find_or_create_resource(ctx: mgp.ProcCtx, uri: str, resources: Dict[str, int]) -> mgp.Vertex:
    """Find existing resource by URI or create new one."""
    if uri in resources:
        return ctx.graph.get_vertex_by_id(resources[uri])

    # Create new vertex
    vertex = ctx.graph.create_vertex()
    vertex.add_label("Resource")
    vertex.properties["uri"] = uri
    resources[uri] = vertex.id
    return vertex


def _process_literal_value(value: Any, datatype: Optional[str], lang: Optional[str], config: GraphConfig) -> Any:
    """Process a literal value according to configuration."""
    result = str(value)

    # Handle language tag
    if lang and config.keepLangTag:
        result = f"{result}@{lang}"

    # Handle custom datatype
    if datatype and config.keepCustomDataTypes:
        # Check if this property should keep its datatype
        dt_short = _shorten_uri(str(datatype))
        result = f"{result}^^{dt_short}"

    return result


def _import_triple(
    ctx: mgp.ProcCtx,
    subject: Any,
    predicate: Any,
    obj: Any,
    config: GraphConfig,
    resources: Dict[str, int],
    handle_vocab: str,
) -> bool:
    """Import a single RDF triple into the graph."""
    subject_uri = str(subject)
    predicate_uri = str(predicate)

    # Check predicate exclusion
    if predicate_uri in config.predicateExclusionList:
        return False

    # Find or create subject vertex
    subject_vertex = _find_or_create_resource(ctx, subject_uri, resources)

    # Process predicate
    predicate_name = _process_uri(predicate_uri, handle_vocab)

    # Handle rdf:type specially
    rdf_type_uri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    if predicate_uri == rdf_type_uri:
        type_uri = str(obj)
        label_name = _process_uri(type_uri, handle_vocab)

        if config.handleRDFTypes in ("LABELS", "LABELS_AND_NODES"):
            try:
                subject_vertex.add_label(label_name)
            except Exception:
                pass  # Label might already exist

        if config.handleRDFTypes in ("NODES", "LABELS_AND_NODES"):
            # Create relationship to type node
            type_vertex = _find_or_create_resource(ctx, type_uri, resources)
            rel_type = _process_uri(rdf_type_uri, handle_vocab)
            ctx.graph.create_edge(subject_vertex, type_vertex, mgp.EdgeType(rel_type))

        return True

    # Check if object is a literal or resource
    if HAS_RDFLIB and isinstance(obj, Literal):
        # Literal value - store as property
        datatype = obj.datatype if hasattr(obj, "datatype") else None
        lang = obj.language if hasattr(obj, "language") else None

        value = _process_literal_value(obj.value, datatype, lang, config)

        # Handle multivalued properties
        if config.handleMultival == "ARRAY":
            if not config.multivalPropList or predicate_uri in config.multivalPropList:
                existing = subject_vertex.properties.get(predicate_name)
                if existing is not None:
                    if isinstance(existing, list):
                        subject_vertex.properties[predicate_name] = existing + [value]
                    else:
                        subject_vertex.properties[predicate_name] = [existing, value]
                else:
                    subject_vertex.properties[predicate_name] = [value]
            else:
                subject_vertex.properties[predicate_name] = value
        else:
            subject_vertex.properties[predicate_name] = value
    else:
        # Object property - create relationship
        object_uri = str(obj)
        object_vertex = _find_or_create_resource(ctx, object_uri, resources)
        ctx.graph.create_edge(subject_vertex, object_vertex, mgp.EdgeType(predicate_name))

    return True


def _parse_rdf_content(content: str, rdf_format: str) -> List[Tuple[Any, Any, Any]]:
    """Parse RDF content and return list of triples."""
    if not HAS_RDFLIB:
        raise ImportError("rdflib is required for RDF parsing. Install it with: pip install rdflib")

    # Map format to rdflib format
    rdflib_format = FORMAT_MAP.get(rdf_format)
    if not rdflib_format:
        raise ValueError(f"Unsupported RDF format: {rdf_format}. " f"Supported formats: {list(FORMAT_MAP.keys())}")

    g = Graph()
    g.parse(data=content, format=rdflib_format)

    return list(g)


@mgp.write_proc
def rdf_import_fetch(
    ctx: mgp.ProcCtx,
    url: str,
    rdf_format: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(
    terminationStatus=str,
    triplesLoaded=int,
    triplesParsed=int,
    namespaces=mgp.Map,
    extraInfo=str,
    callParams=mgp.Map,
):
    """
    Import RDF data from a URL and store it in Memgraph as a property graph.

    This procedure fetches RDF data from the specified URL, parses it according
    to the given format, and imports it into the graph database.

    Parameters
    ----------
    url : str
        URL to fetch RDF data from. Can be:
        - http:// or https:// URL to remote file
        - file:// URL to local file
    rdf_format : str
        RDF serialization format. Valid formats:
        - Turtle, N-Triples, JSON-LD, RDF/XML, TriG, N-Quads
    params : Map, optional
        Additional parameters:
        - headerParams: Map of HTTP headers (for authenticated endpoints)
        - payload: POST body (for SPARQL endpoints)
        - languageFilter: Filter literals by language tag

    Returns
    -------
    mgp.Record
        terminationStatus: "OK" on success, "KO" on failure
        triplesLoaded: Number of triples successfully imported
        triplesParsed: Number of triples parsed from source
        namespaces: Map of namespace prefixes used
        extraInfo: Additional information or error message
        callParams: Echo of call parameters

    Example
    -------
    CALL mg_semantics.graphconfig_init();
    CALL mg_semantics.rdf_import_fetch(
        "https://example.com/data.ttl",
        "Turtle"
    );

    CALL mg_semantics.rdf_import_fetch(
        "https://example.com/data.ttl",
        "Turtle",
        {headerParams: {Accept: "text/turtle"}}
    );
    """
    global _GRAPH_CONFIG, _NAMESPACE_PREFIXES

    if not _GRAPH_CONFIG:
        raise ValueError("Graph configuration not initialized. Call graphconfig_init() first.")

    config = _get_config()
    call_params = {"url": url, "format": rdf_format}
    if params:
        call_params.update(dict(params))

    triples_parsed = 0
    triples_loaded = 0
    resources: Dict[str, int] = {}

    try:
        # Fetch RDF content
        if url.startswith("file://"):
            file_path = url[7:]
            # Handle Windows paths
            if file_path.startswith("/") and len(file_path) > 2 and file_path[2] == ":":
                file_path = file_path[1:]
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
        else:
            headers = {}
            if params:
                headers = dict(params.get("headerParams", {}))

            payload = params.get("payload") if params else None

            if payload:
                response = requests.post(url, headers=headers, data=payload, timeout=60)
            else:
                response = requests.get(url, headers=headers, timeout=60)

            response.raise_for_status()
            content = response.text

        # Parse RDF
        triples = _parse_rdf_content(content, rdf_format)
        triples_parsed = len(triples)

        # Language filter
        lang_filter = params.get("languageFilter") if params else None

        # Import triples
        handle_vocab = config.handleVocabUris
        for subject, predicate, obj in triples:
            # Apply language filter for literals
            if lang_filter and HAS_RDFLIB and isinstance(obj, Literal):
                obj_lang = obj.language if hasattr(obj, "language") else None
                if obj_lang and obj_lang != lang_filter:
                    continue

            if _import_triple(ctx, subject, predicate, obj, config, resources, handle_vocab):
                triples_loaded += 1

        return mgp.Record(
            terminationStatus="OK",
            triplesLoaded=triples_loaded,
            triplesParsed=triples_parsed,
            namespaces=dict(_NAMESPACE_PREFIXES),
            extraInfo="",
            callParams=call_params,
        )

    except Exception as e:
        return mgp.Record(
            terminationStatus="KO",
            triplesLoaded=triples_loaded,
            triplesParsed=triples_parsed,
            namespaces=dict(_NAMESPACE_PREFIXES) if _NAMESPACE_PREFIXES else {},
            extraInfo=str(e),
            callParams=call_params,
        )


@mgp.write_proc
def rdf_import_inline(
    ctx: mgp.ProcCtx,
    rdf: str,
    rdf_format: str,
    params: mgp.Nullable[mgp.Map] = None,
) -> mgp.Record(
    terminationStatus=str,
    triplesLoaded=int,
    triplesParsed=int,
    namespaces=mgp.Map,
    extraInfo=str,
    callParams=mgp.Map,
):
    """
    Import RDF data from an inline string and store it in Memgraph.

    This procedure parses RDF data passed directly as a string parameter
    and imports it into the graph database.

    Parameters
    ----------
    rdf : str
        RDF content as a string
    rdf_format : str
        RDF serialization format (Turtle, N-Triples, JSON-LD, RDF/XML, TriG, N-Quads)
    params : Map, optional
        Additional parameters:
        - languageFilter: Filter literals by language tag

    Returns
    -------
    mgp.Record
        terminationStatus: "OK" on success, "KO" on failure
        triplesLoaded: Number of triples successfully imported
        triplesParsed: Number of triples parsed from source
        namespaces: Map of namespace prefixes used
        extraInfo: Additional information or error message
        callParams: Echo of call parameters

    Example
    -------
    WITH '
    <neo4j://individual/JB> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://neo4j.org/voc#Person> .
    <neo4j://individual/JB> <http://neo4j.org/voc#name> "J. Barrasa" .
    ' as payload
    CALL mg_semantics.rdf_import_inline(payload, "N-Triples")
    YIELD terminationStatus, triplesLoaded
    RETURN terminationStatus, triplesLoaded;
    """
    global _GRAPH_CONFIG, _NAMESPACE_PREFIXES

    if not _GRAPH_CONFIG:
        raise ValueError("Graph configuration not initialized. Call graphconfig_init() first.")

    config = _get_config()
    call_params = {"format": rdf_format}
    if params:
        call_params.update(dict(params))

    triples_parsed = 0
    triples_loaded = 0
    resources: Dict[str, int] = {}

    try:
        # Parse RDF
        triples = _parse_rdf_content(rdf, rdf_format)
        triples_parsed = len(triples)

        # Language filter
        lang_filter = params.get("languageFilter") if params else None

        # Import triples
        handle_vocab = config.handleVocabUris
        for subject, predicate, obj in triples:
            # Apply language filter for literals
            if lang_filter and HAS_RDFLIB and isinstance(obj, Literal):
                obj_lang = obj.language if hasattr(obj, "language") else None
                if obj_lang and obj_lang != lang_filter:
                    continue

            if _import_triple(ctx, subject, predicate, obj, config, resources, handle_vocab):
                triples_loaded += 1

        return mgp.Record(
            terminationStatus="OK",
            triplesLoaded=triples_loaded,
            triplesParsed=triples_parsed,
            namespaces=dict(_NAMESPACE_PREFIXES),
            extraInfo="",
            callParams=call_params,
        )

    except Exception as e:
        return mgp.Record(
            terminationStatus="KO",
            triplesLoaded=triples_loaded,
            triplesParsed=triples_parsed,
            namespaces=dict(_NAMESPACE_PREFIXES) if _NAMESPACE_PREFIXES else {},
            extraInfo=str(e),
            callParams=call_params,
        )


# =============================================================================
# RDF UTILITY FUNCTIONS
# =============================================================================


@mgp.function
def rdf_full_uri_from_short_form(
    ctx: mgp.FuncCtx,
    short: str,
) -> mgp.Nullable[str]:
    """
    Expand a shortened URI to its full form using registered namespace prefixes.

    Parameters
    ----------
    short : str
        Shortened URI (e.g., "rdf__type", "ns0__name")

    Returns
    -------
    str or None
        Full URI or None if prefix not found

    Example
    -------
    RETURN mg_semantics.rdf_full_uri_from_short_form("rdf__type") AS uri;
    // Returns: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    """
    global _NAMESPACE_PREFIXES

    if not _NAMESPACE_PREFIXES:
        _init_prefixes()

    # Parse shortened form (prefix__localName)
    if "__" not in short:
        return None

    parts = short.split("__", 1)
    if len(parts) != 2:
        return None

    prefix, local_name = parts

    namespace = _NAMESPACE_PREFIXES.get(prefix)
    if namespace:
        return namespace + local_name

    return None


@mgp.function
def rdf_short_form_from_full_uri(
    ctx: mgp.FuncCtx,
    uri: str,
) -> str:
    """
    Shorten a full URI using registered namespace prefixes.

    Parameters
    ----------
    uri : str
        Full URI (e.g., "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

    Returns
    -------
    str
        Shortened URI (e.g., "rdf__type")

    Example
    -------
    RETURN mg_semantics.rdf_short_form_from_full_uri(
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    ) AS short;
    // Returns: "rdf__type"
    """
    return _shorten_uri(uri)


@mgp.function
def rdf_get_iri_local_name(
    ctx: mgp.FuncCtx,
    url: str,
) -> str:
    """
    Extract the local name (fragment) from an IRI.

    Parameters
    ----------
    url : str
        Full IRI/URI

    Returns
    -------
    str
        Local name part of the IRI

    Example
    -------
    RETURN mg_semantics.rdf_get_iri_local_name(
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    ) AS localName;
    // Returns: "type"
    """
    _, local_name = _get_namespace_and_local(url)
    return local_name


@mgp.function
def rdf_get_iri_namespace(
    ctx: mgp.FuncCtx,
    url: str,
) -> str:
    """
    Extract the namespace from an IRI.

    Parameters
    ----------
    url : str
        Full IRI/URI

    Returns
    -------
    str
        Namespace part of the IRI

    Example
    -------
    RETURN mg_semantics.rdf_get_iri_namespace(
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    ) AS namespace;
    // Returns: "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    """
    namespace, _ = _get_namespace_and_local(url)
    return namespace


@mgp.function
def rdf_get_lang_tag(
    ctx: mgp.FuncCtx,
    value: mgp.Any,
) -> mgp.Nullable[str]:
    """
    Extract the language tag from a literal value.

    Parameters
    ----------
    value : Any
        A string value potentially containing a language tag

    Returns
    -------
    str or None
        Language tag (e.g., "en", "fr-be") or None if no tag

    Example
    -------
    RETURN mg_semantics.rdf_get_lang_tag("That Seventies Show@en-US") AS tag;
    // Returns: "en-US"
    """
    if not isinstance(value, str):
        return None

    # Look for language tag pattern: value@lang
    match = re.search(r"@([a-zA-Z]{2,3}(?:-[a-zA-Z0-9]+)*)$", value)
    if match:
        return match.group(1)

    return None


@mgp.function
def rdf_has_lang_tag(
    ctx: mgp.FuncCtx,
    lang: str,
    value: mgp.Any,
) -> bool:
    """
    Check if a value has a specific language tag.

    Parameters
    ----------
    lang : str
        Language tag to check for (e.g., "en", "fr")
    value : Any
        Value to check

    Returns
    -------
    bool
        True if value has the specified language tag

    Example
    -------
    RETURN mg_semantics.rdf_has_lang_tag("en", "Hello@en") AS hasTag;
    // Returns: true
    """
    if not isinstance(value, str):
        return False

    tag = rdf_get_lang_tag(ctx, value)
    return tag == lang if tag else False


@mgp.function
def rdf_get_lang_value(
    ctx: mgp.FuncCtx,
    lang: str,
    values: mgp.Any,
) -> mgp.Nullable[str]:
    """
    Get the first value matching a specific language tag from a list of values.

    Parameters
    ----------
    lang : str
        Language tag to filter by
    values : Any
        A single value or list of values

    Returns
    -------
    str or None
        First matching value (without tag) or None

    Example
    -------
    RETURN mg_semantics.rdf_get_lang_value("fr", [
        "Hello@en",
        "Bonjour@fr",
        "Hola@es"
    ]) AS value;
    // Returns: "Bonjour"
    """
    if values is None:
        return None

    # Handle single value
    if isinstance(values, str):
        values = [values]
    elif not isinstance(values, (list, tuple)):
        return None

    for value in values:
        if isinstance(value, str):
            tag = rdf_get_lang_tag(ctx, value)
            if tag == lang:
                # Return value without tag
                return re.sub(r"@[a-zA-Z]{2,3}(?:-[a-zA-Z0-9]+)*$", "", value)

    return None


@mgp.function
def rdf_get_value(
    ctx: mgp.FuncCtx,
    literal: str,
) -> str:
    """
    Extract the raw value from a literal, stripping language tags and datatypes.

    Parameters
    ----------
    literal : str
        Literal value potentially with language tag or datatype

    Returns
    -------
    str
        Raw value without annotations

    Example
    -------
    RETURN mg_semantics.rdf_get_value("Hello@en") AS value;
    // Returns: "Hello"

    RETURN mg_semantics.rdf_get_value("42^^xsd__integer") AS value;
    // Returns: "42"
    """
    if not isinstance(literal, str):
        return str(literal)

    # Remove datatype annotation (value^^type)
    if "^^" in literal:
        literal = literal.split("^^")[0]

    # Remove language tag (value@lang)
    literal = re.sub(r"@[a-zA-Z]{2,3}(?:-[a-zA-Z0-9]+)*$", "", literal)

    return literal


@mgp.function
def rdf_get_data_type(
    ctx: mgp.FuncCtx,
    literal: mgp.Any,
) -> mgp.Nullable[str]:
    """
    Extract the datatype from a literal value.

    Parameters
    ----------
    literal : Any
        Literal value potentially containing a datatype annotation

    Returns
    -------
    str or None
        Datatype IRI/short form or None if no datatype

    Example
    -------
    RETURN mg_semantics.rdf_get_data_type("42^^xsd__integer") AS datatype;
    // Returns: "xsd__integer"
    """
    if not isinstance(literal, str):
        return None

    # Look for datatype pattern: value^^type
    if "^^" in literal:
        parts = literal.split("^^")
        if len(parts) == 2:
            return parts[1]

    return None
