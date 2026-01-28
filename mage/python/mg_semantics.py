"""
Semantics module for Memgraph - similar to n10s (neosemantics) for Neo4j

This module provides functionality for:
- Importing RDF/OWL ontologies
- Handling language-tagged values
- Performing ontology-based inference

Before using this module, you must:
1. Create a uniqueness constraint on Resource nodes:
   CREATE CONSTRAINT ON (r:Resource) ASSERT r.uri IS UNIQUE;
2. Initialize the graph configuration:
   CALL mg_semantics.graphconfig.init();
"""

from typing import Any, Dict, List, Optional, Union
from urllib.request import urlopen, Request
from urllib.error import URLError

import mgp
from rdflib import Graph, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, OWL

# Format name mapping for RDF parsers
# Valid formats: Turtle, N-Triples, JSON-LD, RDF/XML
FORMAT_MAP = {
    "turtle": "turtle",
    "nt": "nt",
    "n-triples": "nt",
    "json-ld": "json-ld",
    "rdf/xml": "xml",
}


class Constants:
    """Constants used throughout the semantics module."""

    # GraphConfig node identifier
    GRAPH_CONFIG_NODE_ID = "__mg_semantics_graphconfig__"

    # Node labels
    RESOURCE_LABEL = "Resource"
    CLASS_LABEL = "Class"
    GRAPH_CONFIG_LABEL = "GraphConfig"

    # Relationship types
    SUB_CLASS_OF_REL = "SCO"

    # Property names
    URI_PROPERTY = "uri"
    NAME_PROPERTY = "name"
    LABEL_PROPERTY = "label"
    ID_PROPERTY = "id"
    COMMENT_PROPERTY = "comment"

    # Configuration parameter names
    CONFIG_RESOURCE_LABEL = "resourceLabel"
    CONFIG_CLASS_LABEL = "classLabel"
    CONFIG_SUB_CLASS_OF_REL = "subClassOfRel"
    CONFIG_URI_PROPERTY = "uriProperty"
    CONFIG_NAME_PROPERTY = "nameProperty"
    CONFIG_LABEL_PROPERTY = "labelProperty"
    CONFIG_HANDLE_VOCAB_URIS = "handleVocabUris"
    CONFIG_KEEP_LANG_TAG = "keepLangTag"
    CONFIG_HANDLE_MULTIVAL = "handleMultival"
    CONFIG_HANDLE_RDF_TYPES = "handleRDFTypes"

    # Configuration default values
    DEFAULT_RESOURCE_LABEL = "Resource"
    DEFAULT_CLASS_LABEL = "Class"
    DEFAULT_SUB_CLASS_OF_REL = "SCO"
    DEFAULT_URI_PROPERTY = "uri"
    DEFAULT_NAME_PROPERTY = "name"
    DEFAULT_LABEL_PROPERTY = "label"
    DEFAULT_HANDLE_VOCAB_URIS = "SHORTEN"
    DEFAULT_KEEP_LANG_TAG = False
    DEFAULT_HANDLE_MULTIVAL = "OVERWRITE"
    DEFAULT_HANDLE_RDF_TYPES = "LABELS"

    # Configuration value options
    HANDLE_MULTIVAL_ARRAY = "ARRAY"
    HANDLE_MULTIVAL_OVERWRITE = "OVERWRITE"


@mgp.write_proc
def graphconfig_init(
    ctx: mgp.ProcCtx, params: Optional[mgp.Map] = None
) -> mgp.Record(param=str, value=Any):
    """
    Initialize the graph configuration with default or custom values.

    Similar to n10s.graphconfig.init in Neo4j neosemantics.

    Parameters:
    -----------
    params : Map, optional
        Configuration parameters to set. If not provided, defaults are used.
        Available parameters:
        - resourceLabel: Label for resource nodes (default: "Resource")
        - classLabel: Label for class nodes (default: "Class")
        - subClassOfRel: Relationship type for subClassOf (default: "SCO")
        - uriProperty: Property name for URI (default: "uri")
        - nameProperty: Property name for name (default: "name")
        - labelProperty: Property name for label (default: "label")
        - handleVocabUris: How to handle vocabulary URIs (default: "SHORTEN")
        - keepLangTag: Whether to keep language tags (default: false)
        - handleMultival: How to handle multiple values (default: "OVERWRITE")
        - handleRDFTypes: How to handle RDF types (default: "LABELS")

    Returns:
    --------
    Records with all configuration parameters and their values
    """
    # Default configuration values (matching n10s defaults)
    defaults = {
        Constants.CONFIG_RESOURCE_LABEL: Constants.DEFAULT_RESOURCE_LABEL,
        Constants.CONFIG_CLASS_LABEL: Constants.DEFAULT_CLASS_LABEL,
        Constants.CONFIG_SUB_CLASS_OF_REL: Constants.DEFAULT_SUB_CLASS_OF_REL,
        Constants.CONFIG_URI_PROPERTY: Constants.DEFAULT_URI_PROPERTY,
        Constants.CONFIG_NAME_PROPERTY: Constants.DEFAULT_NAME_PROPERTY,
        Constants.CONFIG_LABEL_PROPERTY: Constants.DEFAULT_LABEL_PROPERTY,
        Constants.CONFIG_HANDLE_VOCAB_URIS: Constants.DEFAULT_HANDLE_VOCAB_URIS,
        Constants.CONFIG_KEEP_LANG_TAG: Constants.DEFAULT_KEEP_LANG_TAG,
        Constants.CONFIG_HANDLE_MULTIVAL: Constants.DEFAULT_HANDLE_MULTIVAL,
        Constants.CONFIG_HANDLE_RDF_TYPES: Constants.DEFAULT_HANDLE_RDF_TYPES,
    }

    # Merge with provided params
    config = dict(defaults)
    if params:
        for key, value in params.items():
            if key in defaults:
                config[key] = value

    # Find or create GraphConfig node
    config_node = None
    for vertex in ctx.graph.vertices:
        if Constants.GRAPH_CONFIG_LABEL in [label.name for label in vertex.labels]:
            if (
                vertex.properties.get(Constants.ID_PROPERTY)
                == Constants.GRAPH_CONFIG_NODE_ID
            ):
                config_node = vertex
                break

    if config_node is None:
        config_node = ctx.graph.create_vertex()
        config_node.add_label(Constants.GRAPH_CONFIG_LABEL)
        config_node.properties[Constants.ID_PROPERTY] = Constants.GRAPH_CONFIG_NODE_ID

    # Store all config values as properties
    for key, value in config.items():
        config_node.properties[key] = value

    # Return all config parameters
    results = []
    for key, value in config.items():
        results.append(mgp.Record(param=key, value=value))

    return results


@mgp.read_proc
def graphconfig_show(ctx: mgp.ProcCtx) -> mgp.Record(param=str, value=Any):
    """
    Display the current graph configuration.

    Similar to n10s.graphconfig.show in Neo4j neosemantics.

    Returns:
    --------
    Records with all configuration parameters and their values
    """
    # Find GraphConfig node
    config_node = None
    for vertex in ctx.graph.vertices:
        if Constants.GRAPH_CONFIG_LABEL in [label.name for label in vertex.labels]:
            if (
                vertex.properties.get(Constants.ID_PROPERTY)
                == Constants.GRAPH_CONFIG_NODE_ID
            ):
                config_node = vertex
                break

    if config_node is None:
        raise ValueError(
            "Graph configuration not initialized. "
            "Please call mg_semantics.graphconfig.init() first."
        )

    # Return all config parameters
    results = []
    # Filter out the internal 'id' property
    for key, value in config_node.properties.items():
        if key != Constants.ID_PROPERTY:
            results.append(mgp.Record(param=key, value=value))

    return results


@mgp.write_proc
def graphconfig_set(
    ctx: mgp.ProcCtx, params: mgp.Map
) -> mgp.Record(param=str, value=Any):
    """
    Update individual configuration items.

    Similar to n10s.graphconfig.set in Neo4j neosemantics.

    Parameters:
    -----------
    params : Map
        Configuration parameters to update

    Returns:
    --------
    Records with all updated configuration parameters and their values
    """
    # Find GraphConfig node
    config_node = None
    for vertex in ctx.graph.vertices:
        if Constants.GRAPH_CONFIG_LABEL in [label.name for label in vertex.labels]:
            if (
                vertex.properties.get(Constants.ID_PROPERTY)
                == Constants.GRAPH_CONFIG_NODE_ID
            ):
                config_node = vertex
                break

    if config_node is None:
        raise ValueError(
            "Graph configuration not initialized. "
            "Please call mg_semantics.graphconfig.init() first."
        )

    # Update specified parameters
    updated = []
    for key, value in params.items():
        if key != Constants.ID_PROPERTY:  # Don't allow updating the internal id
            config_node.properties[key] = value
            updated.append(mgp.Record(param=key, value=value))

    return updated


@mgp.write_proc
def graphconfig_drop(ctx: mgp.ProcCtx) -> mgp.Record():
    """
    Remove the graph configuration.

    Similar to n10s.graphconfig.drop in Neo4j neosemantics.

    Returns:
    --------
    Empty record
    """
    # Find and delete GraphConfig node
    config_node = None
    for vertex in ctx.graph.vertices:
        if Constants.GRAPH_CONFIG_LABEL in [label.name for label in vertex.labels]:
            if (
                vertex.properties.get(Constants.ID_PROPERTY)
                == Constants.GRAPH_CONFIG_NODE_ID
            ):
                config_node = vertex
                break

    if config_node is not None:
        # Note: Memgraph doesn't have a direct delete_vertex method in mgp
        # We'll mark it for deletion by removing all properties and labels
        # In practice, you might need to use a Cypher query to delete it
        # For now, we'll just clear it
        config_node.properties.clear()
        for label in list(config_node.labels):
            config_node.remove_label(label.name)

    return mgp.Record()


def _get_graph_config(ctx: mgp.ProcCtx) -> Dict[str, Any]:
    """
    Retrieve the current graph configuration from the graph.

    Returns:
        Dictionary with configuration values
    """
    config_node = None
    for vertex in ctx.graph.vertices:
        if Constants.GRAPH_CONFIG_LABEL in [label.name for label in vertex.labels]:
            if (
                vertex.properties.get(Constants.ID_PROPERTY)
                == Constants.GRAPH_CONFIG_NODE_ID
            ):
                config_node = vertex
                break

    if config_node is None:
        raise ValueError(
            "Graph configuration not initialized. "
            "Please call mg_semantics.graphconfig.init() first."
        )

    config = {}
    for key, value in config_node.properties.items():
        if key != Constants.ID_PROPERTY:
            config[key] = value

    return config


def _merge_config(
    stored_config: Dict[str, Any], provided_config: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Merge provided config with stored config, with provided config taking precedence.

    Args:
        stored_config: Configuration from graph
        provided_config: Optional config provided to procedure

    Returns:
        Merged configuration dictionary
    """
    merged = dict(stored_config)
    if provided_config:
        merged.update(provided_config)
    return merged


@mgp.write_proc
def rdf_import_fetch(
    ctx: mgp.ProcCtx, url: str, format: str = "turtle", config: Optional[mgp.Map] = None
) -> mgp.Record(
    terminationStatus=str,
    triplesLoaded=int,
    triplesParsed=int,
    namespaces=int,
    extraInfo=str,
):
    """
    Import RDF data from a URL into Memgraph.

    Similar to n10s.rdf.import.fetch in Neo4j neosemantics.

    Parameters:
    -----------
    url : str
        URL to fetch RDF data from
    format : str
        RDF format. Valid formats: Turtle, N-Triples, JSON-LD, RDF/XML. Default: "turtle"
    config : Map, optional
        Optional configuration overrides. If not provided, uses stored GraphConfig.
        Configuration options:
        - resourceLabel: Label for resource nodes
        - classLabel: Label for class nodes
        - subClassOfRel: Relationship type for subClassOf
        - uriProperty: Property name for URI
        - nameProperty: Property name for name
        - labelProperty: Property name for label
        - handleVocabUris: How to handle vocabulary URIs (IGNORE, SHORTEN, KEEP)
        - keepLangTag: Whether to keep language tags
        - handleMultival: How to handle multiple values (ARRAY, OVERWRITE)

    Returns:
    --------
    Record with import statistics
    """
    # Fetch RDF data from URL
    request = Request(url)
    request.add_header("User-Agent", "Memgraph MAGE semantics module")

    try:
        response = urlopen(request)
        rdf_data = response.read().decode("utf-8")
    except URLError as e:
        raise URLError(f"Failed to fetch RDF from URL: {e}")
    except Exception as e:
        raise Exception(f"Error reading RDF data: {e}")

    # Parse RDF
    # Normalize format name (rdflib expects lowercase)
    normalized_format = format.lower() if format else "turtle"
    rdf_format = FORMAT_MAP.get(normalized_format, normalized_format)

    rdf_graph = Graph()
    try:
        rdf_graph.parse(data=rdf_data, format=rdf_format)
    except Exception as e:
        raise Exception(f"Failed to parse RDF data: {e}")

    # Get stored config and merge with provided config
    stored_config = _get_graph_config(ctx)
    provided_config_dict = {}
    if config:
        for key in config:
            provided_config_dict[key] = config[key]

    config_dict = _merge_config(stored_config, provided_config_dict)

    # Import into graph
    stats = _import_rdf_graph(ctx, rdf_graph, config_dict)

    return mgp.Record(
        terminationStatus="OK",
        triplesLoaded=stats["triplesLoaded"],
        triplesParsed=stats["triplesLoaded"],
        namespaces=0,  # Could be enhanced to count namespaces
        extraInfo="",
    )


@mgp.function
def rdf_get_lang_value(
    lang: mgp.Nullable[str], values: mgp.Nullable[mgp.Any]
) -> mgp.Nullable[str]:
    """
    Get a language-specific value from a list of language-tagged values.

    Similar to n10s.rdf.getLangValue in Neo4j neosemantics.

    Parameters:
    -----------
    lang : str, optional
        Language code (e.g., 'en', 'es', 'fr'). If None, returns first value.
    values : Any, optional
        List of language-tagged values or a single value. If None, returns None.

    Returns:
    --------
    The value for the specified language, or None if not found
    """
    return _get_lang_value(lang, values)


@mgp.function
def rdf_get_value(literal: mgp.Nullable[mgp.Any]) -> mgp.Nullable[str]:
    """
    Get the value of a literal after stripping out datatype and language information.

    Similar to n10s.rdf.getValue in Neo4j neosemantics.

    Parameters:
    -----------
    literal : Any, optional
        A literal value which may be:
        - A plain string
        - A dictionary/map with 'value' key (language-tagged or datatyped)
        - A list of language-tagged values (returns first value)
        - None

    Returns:
    --------
    The string value without datatype/language information, or None if not found
    """
    return _get_value_from_literal(literal)


@mgp.write_proc
def rdf_import_inline(
    ctx: mgp.ProcCtx,
    payload: str,
    format: str = "turtle",
    config: Optional[mgp.Map] = None,
) -> mgp.Record(
    terminationStatus=str,
    triplesLoaded=int,
    triplesParsed=int,
    namespaces=int,
    extraInfo=str,
):
    """
    Import an ontology from an inline string.

    Similar to n10s.onto.import.inline in Neo4j neosemantics.

    Parameters:
    -----------
    payload : str
        RDF/OWL ontology as a string
    format : str
        RDF format (turtle, rdfxml, n3, etc.). Default: "turtle"
    config : Map, optional
        Optional configuration overrides. If not provided, uses stored GraphConfig.
        Configuration options (same as rdf_import_fetch)

    Returns:
    --------
    Record with import statistics
    """
    # Parse RDF
    # Normalize format name (rdflib expects lowercase)
    normalized_format = format.lower() if format else "turtle"
    rdf_format = FORMAT_MAP.get(normalized_format, normalized_format)

    rdf_graph = Graph()
    try:
        rdf_graph.parse(data=payload, format=rdf_format)
    except Exception as e:
        raise Exception(f"Failed to parse RDF data: {e}")

    # Get stored config and merge with provided config
    stored_config = _get_graph_config(ctx)
    provided_config_dict = {}
    if config:
        for key in config:
            provided_config_dict[key] = config[key]

    config_dict = _merge_config(stored_config, provided_config_dict)

    # Import into graph
    stats = _import_rdf_graph(ctx, rdf_graph, config_dict)

    return mgp.Record(
        terminationStatus="OK",
        triplesLoaded=stats["triplesLoaded"],
        triplesParsed=stats["triplesLoaded"],
        namespaces=0,
        extraInfo="",
    )


@mgp.function
def inference_in_category(
    node: mgp.Vertex, category: mgp.Vertex, params: Optional[mgp.Map] = None
) -> bool:
    """
    Check if a node belongs to a category through ontology inference.

    Similar to n10s.inference.inCategory in Neo4j neosemantics.

    Parameters:
    -----------
    node : Vertex
        The node to check
    category : Vertex
        The category/class node
    params : Map, optional
        Configuration parameters:
        - inCatRel: Relationship type connecting instance nodes to category (default: "IN_CAT")
        - subCatRel: Relationship type connecting child category to parent (default: "SCO")

    Returns:
    --------
    True if the node belongs to the category (directly or through hierarchy)
    """
    # Get parameters with defaults
    in_cat_rel = "IN_CAT"
    sub_class_of_rel = "SCO"

    if params:
        if "inCatRel" in params:
            in_cat_rel = params["inCatRel"]
        if "subCatRel" in params:
            sub_class_of_rel = params["subCatRel"]

    return _is_in_category(node, category, in_cat_rel, sub_class_of_rel)


@mgp.read_proc
def inference_nodes_in_category(
    ctx: mgp.ProcCtx,
    category: mgp.Vertex,
    params: Optional[mgp.Map] = None,
) -> mgp.Record(node=mgp.Vertex):
    """
    Find all nodes that belong to a category through ontology inference.

    Similar to n10s.inference.nodesInCategory in Neo4j neosemantics.

    Parameters:
    -----------
    category : Vertex
        The category/class node
    params : Map, optional
        Configuration parameters:
        - inCatRel: Relationship type connecting instance nodes to category (default: "IN_CAT")
        - subCatRel: Relationship type connecting child category to parent (default: "SCO")

    Returns:
    --------
    Records with nodes that belong to the category
    """
    # Get parameters with defaults
    in_cat_rel = "IN_CAT"
    sub_class_of_rel = "SCO"

    if params:
        if "inCatRel" in params:
            in_cat_rel = params["inCatRel"]
        if "subCatRel" in params:
            sub_class_of_rel = params["subCatRel"]

    results = []

    for vertex in ctx.graph.vertices:
        if _is_in_category(vertex, category, in_cat_rel, sub_class_of_rel):
            results.append(mgp.Record(node=vertex))

    return results


# Private helper functions


def _get_or_create_resource_node(
    ctx: mgp.ProcCtx,
    uri: str,
    node_type: str,
    config: Dict[str, Any],
    nodes_by_uri: Optional[Dict[str, mgp.Vertex]] = None,
) -> mgp.Vertex:
    """
    Get or create a Resource node with the given URI.
    Returns the vertex if it exists, creates it otherwise.

    Args:
        ctx: Memgraph procedure context
        uri: URI of the resource
        node_type: Label for the node
        config: Configuration dictionary with property names
        nodes_by_uri: Optional dictionary to cache nodes by URI (for efficiency)
    """
    uri_property = config.get(
        Constants.CONFIG_URI_PROPERTY, Constants.DEFAULT_URI_PROPERTY
    )

    # Check cache first if provided
    if nodes_by_uri is not None and uri in nodes_by_uri:
        return nodes_by_uri[uri]

    # Try to find existing node with this URI
    for vertex in ctx.graph.vertices:
        if uri_property in vertex.properties:
            if vertex.properties[uri_property] == uri:
                # Cache it if dictionary provided
                if nodes_by_uri is not None:
                    nodes_by_uri[uri] = vertex
                return vertex

    # Create new node
    vertex = ctx.graph.create_vertex()
    vertex.add_label(node_type)
    vertex.properties[uri_property] = uri

    # Cache it if dictionary provided
    if nodes_by_uri is not None:
        nodes_by_uri[uri] = vertex

    return vertex


def _get_lang_value(
    lang: Optional[str], values: Optional[Any]
) -> Optional[str]:
    """
    Extract value for a specific language from a list of language-tagged values.

    Args:
        lang: Language code (e.g., 'en', 'es', 'fr'). If None, returns first value.
        values: List of language-tagged values or single value. If None, returns None.

    Returns:
        The value for the specified language, or None if not found
    """
    if values is None:
        return None

    if isinstance(values, str):
        return values

    # Check if it's an iterable collection (list, tuple, mgp.List)
    if not isinstance(values, (mgp.List, list, tuple)):
        # Not a list, treat as single value
        return str(values)

    # Convert to list for easier iteration (mgp.List is iterable but we need to handle it)
    try:
        values_list = list(values) if isinstance(values, mgp.List) else values
    except (TypeError, AttributeError):
        return str(values)

    if not values_list:
        return None

    # If lang is None, return first value
    if lang is None:
        first_val = values_list[0]
        if isinstance(first_val, dict):
            return str(first_val.get("value", ""))
        elif isinstance(first_val, mgp.Map):
            return str(first_val.get("value", ""))
        return str(first_val)

    # Look for exact language match
    for value in values_list:
        if isinstance(value, dict):
            if value.get("lang") == lang:
                return str(value.get("value", ""))
        elif isinstance(value, mgp.Map):
            if value.get("lang") == lang:
                return str(value.get("value", ""))

    # If no exact match, return first value
    first_val = values_list[0]
    if isinstance(first_val, dict):
        return str(first_val.get("value", ""))
    elif isinstance(first_val, mgp.Map):
        return str(first_val.get("value", ""))
    return str(first_val)


def _get_value(value: Union[Dict, str, Any]) -> str:
    """
    Extract the actual value from a language-tagged value or plain value.
    """
    if isinstance(value, dict):
        return value.get("value", str(value))
    return str(value)


def _get_value_from_literal(literal: Optional[Any]) -> Optional[str]:
    """
    Extract the value from a literal, stripping datatype and language information.

    Args:
        literal: A literal value which may be a string, dict/map with 'value' key,
                 a list of values, or None

    Returns:
        The string value without datatype/language information, or None
    """
    if literal is None:
        return None

    if isinstance(literal, str):
        return literal

    # Handle dictionary/map with 'value' key
    if isinstance(literal, dict):
        return str(literal.get("value", ""))
    elif isinstance(literal, mgp.Map):
        return str(literal.get("value", ""))

    # Handle list - return first value
    if isinstance(literal, (mgp.List, list, tuple)):
        try:
            values_list = list(literal) if isinstance(literal, mgp.List) else literal
            if values_list:
                first_val = values_list[0]
                if isinstance(first_val, dict):
                    return str(first_val.get("value", ""))
                elif isinstance(first_val, mgp.Map):
                    return str(first_val.get("value", ""))
                return str(first_val)
        except (TypeError, AttributeError):
            pass

    # For any other type, convert to string
    return str(literal)


def _parse_rdf_literal(literal: Literal) -> Dict[str, Any]:
    """
    Parse an RDF literal into a dictionary with value and optional language tag.
    """
    result = {"value": str(literal)}
    if literal.language:
        result["lang"] = literal.language
    return result


def _handle_rdf_type_predicate(
    subject_node: mgp.Vertex, obj: Any, class_label: str
) -> None:
    """Handle RDF.type predicate."""
    if obj == OWL.Class:
        if class_label not in [label.name for label in subject_node.labels]:
            subject_node.add_label(class_label)


def _handle_subclassof_predicate(
    ctx: mgp.ProcCtx,
    subject_node: mgp.Vertex,
    obj: Any,
    rdf_graph: Graph,
    config: Dict[str, Any],
    nodes_by_uri: Dict[str, mgp.Vertex],
    node_properties: Dict[str, Dict[str, Any]],
    stats: Dict[str, int],
) -> None:
    """Handle RDFS.subClassOf predicate."""
    if isinstance(obj, URIRef):
        obj_uri = str(obj)
        class_label = config.get(Constants.CONFIG_CLASS_LABEL)
        sub_class_of_rel = config.get(Constants.CONFIG_SUB_CLASS_OF_REL)

        if obj_uri not in nodes_by_uri:
            obj_node = _get_or_create_resource_node(
                ctx, obj_uri, class_label, config, nodes_by_uri
            )
            nodes_by_uri[obj_uri] = obj_node
            node_properties[obj_uri] = {}
            stats["nodesCreated"] += 1
        else:
            obj_node = nodes_by_uri[obj_uri]

        # Check if relationship already exists
        relationship_exists = False
        for edge in subject_node.out_edges:
            if edge.type.name == sub_class_of_rel and edge.to_vertex == obj_node:
                relationship_exists = True
                break

        if not relationship_exists:
            ctx.graph.create_edge(
                subject_node, obj_node, mgp.EdgeType(sub_class_of_rel)
            )
            stats["relationshipsCreated"] += 1


def _handle_label_predicate(
    subject_uri: str,
    obj: Any,
    label_property: str,
    keep_lang_tag: bool,
    handle_multival: str,
    node_properties: Dict[str, Dict[str, Any]],
) -> None:
    """Handle RDFS.label predicate."""
    if isinstance(obj, Literal):
        label_data = _parse_rdf_literal(obj)
        if subject_uri not in node_properties:
            node_properties[subject_uri] = {}

        if label_property not in node_properties[subject_uri]:
            if keep_lang_tag and handle_multival == Constants.HANDLE_MULTIVAL_ARRAY:
                node_properties[subject_uri][label_property] = []
            else:
                node_properties[subject_uri][label_property] = None

        if keep_lang_tag and handle_multival == Constants.HANDLE_MULTIVAL_ARRAY:
            if isinstance(node_properties[subject_uri][label_property], list):
                node_properties[subject_uri][label_property].append(label_data)
            else:
                node_properties[subject_uri][label_property] = [label_data]
        else:
            node_properties[subject_uri][label_property] = label_data


def _handle_comment_predicate(
    subject_uri: str,
    obj: Any,
    keep_lang_tag: bool,
    handle_multival: str,
    node_properties: Dict[str, Dict[str, Any]],
) -> None:
    """Handle RDFS.comment predicate."""
    if isinstance(obj, Literal):
        comment_data = _parse_rdf_literal(obj)
        if subject_uri not in node_properties:
            node_properties[subject_uri] = {}

        prop_name = Constants.COMMENT_PROPERTY
        if prop_name not in node_properties[subject_uri]:
            if keep_lang_tag and handle_multival == Constants.HANDLE_MULTIVAL_ARRAY:
                node_properties[subject_uri][prop_name] = []
            else:
                node_properties[subject_uri][prop_name] = None

        if keep_lang_tag and handle_multival == Constants.HANDLE_MULTIVAL_ARRAY:
            if isinstance(node_properties[subject_uri][prop_name], list):
                node_properties[subject_uri][prop_name].append(comment_data)
            else:
                node_properties[subject_uri][prop_name] = [comment_data]
        else:
            node_properties[subject_uri][prop_name] = comment_data


def _import_rdf_graph(
    ctx: mgp.ProcCtx, rdf_graph: Graph, config: Dict[str, Any]
) -> Dict[str, int]:
    """
    Import an RDF graph into Memgraph.

    Args:
        ctx: Memgraph procedure context
        rdf_graph: RDFLib Graph object
        config: Configuration dictionary with options:
            - resourceLabel: Label for resource nodes (required)
            - classLabel: Label for class nodes (required)
            - subClassOfRel: Relationship type for subClassOf (required)
            - uriProperty: Property name for URI (required)
            - nameProperty: Property name for name (required)
            - labelProperty: Property name for label (required)
            - handleVocabUris: How to handle vocabulary URIs (IGNORE, SHORTEN, KEEP)
            - keepLangTag: Whether to keep language tags (default: True)
            - handleMultival: How to handle multiple values (ARRAY, OVERWRITE)

    Returns:
        Dictionary with statistics about imported triples
    """
    # Required configuration values
    resource_label = config.get(Constants.CONFIG_RESOURCE_LABEL)
    class_label = config.get(Constants.CONFIG_CLASS_LABEL)
    sub_class_of_rel = config.get(Constants.CONFIG_SUB_CLASS_OF_REL)
    uri_property = config.get(Constants.CONFIG_URI_PROPERTY)
    name_property = config.get(Constants.CONFIG_NAME_PROPERTY)
    label_property = config.get(Constants.CONFIG_LABEL_PROPERTY)

    if not all(
        [
            resource_label,
            class_label,
            sub_class_of_rel,
            uri_property,
            name_property,
            label_property,
        ]
    ):
        raise ValueError(
            f"Config must include: {Constants.CONFIG_RESOURCE_LABEL}, {Constants.CONFIG_CLASS_LABEL}, "
            f"{Constants.CONFIG_SUB_CLASS_OF_REL}, {Constants.CONFIG_URI_PROPERTY}, "
            f"{Constants.CONFIG_NAME_PROPERTY}, {Constants.CONFIG_LABEL_PROPERTY}"
        )

    keep_lang_tag = config.get(
        Constants.CONFIG_KEEP_LANG_TAG, Constants.DEFAULT_KEEP_LANG_TAG
    )
    handle_multival = config.get(
        Constants.CONFIG_HANDLE_MULTIVAL, Constants.DEFAULT_HANDLE_MULTIVAL
    )

    stats = {"triplesLoaded": 0, "nodesCreated": 0, "relationshipsCreated": 0}

    # Dictionary to store nodes by URI
    nodes_by_uri: Dict[str, mgp.Vertex] = {}

    # Dictionary to store properties for each node
    node_properties: Dict[str, Dict[str, Any]] = {}

    # Process all triples
    for subject, predicate, obj in rdf_graph:
        # Skip blank nodes for now
        if isinstance(subject, BNode) or isinstance(obj, BNode):
            continue

        subject_uri = str(subject)

        # Get or create subject node
        if subject_uri not in nodes_by_uri:
            # Determine node type
            node_type = resource_label
            if (subject, RDF.type, OWL.Class) in rdf_graph:
                node_type = class_label

            node = _get_or_create_resource_node(
                ctx, subject_uri, node_type, config, nodes_by_uri
            )
            nodes_by_uri[subject_uri] = node
            node_properties[subject_uri] = {}
            stats["nodesCreated"] += 1

        subject_node = nodes_by_uri[subject_uri]

        # Handle special predicates
        if predicate == RDF.type:
            _handle_rdf_type_predicate(subject_node, obj, class_label)
        elif predicate == RDFS.subClassOf:
            _handle_subclassof_predicate(
                ctx,
                subject_node,
                obj,
                rdf_graph,
                config,
                nodes_by_uri,
                node_properties,
                stats,
            )
        elif predicate == RDFS.label:
            _handle_label_predicate(
                subject_uri,
                obj,
                label_property,
                keep_lang_tag,
                handle_multival,
                node_properties,
            )
        elif predicate == RDFS.comment:
            _handle_comment_predicate(
                subject_uri, obj, keep_lang_tag, handle_multival, node_properties
            )

        stats["triplesLoaded"] += 1

    # Set properties on nodes
    for uri, props in node_properties.items():
        node = nodes_by_uri[uri]
        for prop_name, prop_value in props.items():
            node.properties[prop_name] = prop_value

        # Set name property from URI (short name)
        if name_property not in node.properties:
            # Extract short name from URI
            uri_parts = uri.split("#")
            if len(uri_parts) > 1:
                name = uri_parts[-1]
            else:
                uri_parts = uri.split("/")
                name = uri_parts[-1]
            node.properties[name_property] = name

    return stats


def _is_in_category(
    node: mgp.Vertex, category_node: mgp.Vertex, in_cat_rel: str, sub_class_of_rel: str
) -> bool:
    """
    Check if a node is in a category by traversing the ontology hierarchy.

    Args:
        node: The node to check
        category_node: The category/class node
        in_cat_rel: Relationship type that connects nodes to categories
        sub_class_of_rel: Relationship type for subClassOf

    Returns:
        True if node is in category (directly or through hierarchy)
    """
    # Check direct connection
    for edge in node.out_edges:
        if edge.type.name == in_cat_rel:
            target = edge.to_vertex
            if target == category_node:
                return True

            # Check if target is a subclass of category_node
            if _is_subclass_of(target, category_node, sub_class_of_rel):
                return True

    return False


def _is_subclass_of(
    subclass_node: mgp.Vertex, superclass_node: mgp.Vertex, sub_class_of_rel: str
) -> bool:
    """
    Check if subclass_node is a subclass of superclass_node by traversing subClassOf relationships.

    Args:
        subclass_node: The potential subclass
        superclass_node: The potential superclass
        sub_class_of_rel: Relationship type for subClassOf

    Returns:
        True if subclass_node is a subclass of superclass_node
    """
    if subclass_node == superclass_node:
        return True

    # Traverse subClassOf relationships
    visited = set()
    to_visit = [subclass_node]

    while to_visit:
        current = to_visit.pop()
        if current.id in visited:
            continue
        visited.add(current.id)

        if current == superclass_node:
            return True

        # Follow subClassOf relationships
        for edge in current.out_edges:
            if edge.type.name == sub_class_of_rel:
                to_visit.append(edge.to_vertex)

    return False
