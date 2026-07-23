# Complete code
import json
from mgp import Vertex, Edge

def serialize_vertex(vertex: Vertex) -> dict:
    """
    Serialize a Vertex object into a JSON-compatible dictionary.
    
    Args:
    vertex (Vertex): The Vertex object to serialize.
    
    Returns:
    dict: A dictionary representation of the Vertex object.
    """
    data = {
        "id": vertex.id,
        "label": vertex.label,
        "properties": {}
    }
    
    for key, value in vertex.properties.items():
        if isinstance(value, list):
            data["properties"][key] = [serialize_value(v) for v in value]
        else:
            data["properties"][key] = serialize_value(value)
    
    return data

def serialize_edge(edge: Edge) -> dict:
    """
    Serialize an Edge object into a JSON-compatible dictionary.
    
    Args:
    edge (Edge): The Edge object to serialize.
    
    Returns:
    dict: A dictionary representation of the Edge object.
    """
    data = {
        "id": edge.id,
        "label": edge.label,
        "start_vertex_id": edge.start_vertex_id,
        "end_vertex_id": edge.end_vertex_id,
        "properties": {}
    }
    
    for key, value in edge.properties.items():
        if isinstance(value, list):
            data["properties"][key] = [serialize_value(v) for v in value]
        else:
            data["properties"][key] = serialize_value(value)
    
    return data

def serialize_value(value):
    """
    Serialize a value into a JSON-compatible format.
    
    Args:
    value: The value to serialize.
    
    Returns:
    dict: A dictionary representation of the value.
    """
    if isinstance(value, Vertex):
        return serialize_vertex(value).copy()
    elif isinstance(value, Edge):
        return serialize_edge(value).copy()
    elif isinstance(value, list):
        return [serialize_value(v) for v in value]
    else:
        return value

def to_json(obj):
    """
    Serialize an object into a JSON string.
    
    Args:
    obj: The object to serialize.
    
    Returns:
    str: A JSON string representation of the object.
    """
    return json.dumps(obj, default=serialize_value)