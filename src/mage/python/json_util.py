import json
from datetime import date, datetime, time, timedelta
from io import TextIOWrapper
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import mgp


def _convert_value_to_json_compatible(value: Any) -> Any:
    """Helper function to convert Memgraph values to JSON-compatible Python types."""
    if isinstance(value, mgp.Vertex):
        return {
            "type": "node",
            "id": value.id,
            "labels": [label.name for label in value.labels],
            "properties": {k: _convert_value_to_json_compatible(v) for k, v in value.properties.items()},
        }
    elif isinstance(value, mgp.Edge):
        return {
            "type": "relationship",
            "id": value.id,
            "start": value.from_vertex.id,
            "end": value.to_vertex.id,
            "relationship_type": value.type.name,
            "properties": {k: _convert_value_to_json_compatible(v) for k, v in value.properties.items()},
        }
    elif isinstance(value, mgp.Path):
        return {
            "type": "path",
            "start": _convert_value_to_json_compatible(value.vertices[0]),
            "end": _convert_value_to_json_compatible(value.vertices[-1]),
            "nodes": [_convert_value_to_json_compatible(v) for v in value.vertices],
            "relationships": [_convert_value_to_json_compatible(e) for e in value.edges],
        }
    elif isinstance(value, (list, tuple)):
        return [_convert_value_to_json_compatible(item) for item in value]
    elif isinstance(value, dict):
        return {k: _convert_value_to_json_compatible(v) for k, v in value.items()}
    elif isinstance(value, (datetime, date)):
        return value.isoformat()
    elif isinstance(value, time):
        return value.isoformat()
    elif isinstance(value, timedelta):
        total_seconds = value.total_seconds()
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        microseconds = value.microseconds
        return f"P0DT{hours}H{minutes}M{seconds}.{microseconds:06d}S"
    elif isinstance(value, (int, float, str, bool)) or value is None:
        return value
    else:
        return str(value)


def extract_objects(file: TextIOWrapper):
    """Helper function to extract objects from a JSON file."""
    objects = json.load(file)
    if type(objects) is dict:
        objects = [objects]
    return objects


@mgp.function
def to_json(value: Any):
    converted = _convert_value_to_json_compatible(value)
    return json.dumps(converted, ensure_ascii=False)


@mgp.function
def from_json_list(json_str: mgp.Nullable[str]):
    if json_str is None:
        return None

    value = json.loads(json_str)
    if not isinstance(value, list):
        raise ValueError("Input JSON must represent a list")
    return value


@mgp.read_proc
def load_from_path(ctx: mgp.ProcCtx, path: str) -> mgp.Record(objects=mgp.List[object]):
    file = Path(path)
    if file.exists():
        opened_file = open(file)
        objects = extract_objects(opened_file)
    else:
        raise FileNotFoundError("There is no file " + path)

    opened_file.close()

    return mgp.Record(objects=objects)


@mgp.read_proc
def load_from_str(ctx: mgp.ProcCtx, json_str: str) -> mgp.Record(objects=mgp.List[object]):
    """
    Procedure to load JSON from a string.

    Parameters
    ----------
    json_str : str
        JSON string that is being loaded.
    """
    objects = json.loads(json_str)
    if type(objects) is dict:
        objects = [objects]

    return mgp.Record(objects=objects)


@mgp.read_proc
def load_from_url(ctx: mgp.ProcCtx, url: str) -> mgp.Record(objects=mgp.List[object]):
    request = Request(url)
    request.add_header("User-Agent", "MAGE module")
    try:
        content = urlopen(request)
    except URLError as url_error:
        raise url_error
    else:
        objects = extract_objects(content)

    return mgp.Record(objects=objects)
