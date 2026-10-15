import ast
import json as js
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Union

import defusedxml.ElementTree as ET
import gqlalchemy
import mgp
from mage.export_import_util.parameters import Parameter


@dataclass
class Node:
    id: int
    labels: list
    properties: dict

    def get_dict(self) -> dict:
        return {
            Parameter.ID.value: self.id,
            Parameter.LABELS.value: self.labels,
            Parameter.PROPERTIES.value: self.properties,
            Parameter.TYPE.value: Parameter.NODE.value,
        }


@dataclass
class Relationship:
    end: int
    id: int
    label: str
    properties: dict
    start: int
    id: int

    def get_dict(self) -> dict:
        return {
            Parameter.END.value: self.end,
            Parameter.ID.value: self.id,
            Parameter.LABEL.value: self.label,
            Parameter.PROPERTIES.value: self.properties,
            Parameter.START.value: self.start,
            Parameter.TYPE.value: Parameter.RELATIONSHIP.value,
        }


@dataclass
class KeyObjectGraphML:
    name: str
    is_for: str
    type: str
    type_is_list: bool
    default_value: str
    id: str = None

    def __init__(
        self,
        name: str,
        is_for: str,
        type: str = "",
        type_is_list: str = False,
        default_value: str = "",
    ):
        self.name = name
        self.is_for = is_for
        self.type = type
        self.type_is_list = type_is_list
        self.default_value = default_value

    def __hash__(self):
        return hash(
            (
                self.name,
                self.is_for,
                self.type,
                self.type_is_list,
                self.default_value,
            )
        )

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return (
            self.name == other.name
            and self.is_for == other.is_for
            and self.type == other.type
            and self.type_is_list == other.type_is_list
            and self.default_value == other.default_value
        )


def convert_to_isoformat(
    property: Union[
        None,
        str,
        bool,
        int,
        float,
        List[Any],
        Dict[str, Any],
        timedelta,
        time,
        datetime,
        date,
    ]
):
    if isinstance(property, timedelta):
        return Parameter.DURATION.value + str(property) + ")"

    elif isinstance(property, time):
        return Parameter.LOCALTIME.value + property.isoformat() + ")"

    elif isinstance(property, datetime):
        return Parameter.LOCALDATETIME.value + property.isoformat() + ")"

    elif isinstance(property, date):
        return Parameter.DATE.value + property.isoformat() + ")"

    else:
        return property


def to_duration_isoformat(value: timedelta) -> str:
    """Converts timedelta to ISO-8601 duration: P<date>T<time>"""
    date_parts: List[str] = []
    time_parts: List[str] = []

    if value.days != 0:
        date_parts.append(f"{abs(value.days)}D")

    if value.seconds != 0 or value.microseconds != 0:
        abs_seconds = abs(value.seconds)
        minutes, seconds = divmod(abs_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        microseconds = value.microseconds

        if hours > 0:
            time_parts.append(f"{hours}H")
        if minutes > 0:
            time_parts.append(f"{minutes}M")
        if seconds > 0 or microseconds > 0:
            microseconds_part = f".{abs(value.microseconds)}" if value.microseconds != 0 else ""
            time_parts.append(f"{seconds}{microseconds_part}S")

    date_duration_str = "".join(date_parts)
    time_duration_str = f'T{"".join(time_parts)}' if time_parts else ""

    return f"P{date_duration_str}{time_duration_str}"


def convert_to_isoformat_graphML(
    property: Union[
        None,
        str,
        bool,
        int,
        float,
        List[Any],
        Dict[str, Any],
        timedelta,
        time,
        datetime,
        date,
    ]
):
    if isinstance(property, timedelta):
        return to_duration_isoformat(property)

    if isinstance(property, (time, date, datetime)):
        return property.isoformat()

    else:
        return property


def get_graph(
    ctx: mgp.ProcCtx,
    config: Union[mgp.Map, None] = {
        "graphML": False,
        "leaveOutLabels": False,
        "leaveOutProperties": False,
    },
) -> List[Union[Node, Relationship]]:
    """
    config : Map
        - graphML: bool
        - leaveOutLabels: bool
        - leaveOutProperties: bool

    """
    nodes = list()
    relationships = list()

    for vertex in ctx.graph.vertices:
        labels = []
        properties = dict()
        if not config.get("leaveOutLabels"):
            labels = [label.name for label in vertex.labels]
        if config.get("graphML") and not config.get("leaveOutProperties"):
            properties = {
                key: convert_to_isoformat_graphML(vertex.properties.get(key)) for key in vertex.properties.keys()
            }
        elif not config.get("leaveOutProperties"):
            properties = {key: convert_to_isoformat(vertex.properties.get(key)) for key in vertex.properties.keys()}

        nodes.append(Node(vertex.id, labels, properties).get_dict())

        for edge in vertex.out_edges:
            if not config.get("leaveOutProperties"):
                properties = {key: convert_to_isoformat(edge.properties.get(key)) for key in edge.properties.keys()}

            relationships.append(
                Relationship(
                    edge.to_vertex.id,
                    edge.id,
                    edge.type.name,
                    properties,
                    edge.from_vertex.id,
                ).get_dict()
            )

    return nodes + relationships


def convert_from_isoformat(property: Union[None, str, bool, int, float, List[Any], Dict[str, Any]]):
    if not isinstance(property, str):
        return property

    if str.startswith(property, Parameter.DURATION.value):
        duration_iso = property.split("(")[-1].split(")")[0]
        parsed_time = datetime.strptime(duration_iso, "%H:%M:%S.%f")
        return timedelta(
            hours=parsed_time.hour,
            minutes=parsed_time.minute,
            seconds=parsed_time.second,
            microseconds=parsed_time.microsecond,
        )
    elif str.startswith(property, Parameter.LOCALTIME.value):
        local_time_iso = property.split("(")[-1].split(")")[0]
        return time.fromisoformat(local_time_iso)
    elif str.startswith(property, Parameter.LOCALDATETIME.value):
        local_datetime_iso = property.split("(")[-1].split(")")[0]
        return datetime.fromisoformat(local_datetime_iso)
    elif str.startswith(property, Parameter.DATE.value):
        date_iso = property.split("(")[-1].split(")")[0]
        return date.fromisoformat(date_iso)
    else:
        return property


def create_vertex(ctx: mgp.ProcCtx, properties: Dict[str, Any], labels: List[str]):
    vertex = ctx.graph.create_vertex()
    vertex_properties = vertex.properties

    for key, value in properties.items():
        vertex_properties[key] = convert_from_isoformat(value)

    for label in labels:
        vertex.add_label(label)

    return vertex.id


def create_edge(
    ctx: mgp.ProcCtx,
    properties: Dict[str, Any],
    start_node_id: Union[int, str],
    end_node_id: Union[int, str],
    type: str,
    vertex_ids: Dict[Union[int, str], int],
):
    vertex_from = ctx.graph.get_vertex_by_id(vertex_ids[start_node_id])
    vertex_to = ctx.graph.get_vertex_by_id(vertex_ids[end_node_id])
    edge = ctx.graph.create_edge(vertex_from, vertex_to, mgp.EdgeType(type))
    edge_properties = edge.properties

    for key, value in properties.items():
        edge_properties[key] = convert_from_isoformat(value)


@mgp.write_proc
def cypher(ctx: mgp.ProcCtx, path: str) -> mgp.Record():
    """
    Procedure to import the Cypher created by the export_util.json procedure.
    The lab import feature should be prefered.

    Parameters
    ----------
    path : str
        Path to the JSON file that is being imported.
    """

    memgraph = gqlalchemy.Memgraph()
    try:
        with open(path, "r") as file:
            for query in file.readlines():
                stripped_query = query.strip()
                if stripped_query:
                    memgraph.execute(stripped_query)
    except OSError:
        raise OSError("Could not open/read file.")
    except Exception:
        raise Exception("Unable to execute the given queries")

    return mgp.Record()


@mgp.write_proc
def json(ctx: mgp.ProcCtx, path: str) -> mgp.Record():
    """
    Procedure to import the JSON created by the export_util.json procedure.

    Parameters
    ----------
    path : str
        Path to the JSON file that is being imported.
    """
    try:
        with open(path, "r") as file:
            graph_objects = js.load(file)
    except Exception:
        raise OSError("Could not open/read file.")

    vertex_ids = dict()

    for object in graph_objects:
        if all(
            key in object
            for key in (
                Parameter.TYPE.value,
                Parameter.PROPERTIES.value,
                Parameter.ID.value,
            )
        ):
            type_value = object[Parameter.TYPE.value]
            properties_value = object[Parameter.PROPERTIES.value]
            id_value = object[Parameter.ID.value]
        else:
            raise KeyError(
                "Each graph object needs to have 'type', \
                 'properties' and 'id' keys."
            )

        if type_value == Parameter.NODE.value:
            if Parameter.LABELS.value in object:
                labels_value = object[Parameter.LABELS.value]
            else:
                raise KeyError("Each node object needs to have 'labels' key.")

            vertex_ids[id_value] = create_vertex(ctx, properties_value, labels_value)

        elif type_value == Parameter.RELATIONSHIP.value:
            if all(
                key in object
                for key in (
                    Parameter.START.value,
                    Parameter.END.value,
                    Parameter.LABEL.value,
                )
            ):
                start_node_id = object[Parameter.START.value]
                end_node_id = object[Parameter.END.value]
                edge_type = object[Parameter.LABEL.value]
            else:
                raise KeyError(
                    "Each relationship object needs to have 'start', \
                     'end' and 'label' keys."
                )

            create_edge(
                ctx,
                properties_value,
                start_node_id,
                end_node_id,
                edge_type,
                vertex_ids,
            )
        else:
            raise KeyError("The provided file does not match the correct JSON format.")

    return mgp.Record()


def find_node(ctx: mgp.ProcCtx, label: str, prop_key: str, prop_value: str) -> Union[int, None]:
    for vertex in ctx.graph.vertices:
        if (
            label in [label.name for label in vertex.labels]
            and prop_key in vertex.properties.keys()
            and str(convert_to_isoformat_graphML(vertex.properties.get(prop_key))) == prop_value
        ):
            return vertex.id
    return None


def cast_element(text: str, type: str) -> Union[List[Any], str, int, bool, float]:
    if text == "":
        return ""
    if type == "string":
        return str(text)
    if type == "int" or type == "long":
        return int(text)
    if type == "boolean":
        return bool(text)
    if type == "float" or type == "double":
        return float(text)
    if type == "":
        return text


def cast(text: str, type: str, is_list: bool) -> Union[List[Any], str, int, bool, float]:
    if is_list:
        casted_list = list()
        for element in ast.literal_eval(text):
            casted_list.append(cast_element(element, type))
        return casted_list
    return cast_element(text, type)


def set_default_keys(key_dict: Dict[str, Any], properties: Dict[str, Any], is_for: str):
    for key_object in key_dict.values():
        if key_object.default_value != "" and key_object.is_for == is_for:
            properties.update(
                {
                    key_object.name: cast(
                        key_object.default_value,
                        key_object.type,
                        key_object.type_is_list,
                    )
                }
            )


def set_default_config(config: mgp.Map) -> mgp.Map:
    if config is None:
        config = dict()
    if not config.get("readLabels"):
        config.update({"readLabels": False})
    if not config.get("defaultRelationshipType"):
        config.update({"defaultRelationshipType": "RELATED"})
    if not config.get("storeNodeIds"):
        config.update({"storeNodeIds": False})
    if not config.get("source"):
        config.update({"source": {}})
    if not config.get("target"):
        config.update({"target": {}})
    if (
        not isinstance(config.get("readLabels"), bool)
        or not isinstance(config.get("defaultRelationshipType"), str)
        or not isinstance(config.get("storeNodeIds"), bool)
        or not isinstance(config.get("source"), dict)
        or not isinstance(config.get("target"), dict)
        or (config.get("source") and "label" not in config.get("source").keys())
        or (config.get("target") and "label" not in config.get("target").keys())
    ):
        raise TypeError(
            "Config parameter must be a map with specific \
             keys and values described in documentation."
        )
    return config


@mgp.write_proc
def graphml(  # noqa: C901
    ctx: mgp.ProcCtx,
    path: str = "",
    config: Union[mgp.Map, None] = None,
) -> mgp.Record(status=str):
    """
    Procedure to export the whole database to a graphML file.

    Parameters
    ----------
    path : str
        Path to the graphML file containing the exported graph database.
    config : Map

    """

    config = set_default_config(config)

    try:
        tree = ET.parse(path)
    except Exception:
        raise OSError("Could not open/read file.")

    root = tree.getroot()
    graphml_ns = root.tag.split("}")[0].strip("{")
    namespace = {"graphml": graphml_ns}

    keys = dict()

    for key in root.findall(".//graphml:key", namespace):
        working_key = KeyObjectGraphML(key.attrib["attr.name"], key.attrib["for"])
        if "attr.list" in key.attrib.keys():
            working_key.type_is_list = True
            working_key.type = key.attrib["attr.list"]
        elif "attr.type" in key.attrib.keys():
            working_key.type = key.attrib["attr.type"]
        child = key.findall(".//graphml:default", namespace)
        if child:
            working_key.default_value = child[0].text
        working_key.id = key.attrib["id"]
        keys.update({key.attrib["id"]: working_key})

    real_ids = dict()

    for node in root.findall(".//graphml:node", namespace):
        labels = []
        properties = dict()
        if config.get("readLabels"):
            labels = node.attrib["labels"].split(":")
            labels.pop(0)
        if config.get("storeNodeIds"):
            properties.update({"id": node.attrib["id"]})

        set_default_keys(keys, properties, "node")

        for data in node.findall("graphml:data", namespace):
            working_key = keys.get(data.attrib["key"])
            if key is None:
                working_key = KeyObjectGraphML(data.attrib["key"], "node", "string")
            if config.get("readLabels") and working_key.name == "labels":
                new_labels = data.text.split(":")
                new_labels.pop(0)
                if new_labels != labels:
                    labels = labels + new_labels
            else:
                properties.update(
                    {
                        working_key.name: cast(
                            data.text,
                            working_key.type,
                            working_key.type_is_list,
                        )
                    }
                )

        real_ids.update({node.attrib["id"]: create_vertex(ctx, properties, labels)})

    for rel in root.findall(".//graphml:edge", namespace):
        if "label" in rel.attrib.keys():
            rel_type = rel.attrib["label"]
        else:
            rel_type = config.get("defaultRelationshipType")

        properties = dict()
        set_default_keys(keys, properties, "edge")

        for data in rel.findall("graphml:data", namespace):
            working_key = keys.get(data.attrib["key"])
            if working_key is None:
                working_key = KeyObjectGraphML(data.attrib["key"], "edge", "string")
            if not working_key.name == "label":  # Tinkerpop???
                properties.update(
                    {
                        working_key.name: cast(
                            data.text,
                            working_key.type,
                            working_key.type_is_list,
                        )
                    }
                )

        if rel.attrib["source"] not in real_ids.keys():
            if not config.get("source"):
                # without source/target config, we try with the internal id
                real_ids.update({rel.attrib["source"]: int(rel.attrib["source"])})
            else:
                source_config = config.get("source")
                if "id" not in source_config.keys():
                    source_config.update({"id": "id"})
                node_id = find_node(
                    ctx,
                    source_config["label"],
                    source_config["id"],
                    rel.attrib["source"],
                )
                real_ids.update({rel.attrib["source"]: node_id})

        if rel.attrib["target"] not in real_ids.keys():
            if not config.get("target"):
                # without source/target config, we look for the internal id
                real_ids.update({rel.attrib["target"]: int(rel.attrib["target"])})
            else:
                target_config = config.get("target")
                if "id" not in target_config.keys():
                    target_config.update({"id": "id"})
                node_id = find_node(
                    ctx,
                    target_config["label"],
                    target_config["id"],
                    rel.attrib["target"],
                )
                real_ids.update({rel.attrib["target"]: node_id})

        create_edge(
            ctx,
            properties,
            rel.attrib["source"],
            rel.attrib["target"],
            rel_type,
            real_ids,
        )

    return mgp.Record(status="success")
