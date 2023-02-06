import inspect
import pytest

import mgp
import mgp_mock

from typing import Dict


def get_class_members(cls):
    object_names = dir(type("dummy", (object,), {}))
    return [item for item in inspect.getmembers(cls) if item[0] not in object_names]


def get_function_signature(member) -> str:
    raw_signature = inspect.signature(member)
    signature = str(raw_signature)

    if "NewType.<locals>.new_type" not in signature:
        return signature

    for parameter in raw_signature.parameters.values():
        if "NewType.<locals>.new_type" in str(parameter.annotation):
            signature = signature.replace(str(parameter.annotation), parameter.annotation.__name__)

    return signature


def normalize_type(raw_type, type_name) -> str:
    if "NewType.<locals>.new_type" in type_name:
        return raw_type.__name__
    elif type_name.startswith("<class '"):
        return type_name.split("'")[1]
    else:
        return type_name


def get_property_type(member) -> str:
    fget = list(filter(lambda member: member[0] == "fget", inspect.getmembers(member)))[0][1]
    raw_property_type = inspect.signature(fget).return_annotation
    property_type_name = str(raw_property_type)

    return normalize_type(raw_property_type, property_type_name)


def get_member_type(member_descriptor) -> str:
    return str(member_descriptor.__objclass__).split("'")[1]


def get_class_signature(cls) -> str:
    signature: Dict[str, Dict[str, str]] = {"attributes": {}, "methods": {}, "properties": {}}
    members = get_class_members(cls)
    for name, member in members:
        type_name = type(member).__name__
        if type_name == "function":
            signature["methods"][name] = get_function_signature(member)
        elif type_name == "property":
            signature["properties"][name] = get_property_type(member)
        else:
            signature["attributes"][name] = get_member_type(member) if type_name == "member_descriptor" else type_name

    return str(signature)


@pytest.mark.parametrize(
    "mock_api_class,api_class",
    [
        (mgp_mock.Vertex, mgp.Vertex),
        (mgp_mock.Edge, mgp.Edge),
        (mgp_mock.Path, mgp.Path),
        (mgp_mock.Graph, mgp.Graph),
        (mgp_mock.Properties, mgp.Properties),
        (mgp_mock.Label, mgp.Label),
        (mgp_mock.EdgeType, mgp.EdgeType),
        (mgp_mock.Record, mgp.Record),
        (mgp_mock.Message, mgp.Message),
        (mgp_mock.Messages, mgp.Messages),
        (mgp_mock.ProcCtx, mgp.ProcCtx),
        (mgp_mock.TransCtx, mgp.TransCtx),
        (mgp_mock.FuncCtx, mgp.FuncCtx),
    ],
)
def test_class_signatures_match(mock_api_class, api_class):
    assert get_class_signature(mock_api_class).replace("mgp_mock", "mgp") == get_class_signature(api_class)
