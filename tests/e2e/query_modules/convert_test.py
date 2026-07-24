# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import json
import sys

import mgclient
import pytest
from common import connect, execute_and_fetch_all


def test_convert_list1():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN convert.str2object('[2, 4, 8, [2]]') AS result;")[0][0]
    assert (result) == [2, 4, 8, [2]]


def test_convert_map():
    cursor = connect().cursor()
    query = """RETURN convert.str2object('{"test": "true", "test2": true, "test3": 1, "test4": 1.5, "test5": null, "test6": {"test7": true, "test8": 1, "test9": 1.5, "test10": null}, "test11": ["string", 1, 1.5, null, {"test12": "string", "test13": 1, "test14": 1.5, "test15": null}]}');"""
    result = execute_and_fetch_all(cursor, query)[0][0]
    assert result == {
        "test": "true",
        "test11": ["string", 1, 1.5, None, {"test12": "string", "test13": 1, "test14": 1.5, "test15": None}],
        "test2": True,
        "test3": 1,
        "test4": 1.5,
        "test5": None,
        "test6": {"test10": None, "test7": True, "test8": 1, "test9": 1.5},
    }


def test_convert_null():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, 'RETURN convert.str2object("null");')[0][0]
    assert result == None


def test_convert_int():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, 'RETURN convert.str2object("1");')[0][0]
    assert result == 1


def test_convert_double():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, 'RETURN convert.str2object("1.5");')[0][0]
    assert result == 1.5


def test_convert_bool():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, 'RETURN convert.str2object("true");')[0][0]
    assert result == True


def test_convert_string():
    cursor = connect().cursor()
    query = r"""RETURN convert.str2object("\"Cool string\"");"""
    result = execute_and_fetch_all(cursor, query)[0][0]
    assert result == "Cool string"


def test_convert_invalid():
    cursor = connect().cursor()
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, 'RETURN convert.str2object("this is not a string since it is not escaped");')


def test_from_json_map_basic():
    cursor = connect().cursor()
    query = """RETURN convert.from_json_map('{"name": "GDS"}') AS result;"""
    result = execute_and_fetch_all(cursor, query)[0][0]
    assert result == {"name": "GDS"}


def test_from_json_map_nested():
    cursor = connect().cursor()
    query = """RETURN convert.from_json_map('{"a": 1, "b": [1, 2, {"c": true}], "d": null, "e": 1.5}') AS result;"""
    result = execute_and_fetch_all(cursor, query)[0][0]
    assert result == {"a": 1, "b": [1, 2, {"c": True}], "d": None, "e": 1.5}


def test_from_json_map_null():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN convert.from_json_map(null) AS result;")[0][0]
    assert result is None


def test_from_json_map_array_fails():
    cursor = connect().cursor()
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "RETURN convert.from_json_map('[1, 2, 3]') AS result;")


def test_from_json_map_scalar_fails():
    cursor = connect().cursor()
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "RETURN convert.from_json_map('5') AS result;")


def test_from_json_map_path_object():
    cursor = connect().cursor()
    query = """RETURN convert.from_json_map('{"a": 1, "b": {"c": 2, "d": [10, 20]}}', '$.b') AS result;"""
    result = execute_and_fetch_all(cursor, query)[0][0]
    assert result == {"c": 2, "d": [10, 20]}


def test_from_json_map_path_nested_object():
    cursor = connect().cursor()
    query = """RETURN convert.from_json_map('{"a": {"b": {"c": 1, "d": [10, 20]}}}', '$.a.b') AS result;"""
    result = execute_and_fetch_all(cursor, query)[0][0]
    assert result == {"c": 1, "d": [10, 20]}


def test_from_json_map_path_bracket():
    cursor = connect().cursor()
    query = """RETURN convert.from_json_map('{"a": 1, "b": {"c": 2}}', "$['b']") AS result;"""
    result = execute_and_fetch_all(cursor, query)[0][0]
    assert result == {"c": 2}


def test_from_json_map_path_array_index():
    cursor = connect().cursor()
    query = """RETURN convert.from_json_map('{"e": [{"x": 1}, {"x": 2}]}', '$.e[0]') AS result;"""
    result = execute_and_fetch_all(cursor, query)[0][0]
    assert result == {"x": 1}


def test_from_json_map_array_root_with_path():
    cursor = connect().cursor()
    query = """RETURN convert.from_json_map('[{"x": 1}]', '$[0]') AS result;"""
    result = execute_and_fetch_all(cursor, query)[0][0]
    assert result == {"x": 1}


def test_from_json_map_path_root_and_empty():
    cursor = connect().cursor()
    doc = '{"a": 1, "b": {"c": 2}}'
    expected = {"a": 1, "b": {"c": 2}}
    assert execute_and_fetch_all(cursor, f"RETURN convert.from_json_map('{doc}', '$') AS result;")[0][0] == expected
    assert execute_and_fetch_all(cursor, f"RETURN convert.from_json_map('{doc}', '') AS result;")[0][0] == expected
    assert execute_and_fetch_all(cursor, f"RETURN convert.from_json_map('{doc}', null) AS result;")[0][0] == expected


def test_from_json_map_path_missing_is_null():
    cursor = connect().cursor()
    doc = '{"e": [{"x": 1}]}'
    assert execute_and_fetch_all(cursor, f"RETURN convert.from_json_map('{doc}', '$.zzz') AS result;")[0][0] is None
    assert execute_and_fetch_all(cursor, f"RETURN convert.from_json_map('{doc}', '$.zzz.yyy') AS result;")[0][0] is None
    assert execute_and_fetch_all(cursor, f"RETURN convert.from_json_map('{doc}', '$.e[9]') AS result;")[0][0] is None


def test_from_json_map_path_json_null_leaf_is_null():
    cursor = connect().cursor()
    query = """RETURN convert.from_json_map('{"a": null}', '$.a') AS result;"""
    result = execute_and_fetch_all(cursor, query)[0][0]
    assert result is None


def test_from_json_map_path_non_object_fails():
    cursor = connect().cursor()
    doc = '{"b": {"c": 2, "d": [10, 20]}}'
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, f"RETURN convert.from_json_map('{doc}', '$.b.c') AS result;")
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, f"RETURN convert.from_json_map('{doc}', '$.b.d') AS result;")


def test_from_json_map_path_unsupported_fails():
    cursor = connect().cursor()
    doc = '{"e": [{"x": 1}, {"x": 2}]}'
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, f"RETURN convert.from_json_map('{doc}', '$.e[*].x') AS result;")
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, f"RETURN convert.from_json_map('{doc}', '$..x') AS result;")


def test_to_map_from_map():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN convert.to_map({a: 1, b: 'x'}) AS result;")[0][0]
    assert result == {"a": 1, "b": "x"}


def test_to_map_from_node():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor, "CREATE (n:ToMapNode {id: 4, name: 'z'}) RETURN convert.to_map(n) AS result;"
    )[0][0]
    assert result == {"id": 4, "name": "z"}


def test_to_map_from_relationship():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor, "CREATE (a)-[r:TO_MAP_REL {w: 2.5, k: 'v'}]->(b) RETURN convert.to_map(r) AS result;"
    )[0][0]
    assert result == {"w": 2.5, "k": "v"}


def test_to_map_null():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN convert.to_map(null) AS result;")[0][0]
    assert result is None


def test_to_map_list_is_null():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN convert.to_map([1, 2, 3]) AS result;")[0][0]
    assert result is None


def test_to_map_scalar_is_null():
    cursor = connect().cursor()
    assert execute_and_fetch_all(cursor, "RETURN convert.to_map('hello') AS result;")[0][0] is None
    assert execute_and_fetch_all(cursor, "RETURN convert.to_map(5) AS result;")[0][0] is None


def test_from_json_list_basic():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN convert.from_json_list('[1, 2, 3]') AS result;")[0][0]
    assert result == [1, 2, 3]


def test_from_json_list_null():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN convert.from_json_list(null) AS result;")[0][0]
    assert result is None


def test_from_json_list_object_fails():
    cursor = connect().cursor()
    query = """RETURN convert.from_json_list('{"a": 1}') AS result;"""
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, query)


def test_from_json_list_path_to_array():
    cursor = connect().cursor()
    query = """RETURN convert.from_json_list('{"a": [1, 2, 3]}', '$.a') AS result;"""
    result = execute_and_fetch_all(cursor, query)[0][0]
    assert result == [1, 2, 3]


def test_from_json_list_path_nested_and_index():
    cursor = connect().cursor()
    q1 = """RETURN convert.from_json_list('{"a": {"b": [4, 5]}}', '$.a.b') AS r;"""
    assert execute_and_fetch_all(cursor, q1)[0][0] == [4, 5]
    q2 = """RETURN convert.from_json_list('{"a": [[1, 2], [3, 4]]}', '$.a[1]') AS r;"""
    assert execute_and_fetch_all(cursor, q2)[0][0] == [3, 4]


def test_from_json_list_path_root_on_array():
    cursor = connect().cursor()
    assert execute_and_fetch_all(cursor, "RETURN convert.from_json_list('[7, 8, 9]', '$') AS r;")[0][0] == [7, 8, 9]


def test_from_json_list_path_unresolved_and_json_null_is_null():
    cursor = connect().cursor()
    q1 = """RETURN convert.from_json_list('{"a": [1]}', '$.zzz') AS r;"""
    assert execute_and_fetch_all(cursor, q1)[0][0] is None
    q2 = """RETURN convert.from_json_list('{"a": null}', '$.a') AS r;"""
    assert execute_and_fetch_all(cursor, q2)[0][0] is None


def test_from_json_list_path_non_array_fails():
    cursor = connect().cursor()
    q1 = """RETURN convert.from_json_list('{"a": {"b": 1}}', '$.a') AS r;"""
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, q1)
    q2 = """RETURN convert.from_json_list('{"a": 5}', '$.a') AS r;"""
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, q2)


def test_to_json_scalars():
    cursor = connect().cursor()
    assert execute_and_fetch_all(cursor, "RETURN convert.to_json('hi') AS r;")[0][0] == '"hi"'
    assert execute_and_fetch_all(cursor, "RETURN convert.to_json(5) AS r;")[0][0] == "5"
    assert execute_and_fetch_all(cursor, "RETURN convert.to_json(1.5) AS r;")[0][0] == "1.5"
    assert execute_and_fetch_all(cursor, "RETURN convert.to_json(true) AS r;")[0][0] == "true"
    assert execute_and_fetch_all(cursor, "RETURN convert.to_json(null) AS r;")[0][0] == "null"


def test_to_json_list():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN convert.to_json([1, 'a', true, null, [2]]) AS r;")[0][0]
    assert json.loads(result) == [1, "a", True, None, [2]]


def test_to_json_map():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN convert.to_json({a: 1, b: 'x', c: [1, 2], d: null}) AS r;")[0][0]
    assert json.loads(result) == {"a": 1, "b": "x", "c": [1, 2], "d": None}


def test_to_json_empty_map_and_list():
    cursor = connect().cursor()
    assert execute_and_fetch_all(cursor, "RETURN convert.to_json({}) AS r;")[0][0] == "{}"
    assert execute_and_fetch_all(cursor, "RETURN convert.to_json([]) AS r;")[0][0] == "[]"


def test_to_json_node():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor, "CREATE (n:Person:Human {name: 'Ana', age: 30}) RETURN convert.to_json(n) AS r;"
    )[0][0]
    parsed = json.loads(result)
    assert isinstance(parsed["id"], str)
    assert parsed["type"] == "node"
    assert sorted(parsed["labels"]) == ["Human", "Person"]
    assert parsed["properties"] == {"name": "Ana", "age": 30}


def test_to_json_node_no_properties_omits_key():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "CREATE (n:Empty) RETURN convert.to_json(n) AS r;")[0][0]
    parsed = json.loads(result)
    assert parsed["type"] == "node"
    assert parsed["labels"] == ["Empty"]
    assert "properties" not in parsed


def test_to_json_relationship():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor, "CREATE (a:A {p: 1})-[r:KNOWS {since: 2020}]->(b:B {q: 2}) RETURN convert.to_json(r) AS r;"
    )[0][0]
    parsed = json.loads(result)
    assert parsed["type"] == "relationship"
    assert parsed["label"] == "KNOWS"
    assert parsed["properties"] == {"since": 2020}
    assert parsed["start"]["type"] == "node" and parsed["start"]["labels"] == ["A"]
    assert parsed["start"]["properties"] == {"p": 1}
    assert parsed["end"]["labels"] == ["B"]
    assert isinstance(parsed["start"]["id"], str)


def test_to_json_path():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        "CREATE p=(a:A2 {x: 1})-[:R2 {w: 2}]->(b:B2 {y: 3}) RETURN convert.to_json(p) AS r;",
    )[0][0]
    parsed = json.loads(result)
    assert isinstance(parsed, list) and len(parsed) == 3
    assert parsed[0]["type"] == "node" and parsed[0]["properties"] == {"x": 1}
    assert parsed[1]["type"] == "relationship" and parsed[1]["label"] == "R2"
    assert parsed[1]["properties"] == {"w": 2}
    assert parsed[2]["type"] == "node" and parsed[2]["properties"] == {"y": 3}


def test_to_json_point_cartesian():
    cursor = connect().cursor()
    r2d = json.loads(execute_and_fetch_all(cursor, "RETURN convert.to_json(point({x: 1.0, y: 2.0})) AS r;")[0][0])
    assert r2d == {"crs": "cartesian", "x": 1.0, "y": 2.0, "z": None}
    r3d = json.loads(
        execute_and_fetch_all(cursor, "RETURN convert.to_json(point({x: 1.0, y: 2.0, z: 3.0})) AS r;")[0][0]
    )
    assert r3d == {"crs": "cartesian-3d", "x": 1.0, "y": 2.0, "z": 3.0}


def test_to_json_point_wgs84():
    cursor = connect().cursor()
    r2d = json.loads(
        execute_and_fetch_all(cursor, "RETURN convert.to_json(point({latitude: 1.0, longitude: 2.0})) AS r;")[0][0]
    )
    assert r2d == {"crs": "wgs-84", "latitude": 1.0, "longitude": 2.0, "height": None}
    r3d = json.loads(
        execute_and_fetch_all(
            cursor, "RETURN convert.to_json(point({latitude: 1.0, longitude: 2.0, height: 3.0})) AS r;"
        )[0][0]
    )
    assert r3d == {"crs": "wgs-84-3d", "latitude": 1.0, "longitude": 2.0, "height": 3.0}


def test_to_json_temporals():
    cursor = connect().cursor()
    assert execute_and_fetch_all(cursor, "RETURN convert.to_json(date('2020-01-02')) AS r;")[0][0] == '"2020-01-02"'
    assert (
        execute_and_fetch_all(cursor, "RETURN convert.to_json(localTime('03:04:05.123456')) AS r;")[0][0]
        == '"03:04:05.123456"'
    )
    assert (
        execute_and_fetch_all(cursor, "RETURN convert.to_json(localDateTime('2020-01-02T03:04:05.123456')) AS r;")[0][0]
        == '"2020-01-02T03:04:05.123456"'
    )
    assert (
        execute_and_fetch_all(cursor, "RETURN convert.to_json(duration('P1DT2H3M4.5S')) AS r;")[0][0]
        == '"P1DT2H3M4.500000S"'
    )
    assert (
        execute_and_fetch_all(cursor, "RETURN convert.to_json(datetime('2020-01-02T03:04:05+01:00')) AS r;")[0][0]
        == '"2020-01-02T03:04:05.000000+01:00"'
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
