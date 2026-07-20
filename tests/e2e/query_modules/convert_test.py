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

import sys

import mgclient
import pytest
from common import connect, execute_and_fetch_all


def test_convert_list1():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, f"RETURN convert.str2object('[2, 4, 8, [2]]') AS result;")[0][0]
    assert (result) == [2, 4, 8, [2]]


def test_convert_list_wrong():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, f"RETURN convert.str2object('[2, 4, 8, [2]]') AS result;")[0][0]
    assert (result) != [3, 4, 8, [2]]


def test_convert_map():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        'RETURN convert.str2object(\'{"test": "true", "test2": true, "test3": 1, "test4": 1.5, "test5": null, "test6": {"test7": true, "test8": 1, "test9": 1.5, "test10": null}, "test11": ["string", 1, 1.5, null, {"test12": "string", "test13": 1, "test14": 1.5, "test15": null}]}\');',
    )[0][0]
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
    result = execute_and_fetch_all(
        cursor,
        'RETURN convert.str2object("null");',
    )[
        0
    ][0]
    assert result == None


def test_convert_int():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        'RETURN convert.str2object("1");',
    )[
        0
    ][0]
    assert result == 1


def test_convert_double():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        'RETURN convert.str2object("1.5");',
    )[
        0
    ][0]
    assert result == 1.5


def test_convert_bool():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        'RETURN convert.str2object("true");',
    )[
        0
    ][0]
    assert result == True


def test_convert_string():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        'RETURN convert.str2object("\\"Cool string\\"");',
    )[
        0
    ][0]
    assert result == "Cool string"


def test_convert_invalid():
    cursor = connect().cursor()
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(
            cursor,
            'RETURN convert.str2object("this is not a string since it is not escaped");',
        )


def test_from_json_map_basic():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, 'RETURN convert.from_json_map(\'{"name": "GDS"}\') AS result;')[0][0]
    assert result == {"name": "GDS"}


def test_from_json_map_nested():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        'RETURN convert.from_json_map(\'{"a": 1, "b": [1, 2, {"c": true}], "d": null, "e": 1.5}\') AS result;',
    )[0][0]
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
    result = execute_and_fetch_all(
        cursor,
        'RETURN convert.from_json_map(\'{"a": 1, "b": {"c": 2, "d": [10, 20]}}\', \'$.b\') AS result;',
    )[0][0]
    assert result == {"c": 2, "d": [10, 20]}


def test_from_json_map_path_bracket():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        'RETURN convert.from_json_map(\'{"a": 1, "b": {"c": 2}}\', "$[\'b\']") AS result;',
    )[0][0]
    assert result == {"c": 2}


def test_from_json_map_path_array_index():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        'RETURN convert.from_json_map(\'{"e": [{"x": 1}, {"x": 2}]}\', \'$.e[0]\') AS result;',
    )[0][0]
    assert result == {"x": 1}


def test_from_json_map_array_root_with_path():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN convert.from_json_map('[{\"x\": 1}]', '$[0]') AS result;")[0][0]
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
    result = execute_and_fetch_all(cursor, "RETURN convert.from_json_map('{\"a\": null}', '$.a') AS result;")[0][0]
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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
