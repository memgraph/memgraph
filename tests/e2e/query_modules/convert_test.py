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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
