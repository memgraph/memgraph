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

import pytest
from common import connect, execute_and_fetch_all


def test_mgps1():
    cursor = connect().cursor()
    result = list(
        execute_and_fetch_all(
            cursor, f"CALL mgps.components() YIELD edition, name, versions RETURN edition, name, versions;"
        )[0]
    )
    assert (result) == ["community", "Memgraph", ["5.9.0"]]


# Python void procedure: mgps.validate


def test_mgps_validate_py_false():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL mgps.validate(false, 'should not throw', []);")


def test_mgps_validate_py_true():
    cursor = connect().cursor()
    with pytest.raises(Exception, match="validation failed"):
        execute_and_fetch_all(cursor, "CALL mgps.validate(true, 'validation failed: %s', ['test']);")


# C++ void procedure: mgps_cpp.validate


def test_mgps_validate_cpp_false():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL mgps_cpp.validate(false, 'should not throw', []);")


def test_mgps_validate_cpp_true():
    cursor = connect().cursor()
    with pytest.raises(Exception, match="cpp error"):
        execute_and_fetch_all(cursor, "CALL mgps_cpp.validate(true, 'cpp error: %s', ['test']);")


# Neo4j alias: apoc.util.validate -> mgps_cpp.validate


def test_mgps_validate_apoc_alias_false():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL apoc.util.validate(false, 'should not throw', []);")


def test_mgps_validate_apoc_alias_true():
    cursor = connect().cursor()
    with pytest.raises(Exception, match="apoc error"):
        execute_and_fetch_all(cursor, "CALL apoc.util.validate(true, 'apoc error: %s', ['test']);")


# YIELD on void procedure should fail


def test_mgps_validate_py_yield_asterisk_fails():
    cursor = connect().cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "CALL mgps.validate(false, 'msg', []) YIELD * RETURN *;")


def test_mgps_validate_py_yield_field_fails():
    cursor = connect().cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(
            cursor, "WITH false AS predicate CALL mgps.validate(predicate, 'message %d', [42]) YIELD n RETURN n;"
        )


def test_mgps_validate_cpp_yield_asterisk_fails():
    cursor = connect().cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(cursor, "CALL mgps_cpp.validate(false, 'msg', []) YIELD * RETURN *;")


def test_mgps_validate_cpp_yield_field_fails():
    cursor = connect().cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(
            cursor, "WITH false AS predicate CALL mgps_cpp.validate(predicate, 'message %d', [42]) YIELD n RETURN n;"
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
