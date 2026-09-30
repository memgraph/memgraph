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


def test_mgps_components():
    cursor = connect().cursor()
    results = execute_and_fetch_all(
        cursor, f"CALL mgps.components() YIELD edition, name, versions RETURN edition, name, versions;"
    )
    rows = [list(r) for r in results]
    assert ["community", "Memgraph", ["5.9.0"]] in rows
    assert ["community", "Neo4j Kernel", ["5.9.0"]] in rows


def test_mgps_validate_py_false():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL mgps.validate(false, 'should not throw', []);")


def test_mgps_validate_py_true():
    cursor = connect().cursor()
    with pytest.raises(Exception, match="validation failed"):
        execute_and_fetch_all(cursor, "CALL mgps.validate(true, 'validation failed: %s', ['test']);")


def test_mgps_validate_cpp_false():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL mgps_cpp.validate(false, 'should not throw', []);")


def test_mgps_validate_cpp_true():
    cursor = connect().cursor()
    with pytest.raises(Exception, match="cpp error"):
        execute_and_fetch_all(cursor, "CALL mgps_cpp.validate(true, 'cpp error: %s', ['test']);")


def test_mgps_validate_apoc_alias_false():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL apoc.util.validate(false, 'should not throw', []);")


def test_mgps_validate_apoc_alias_true():
    cursor = connect().cursor()
    with pytest.raises(Exception, match="apoc error"):
        execute_and_fetch_all(cursor, "CALL apoc.util.validate(true, 'apoc error: %s', ['test']);")


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


def test_mgps_version():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN mgps.version();")
    assert list(result[0]) == ["5.9.0"]


def test_apoc_version_mapping():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN apoc.version();")
    assert list(result[0]) == ["5.9.0"]


def test_mgps_await_indexes():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CALL mgps.await_indexes(1);")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
