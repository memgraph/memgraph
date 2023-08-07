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
import typing

import mgclient
import pytest
from common import execute_and_fetch_all, has_n_result_row


@pytest.fixture(scope="function")
def multi_db(request, connection):
    cursor = connection.cursor()
    if request.param:
        execute_and_fetch_all(cursor, "CREATE DATABASE clean")
        execute_and_fetch_all(cursor, "USE DATABASE clean")
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    pass
    yield connection


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
@pytest.mark.parametrize("function_type", ["py", "c"])
def test_return_argument(multi_db, function_type):
    cursor = multi_db.cursor()
    execute_and_fetch_all(cursor, "CREATE (n:Label {id: 1});")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 1)
    result = execute_and_fetch_all(
        cursor,
        f"MATCH (n) RETURN {function_type}_read.return_function_argument(n) AS argument;",
    )
    vertex = result[0][0]
    assert isinstance(vertex, mgclient.Node)
    assert vertex.labels == set(["Label"])
    assert vertex.properties == {"id": 1}


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
@pytest.mark.parametrize("function_type", ["py", "c"])
def test_return_optional_argument(multi_db, function_type):
    cursor = multi_db.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    result = execute_and_fetch_all(
        cursor,
        f"RETURN {function_type}_read.return_optional_argument(42) AS argument;",
    )
    result = result[0][0]
    assert isinstance(result, int)
    assert result == 42


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
@pytest.mark.parametrize("function_type", ["py", "c"])
def test_return_optional_argument_no_arg(multi_db, function_type):
    cursor = multi_db.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    result = execute_and_fetch_all(
        cursor,
        f"RETURN {function_type}_read.return_optional_argument() AS argument;",
    )
    result = result[0][0]
    assert isinstance(result, int)
    assert result == 42


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
@pytest.mark.parametrize("function_type", ["py", "c"])
def test_add_two_numbers(multi_db, function_type):
    cursor = multi_db.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    result = execute_and_fetch_all(
        cursor,
        f"RETURN {function_type}_read.add_two_numbers(1, 5) AS total;",
    )
    result_sum = result[0][0]
    assert isinstance(result_sum, (float, int))
    assert result_sum == 6


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
@pytest.mark.parametrize("function_type", ["py", "c"])
def test_return_null(multi_db, function_type):
    cursor = multi_db.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    result = execute_and_fetch_all(
        cursor,
        f"RETURN {function_type}_read.return_null() AS null;",
    )
    result_null = result[0][0]
    assert result_null is None


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
@pytest.mark.parametrize("function_type", ["py", "c"])
def test_too_many_arguments(multi_db, function_type):
    cursor = multi_db.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    # Should raise too many arguments
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(
            cursor,
            f"RETURN {function_type}_read.return_null('parameter') AS null;",
        )


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
@pytest.mark.parametrize("function_type", ["py", "c"])
def test_try_to_write(multi_db, function_type):
    cursor = multi_db.cursor()
    execute_and_fetch_all(cursor, "CREATE (n:Label {id: 1});")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 1)
    # Should raise non mutable
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(
            cursor,
            f"MATCH (n) RETURN {function_type}_write.try_to_write(n, 'property', 1);",
        )


@pytest.mark.parametrize("multi_db", [False, True], indirect=True)
@pytest.mark.parametrize("function_type", ["py", "c"])
def test_case_sensitivity(multi_db, function_type):
    cursor = multi_db.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    # Should raise function does not exist
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(
            cursor,
            f"RETURN {function_type}_read.ReTuRn_nUlL('parameter') AS null;",
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
