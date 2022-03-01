# Copyright 2022 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import typing
import mgclient
import sys
import pytest
from common import execute_and_fetch_all, has_n_result_row


@pytest.mark.parametrize("function_type", ["py", "c"])
def test_return_argument(connection, function_type):
    cursor = connection.cursor()
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


@pytest.mark.parametrize("function_type", ["py", "c"])
def test_add_two_numbers(connection, function_type):
    cursor = connection.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    result = execute_and_fetch_all(
        cursor,
        f"RETURN {function_type}_read.add_two_numbers(1, 5) AS total;",
    )
    result_sum = result[0][0]
    assert isinstance(result_sum, (float, int))
    assert result_sum == 6


@pytest.mark.parametrize("function_type", ["py", "c"])
def test_return_null(connection, function_type):
    cursor = connection.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    result = execute_and_fetch_all(
        cursor,
        f"RETURN {function_type}_read.return_null() AS null;",
    )
    result_null = result[0][0]
    assert result_null is None


@pytest.mark.parametrize("function_type", ["py", "c"])
def test_too_many_arguments(connection, function_type):
    cursor = connection.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    # Should raise too many arguments
    with pytest.raises(Exception) as e_info:
        execute_and_fetch_all(
            cursor,
            f"RETURN {function_type}_read.return_null('parameter') AS null;",
        )


@pytest.mark.parametrize("function_type", ["py", "c"])
def test_try_to_write(connection, function_type):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "CREATE (n:Label {id: 1});")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 1)
    # Should raise non mutable
    with pytest.raises(Exception) as e_info:
        execute_and_fetch_all(
            cursor,
            f"MATCH (n) RETURN {function_type}_write.try_to_write(n, 'property', 1);",
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
