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

# isort: off
import sys
import pytest

from common import execute_and_fetch_all, has_n_result_row, has_one_result_row
from conftest import get_connection
from mgclient import DatabaseError


@pytest.mark.parametrize(
    "is_write",
    [
        True,
        False,
    ],
)
def test_graph_mutability(is_write: bool, connection):
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)

    module = "write" if is_write else "read"

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL batch_py_{module}.graph_is_mutable() " "YIELD mutable, init_called RETURN mutable, init_called",
        )
    )
    assert result == [(is_write, True)]

    execute_and_fetch_all(cursor, "CREATE ()")
    result = list(
        execute_and_fetch_all(
            cursor,
            "MATCH (n) "
            f"CALL batch_py_{module}.underlying_graph_is_mutable(n) "
            "YIELD mutable, init_called RETURN mutable, init_called",
        )
    )
    assert result == [(is_write, True)]

    execute_and_fetch_all(cursor, "CREATE ()-[:TYPE]->()")
    result = list(
        execute_and_fetch_all(
            cursor,
            "MATCH (n)-[e]->(m) "
            f"CALL batch_py_{module}.underlying_graph_is_mutable(e) "
            "YIELD mutable, init_called RETURN mutable, init_called",
        )
    )
    assert result == [(is_write, True)]


def test_batching_nums(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL batch_py_read.batch_nums() " "YIELD num, init_called, is_valid RETURN num, init_called, is_valid",
        )
    )
    assert result == [(i, True, True) for i in range(1, 11)]

    execute_and_fetch_all(cursor, "CREATE () CREATE ()")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 2)
    result = list(
        execute_and_fetch_all(
            cursor,
            "MATCH (n) "
            "CALL batch_py_read.batch_nums() "
            "YIELD num, init_called, is_valid RETURN num, init_called, is_valid ",
        )
    )
    assert result == [(i, True, True) for i in range(1, 11)] * 2


def test_batching_vertices(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)

    execute_and_fetch_all(cursor, f"CREATE () CREATE ()")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 2)

    with pytest.raises(DatabaseError):
        result = list(
            execute_and_fetch_all(
                cursor, f"CALL batch_py_read.batch_vertices() " "YIELD vertex, init_called RETURN vertex, init_called"
            )
        )


def test_batching_nums_c(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)

    num_ints = 10
    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL batch_c_read.batch_nums({num_ints}) " "YIELD output RETURN output",
        )
    )
    result_list = [item[0] for item in result]
    print(result_list)
    print([i for i in range(1, num_ints + 1)])
    assert result_list == [i for i in range(1, num_ints + 1)]


def test_batching_strings_c(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)

    num_strings = 10
    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL batch_c_read.batch_strings({num_strings}) " "YIELD output RETURN output",
        )
    )
    assert len(result) == num_strings


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
