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
from mgclient import DatabaseError, Node, Relationship


def test_fetching_headers(connection):
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)

    result = list(
        execute_and_fetch_all(
            cursor, f"CALL execution_module.execute_fetch_headers('RETURN 1 as x, 2 as y') YIELD output RETURN output"
        )
    )

    assert len(result) == 1

    headers_list = [x for x in result[0][0]]

    assert headers_list == ["x", "y"]


def test_fetch_count(connection):
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    execute_and_fetch_all(cursor, "FOREACH (i in range(1, 10) | CREATE ())")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 10)

    result = list(
        execute_and_fetch_all(
            cursor, f"CALL execution_module.execute_fetch_result_count('MATCH (n) RETURN n') YIELD output RETURN output"
        )
    )

    assert len(result) == 1

    result_count = result[0][0]

    assert result_count == 10


def test_fetch_results_with_result_count(connection):
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    execute_and_fetch_all(cursor, "FOREACH (i in range(1, 10) | CREATE ())")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 10)

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL execution_module.execute_fetch_results('MATCH (n) RETURN count(n) as cnt') YIELD output RETURN output",
        )
    )

    assert len(result) == 1

    result_count = result[0][0]["cnt"]

    assert result_count == 10


def test_fetch_results_primitives(connection):
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    execute_and_fetch_all(cursor, "FOREACH (i in range(1, 10) | CREATE ())")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 10)

    string_value = "'hehe'"
    result = list(
        execute_and_fetch_all(
            cursor,
            f'CALL execution_module.execute_fetch_results("RETURN 1 as w, true as x, {string_value} as y, 1.01 as z") YIELD output RETURN output',
        )
    )

    assert len(result) == 1

    values = result[0][0]
    assert "w" in values and "x" in values and "y" in values and "z" in values
    assert values["w"] == 1
    assert values["x"] == True
    assert values["y"] == "hehe"
    assert values["z"] == 1.01


def test_fetch_results_nodes_rels(connection):
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    execute_and_fetch_all(cursor, "FOREACH (i in range(1, 10) | CREATE ()-[:TYPE]->())")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 20)

    result = list(
        execute_and_fetch_all(
            cursor,
            f'CALL execution_module.execute_fetch_results("MATCH (n)-[r]->(m) RETURN n, r, m") YIELD output RETURN output',
        )
    )

    assert len(result) == 10

    values = result[0]
    for result_map in values:
        assert "n" in result_map and "r" in result_map and "m" in result_map
        assert isinstance(result_map["n"], Node)
        assert isinstance(result_map["r"], Relationship)
        assert isinstance(result_map["m"], Node)


def test_fetch_results_empty_count(connection):
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL execution_module.execute_fetch_results('FOREACH (i in range(1, 10) | CREATE ())') YIELD output RETURN output",
        )
    )

    assert len(result) == 0


def test_fetch_results_with_result_count_on_multitenancy(connection):
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n;")
    execute_and_fetch_all(cursor, "FOREACH (i in range(1, 10) | CREATE ());")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n;", 10)

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL execution_module.execute_fetch_results('MATCH (n) RETURN count(n) as cnt') YIELD output RETURN output;",
        )
    )

    assert len(result) == 1

    result_count = result[0][0]["cnt"]

    assert result_count == 10

    execute_and_fetch_all(cursor, f"CREATE DATABASE mg2;")
    execute_and_fetch_all(cursor, f"USE DATABASE mg2;")

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL execution_module.execute_fetch_results('MATCH (n) RETURN count(n) as cnt') YIELD output RETURN output;",
        )
    )

    assert len(result) == 1

    result_count = result[0][0]["cnt"]

    assert result_count == 0

    execute_and_fetch_all(cursor, f"USE DATABASE memgraph;")
    execute_and_fetch_all(cursor, f"DROP DATABASE mg2;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
