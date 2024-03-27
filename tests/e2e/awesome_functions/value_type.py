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
from common import get_bytes, memgraph

TYPE = "type"


def test_value_type_on_null_type(memgraph):
    result = next(memgraph.execute_and_fetch("RETURN valuetype(null) AS type"))[TYPE]
    assert result == "NULL"


def test_value_type_on_bool_type(memgraph):
    result = next(memgraph.execute_and_fetch("RETURN valuetype(true) AS type"))[TYPE]
    assert result == "BOOLEAN"


def test_value_type_on_int_type(memgraph):
    result = next(memgraph.execute_and_fetch("RETURN valuetype(1) AS type"))[TYPE]
    assert result == "INTEGER"


def test_value_type_on_float_type(memgraph):
    result = next(memgraph.execute_and_fetch("RETURN valuetype(1.1) AS type"))[TYPE]
    assert result == "FLOAT"


def test_value_type_on_string_type(memgraph):
    result = next(memgraph.execute_and_fetch("RETURN valuetype('string_type') AS type"))[TYPE]
    assert result == "STRING"


def test_value_type_on_list_type(memgraph):
    result = next(memgraph.execute_and_fetch("RETURN valuetype([1, 2, 3]) AS type"))[TYPE]
    assert result == "LIST"


def test_value_type_on_map_type(memgraph):
    result = next(memgraph.execute_and_fetch("RETURN valuetype({key: 'value'}) AS type"))[TYPE]
    assert result == "MAP"


def test_value_type_on_date_type(memgraph):
    result = next(memgraph.execute_and_fetch("RETURN valuetype(date('2023-01-01')) AS type"))[TYPE]
    assert result == "DATE"


def test_value_type_on_local_time_type(memgraph):
    result = next(memgraph.execute_and_fetch("RETURN valuetype(localtime('23:00:00')) AS type"))[TYPE]
    assert result == "LOCAL_TIME"


def test_value_type_on_local_date_time_type(memgraph):
    result = next(memgraph.execute_and_fetch("RETURN valuetype(localdatetime('2022-01-01T23:00:00')) AS type"))[TYPE]
    assert result == "LOCAL_DATE_TIME"


def test_value_type_on_duration_type(memgraph):
    result = next(memgraph.execute_and_fetch("RETURN valuetype(duration('P5DT2M2.33S')) AS type"))[TYPE]
    assert result == "DURATION"


def test_value_type_on_node_type(memgraph):
    result = next(memgraph.execute_and_fetch("CREATE (n) WITH n RETURN valuetype(n) AS type"))[TYPE]
    assert result == "NODE"


def test_value_type_on_relationship_type(memgraph):
    result = next(memgraph.execute_and_fetch("CREATE (n)-[r:TYPE]->(m) WITH r RETURN valuetype(r) AS type"))[TYPE]
    assert result == "RELATIONSHIP"


def test_value_type_on_path_type(memgraph):
    result = next(
        memgraph.execute_and_fetch(
            "CREATE (n)-[r:TYPE]->(m) WITH 1 AS x MATCH p=(n)-[r]->(m) RETURN valuetype(p) AS type"
        )
    )[TYPE]
    assert result == "PATH"


def test_value_type_on_graph_type(memgraph):
    result = next(memgraph.execute_and_fetch("MATCH p=(n)-[r]->(m) WITH project(p) AS g RETURN valuetype(g) AS type"))[
        TYPE
    ]
    assert result == "GRAPH"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
