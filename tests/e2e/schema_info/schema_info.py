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

import json
import sys
from functools import partial

import pytest
from common import connect, execute_and_fetch_all
from mg_utils import mg_assert_until, mg_sleep_and_assert


def test_spec(connect):
    memgraph = connect.cursor()

    # Setup
    memgraph.execute("CREATE ENUM Status VALUES { Good, Okay, Bad };")
    memgraph.execute(
        """CREATE
                        (a:Person {name:'John', age:30}),
                        (b:Person :Child {name:'Nick'}),
                        (c:Person {name:'Helen', age:29, occupation:'student'}),
                        (d:Person :Student {name:'Bob', interests: ['programming', 'math']}),
                        (e:School {title: 'School 1', status: Status::Good, location: point({x:1, y:2, z:3, crs:'wgs-84-3d'})}),
                        (f:Node {embedding: [1.0, 2.0]}),
                        (a)-[:IS_FAMILY {since:2015}]->(b),
                        (a)-[:IS_FAMILY {since: 2010}]->(c),
                        (b)-[:IS_FAMILY {since:2015}]->(c),
                        (c)-[:IS_FAMILY {since:2011}]->(d),
                        (a)-[:IS_FAMILY]->(d),
                        (b)-[:IS_STUDENT {start: 2020}]->(e);"""
    )

    memgraph.execute("CREATE INDEX ON :Student;")
    memgraph.execute("CREATE INDEX ON :Person(name);")
    memgraph.execute("CREATE POINT INDEX ON :School(location);")
    memgraph.execute("CREATE TEXT INDEX personTextIndex ON :Person;")
    memgraph.execute(
        "CREATE VECTOR INDEX test_index ON :Node(embedding) WITH CONFIG {'dimension': 2, 'capacity': 100};"
    )
    memgraph.execute("CREATE EDGE INDEX ON :IS_STUDENT;")
    memgraph.execute("CREATE EDGE INDEX ON :IS_FAMILY(since);")
    memgraph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    memgraph.execute("CREATE CONSTRAINT ON (n:School) ASSERT n.title IS UNIQUE;")
    memgraph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT n.age IS TYPED INTEGER;")

    # Show schema info
    memgraph.execute("SHOW SCHEMA INFO;")
    schema = memgraph.fetchall()
    schema_json = json.loads(schema[0][0])

    # Check JSON
    nodes = schema_json["nodes"]
    assert len(nodes) == 5  # Tested via unit tests
    edges = schema_json["edges"]
    assert len(edges) == 5  # Tested via unit tests
    node_constraints = schema_json["node_constraints"]
    assert len(node_constraints) == 3
    for constraint in node_constraints:
        if constraint["type"] == "unique":
            assert constraint["labels"] == ["School"]
            assert constraint["properties"] == ["title"]
        elif constraint["type"] == "existence":
            assert constraint["labels"] == ["Person"]
            assert constraint["properties"] == ["name"]
        else:
            assert constraint["type"] == "data_type"
            assert constraint["labels"] == ["Person"]
            assert constraint["properties"] == ["age"]
            assert constraint["data_type"] == "INTEGER"
    node_indexes = schema_json["node_indexes"]
    assert len(node_indexes) == 5
    for index in node_indexes:
        if index["labels"] == ["Student"]:
            assert index["properties"] == []
            assert index["count"] == 1
        elif index["labels"] == ["School"]:
            assert index["type"] == "label+property_point"
            assert index["properties"] == ["location"]
            assert index["count"] == 1
        elif index["labels"] == ["Node"]:
            assert index["type"] == "label+property_vector"
            assert index["properties"] == ["embedding"]
            assert index["count"] == 1
        else:
            assert index["labels"] == ["Person"]
            if index.get("type", {}) == "label_text":
                assert index["properties"] == []
                assert index["count"] == -1  # TODO Set to 4 once text index has an approximate count
            else:
                assert index["properties"] == ["name"]
                assert index["count"] == 4
    edge_indexes = schema_json["edge_indexes"]
    assert len(edge_indexes) == 2
    for index in edge_indexes:
        if index["edge_type"] == ["IS_STUDENT"]:
            assert index["properties"] == []
            assert index["count"] == 1
        else:
            assert index["edge_type"] == ["IS_FAMILY"]
            assert index["properties"] == ["since"]
            assert index["count"] == 4
    enums = schema_json["enums"]
    assert len(enums) == 1
    assert enums[0]["name"] == "Status"
    assert enums[0]["values"] == ["Good", "Okay", "Bad"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
