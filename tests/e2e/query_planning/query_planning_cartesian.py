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
from common import memgraph

QUERY_PLAN = "QUERY PLAN"


def test_indexed_join_with_indices(memgraph):
    memgraph.execute("CREATE INDEX ON :Node(id);")

    expected_explain = [
        f" * Produce {{a, b, r}}",
        f" * Filter (a :Node), {{a.id}}",
        f" * Expand (b)-[r:EDGE]-(a)",
        f" * ScanAllByLabelPropertyValue (b :Node {{id}})",
        f" * Once",
    ]

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN MATCH (a:Node {id: 1}) MATCH (b:Node {id: 2}) MATCH (a)-[r:EDGE]-(b) return a,b,r;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain


def test_indexed_join_with_indices_and_filter(memgraph):
    memgraph.execute("CREATE INDEX ON :Node;")
    memgraph.execute("CREATE INDEX ON :Node(id);")

    expected_explain = [
        f" * Produce {{n1, n2}}",
        f" * Filter Generic {{n1, n2}}",
        f" * IndexedJoin",
        f" |\\ ",
        f" | * ScanAllByLabelPropertyValue (n1 :Node {{id}})",
        f" | * Once",
        f" * ScanAllByLabel (n2 :Node)",
        f" * Once",
    ]

    results = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n1:Node), (n2:Node) where n1.id = n2.id and n1 <> n2 return *;")
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain


def test_indexed_join_with_indices_split(memgraph):
    memgraph.execute("CREATE INDEX ON :Label1(prop1);")

    expected_explain = [
        " * Produce {a0, n0, n1, n2, n3, n4, n5, r0, r1, r2}",
        " * Expand (n5)-[r2]->(n4)",
        " * Filter (n5 :Label0:Label1)",
        " * ScanAll (n5)",
        " * Unwind",
        " * EdgeUniquenessFilter {r0 : r1}",
        " * IndexedJoin",
        " |\\ ",
        " | * Expand (n0)<-[r0]-(n1)",
        " | * ScanAllByLabelPropertyRange (n0 :Label1 {prop1})",
        " | * Once",
        " * Filter (n2 :Label1)",
        " * Expand (n3)-[r1]->(n2)",
        " * Filter (n3 :Label0)",
        " * ScanAll (n3)",
        " * Once",
    ]

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN MATCH (n0 :Label1)<-[r0]-(n1), (n2 :Label1)<-[r1]-(n3 :Label0) UNWIND [1] AS a0 MATCH (n4)<-[r2]-(n5 :Label0 :Label1) WHERE (((n2.prop0) > (n0.prop1)))  RETURN *"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
