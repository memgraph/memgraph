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
        f" * Filter (b :Node), {{b.id}}",
        f" * Expand (a)-[r:EDGE]-(b)",
        f" * ScanAllByLabelProperties (a :Node {{id}})",
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
        f" | * ScanAllByLabelProperties (n2 :Node {{id}})",
        f" | * Once",
        f" * ScanAllByLabel (n1 :Node)",
        f" * Once",
    ]

    results = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n1:Node), (n2:Node) where n1.id = n2.id and n1 <> n2 return *;")
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain


def test_indexed_join_with_indices_split(memgraph):
    memgraph.execute("CREATE INDEX ON :Label0;")
    memgraph.execute("CREATE INDEX ON :Label1;")
    memgraph.execute("CREATE INDEX ON :Label1(prop0);")
    memgraph.execute("CREATE INDEX ON :Label1(prop1);")

    expected_explain = [
        " * Produce {a0, n0, n1, n2, n3, n4, n5, r0, r1, r2}",
        " * Filter (n5 :Label0:Label1)",
        " * Expand (n4)<-[r2]-(n5)",
        " * ScanAll (n4)",
        " * Unwind",
        " * EdgeUniquenessFilter {r1 : r0}",
        " * IndexedJoin",
        " |\\ ",
        " | * Filter (n3 :Label0)",
        " | * Expand (n2)<-[r1]-(n3)",
        " | * ScanAllByLabelProperties (n2 :Label1 {prop0})",
        " | * Once",
        " * Expand (n0)<-[r0]-(n1)",
        " * ScanAllByLabel (n0 :Label1)",
        " * Once",
    ]

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN MATCH (n0 :Label1)<-[r0]-(n1), (n2 :Label1)<-[r1]-(n3 :Label0) UNWIND [1] AS a0 MATCH (n4)<-[r2]-(n5 :Label0 :Label1) WHERE (((n2.prop0) > (n0.prop1)))  RETURN *"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]
    assert expected_explain == actual_explain


def test_cartesian_with_nested_property_join(memgraph):
    expected_explain = [
        " * Produce {n, m}",
        " * HashJoin {n : m}",
        " |\\ ",
        " | * Filter (m :label1)",
        " | * ScanAll (m)",
        " | * Once",
        " * Filter (n :label)",
        " * ScanAll (n)",
        " * Once",
    ]

    results = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:label), (m:label1) WHERE m.prop1.id = n.prop1.id RETURN n, m;")
    )
    actual_explain = [x[QUERY_PLAN] for x in results]
    assert expected_explain == actual_explain


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
