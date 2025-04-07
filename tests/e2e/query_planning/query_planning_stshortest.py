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


def test_single_source_shortest_executes_when_having_high_amount_of_destination_nodes(memgraph):
    memgraph.execute("CREATE INDEX ON :Node;")
    memgraph.execute("CREATE INDEX ON :Node(id);")

    memgraph.execute("UNWIND range(1, 20) as x CREATE (:Node {id: x})-[:TO]->(:Node {id: 100 + x})")

    expected_explain = [
        f" * Produce {{p}}",
        f" * ConstructNamedPath",
        f" * Filter (b :Node)",
        f" * BFSExpand (a)-[anon1:TYPE]->(b)",
        f" * ScanAllByLabelPropertyValue (a :Node {{id}})",
        f" * Once",
    ]

    results = list(memgraph.execute_and_fetch("EXPLAIN MATCH p=(a:Node {id: 1})-[:TYPE *bfs]->(b:Node) RETURN p"))
    print(results)
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain


def test_stshortest_executes_when_having_low_amount_of_destination_nodes(memgraph):
    memgraph.execute("CREATE INDEX ON :Node;")
    memgraph.execute("CREATE INDEX ON :Node(id);")

    memgraph.execute("UNWIND range(1, 20) as x CREATE (:Node {id: x})-[:TO]->(:Node {id: 100 + x})")

    expected_explain = [
        f" * Produce {{p}}",
        f" * ConstructNamedPath",
        f" * STShortestPath (a)-[anon1:TYPE]->(b)",
        f" * ScanAllByLabelPropertyValue (b :Node {{id}})",
        f" * ScanAllByLabelPropertyValue (a :Node {{id}})",
        f" * Once",
    ]

    results = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH p=(a:Node {id: 1})-[:TYPE *bfs]->(b:Node {id: 2}) RETURN p")
    )
    print(results)
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
