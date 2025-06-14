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
from common import memgraph_reset_plan_cache

QUERY_PLAN = "QUERY PLAN"


def test_reset_plan_cache_to_replan_inefficient_plan(memgraph_reset_plan_cache):
    memgraph_reset_plan_cache.execute("CREATE INDEX ON :Supernode")
    memgraph_reset_plan_cache.execute("CREATE INDEX ON :Supernode(id)")
    memgraph_reset_plan_cache.execute("CREATE INDEX ON :Node")
    memgraph_reset_plan_cache.execute("CREATE INDEX ON :Node(id)")

    memgraph_reset_plan_cache.execute("CREATE (:Supernode {id: 1});")
    memgraph_reset_plan_cache.execute(
        "UNWIND range(1, 10) as x MATCH (n:Supernode {id: 1}) CREATE (n)-[:HAS_REL_TO]->(:Node {id: x});"
    )

    expected_explain = [
        f" * Produce {{count(*)}}",
        f" * Aggregate {{COUNT-1}} {{}}",
        f" * Filter (m :Node), {{m.id}}",
        f" * Expand (n)-[anon1:HAS_REL_TO]->(m)",
        f" * ScanAllByLabelProperties (n :Supernode {{id}})",
        f" * Once",
    ]

    results = list(
        memgraph_reset_plan_cache.execute_and_fetch(
            "EXPLAIN MATCH (n:Supernode {id: 1})-[:HAS_REL_TO]->(m:Node {id: 1}) RETURN count(*);"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain

    memgraph_reset_plan_cache.execute(
        "UNWIND range(1, 1000) as x CREATE (n:Supernode {id: 1})-[:HAS_REL_TO]->(:Node {id: x});"
    )
    memgraph_reset_plan_cache.execute("RESET PLAN CACHE")

    expected_explain = [
        f" * Produce {{count(*)}}",
        f" * Aggregate {{COUNT-1}} {{}}",
        f" * Filter (n :Supernode), {{n.id}}",
        f" * Expand (m)<-[anon1:HAS_REL_TO]-(n)",
        f" * ScanAllByLabelProperties (m :Node {{id}})",
        f" * Once",
    ]

    results = list(
        memgraph_reset_plan_cache.execute_and_fetch(
            "EXPLAIN MATCH (n:Supernode {id: 1})-[:HAS_REL_TO]->(m:Node {id: 1}) RETURN count(*);"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
