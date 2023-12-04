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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
