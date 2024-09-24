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

QUERY_PLAN = "QUERY PLAN"


def test_id_planner_uppercase(memgraph):
    memgraph.execute("CREATE ()")

    expected_plan = [" * Produce {n}", " * ScanAllById (n)", " * Once"]

    actual_plan = [
        x[QUERY_PLAN] for x in list(memgraph.execute_and_fetch("EXPLAIN MATCH (n) WHERE ID(n) = 1 RETURN n"))
    ]

    assert actual_plan == expected_plan


def test_id_planner_lowercase(memgraph):
    memgraph.execute("CREATE ()")

    expected_plan = [" * Produce {n}", " * ScanAllById (n)", " * Once"]

    actual_plan = [
        x[QUERY_PLAN] for x in list(memgraph.execute_and_fetch("EXPLAIN MATCH (n) WHERE id(n) = 1 RETURN n"))
    ]

    assert actual_plan == expected_plan


def test_id_planner_pascal_case(memgraph):
    memgraph.execute("CREATE ()")

    expected_plan = [" * Produce {n}", " * ScanAllById (n)", " * Once"]

    actual_plan = [
        x[QUERY_PLAN] for x in list(memgraph.execute_and_fetch("EXPLAIN MATCH (n) WHERE Id(n) = 1 RETURN n"))
    ]

    assert actual_plan == expected_plan


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
