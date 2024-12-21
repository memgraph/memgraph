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

EXPECTED_PLAN_DISTANCE_WITH = [
    f" * Produce {{a}}",
    f" * ScanAllByPointDistance (a :Node {{point}})",
    f" * Produce {{p}}",
    f" * Once",
]

EXPECTED_PLAN_DISTANCE_NO_WITH = [
    f" * Produce {{a}}",
    f" * ScanAllByPointDistance (a :Node {{point}})",
    f" * Once",
]

EXPECTED_PLAN_WITHINBBOX_WITH = [
    f" * Produce {{a}}",
    f" * ScanAllByPointWithinbbox (a :Node {{point}})",
    f" * Produce {{lb, ub}}",
    f" * Once",
]

EXPECTED_PLAN_WITHINBBOX_NO_WITH = [
    f" * Produce {{a}}",
    f" * ScanAllByPointWithinbbox (a :Node {{point}})",
    f" * Once",
]


def test_ScanAllByPointDistance_used(memgraph):
    memgraph.execute("CREATE POINT INDEX ON :Node(point);")

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN WITH point({x:1, y:1}) as p MATCH (a:Node) WHERE point.distance(p, a.point) < 1 return a;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert EXPECTED_PLAN_DISTANCE_WITH == actual_explain

    memgraph.execute("DROP POINT INDEX ON :Node(point);")


def test_ScanAllByPointDistance_used_commutative(memgraph):
    memgraph.execute("CREATE POINT INDEX ON :Node(point);")

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN WITH point({x:1, y:1}) as p MATCH (a:Node) WHERE 1 > point.distance(p, a.point) return a;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert EXPECTED_PLAN_DISTANCE_WITH == actual_explain

    memgraph.execute("DROP POINT INDEX ON :Node(point);")


def test_ScanAllByPointDistance_no_with(memgraph):
    memgraph.execute("CREATE POINT INDEX ON :Node(point);")

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN MATCH (a:Node) WHERE point.distance(point({x:1, y:1}), a.point) < 1 return a;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert EXPECTED_PLAN_DISTANCE_NO_WITH == actual_explain

    memgraph.execute("DROP POINT INDEX ON :Node(point);")


def test_ScanAllByPointDistance_no_with_commutative(memgraph):
    memgraph.execute("CREATE POINT INDEX ON :Node(point);")

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN MATCH (a:Node) WHERE 1 > point.distance(a.point, point({x:1, y:1})) return a;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert EXPECTED_PLAN_DISTANCE_NO_WITH == actual_explain

    memgraph.execute("DROP POINT INDEX ON :Node(point);")


def test_ScanAllByPointWithinbbox_no_with_commutative(memgraph):
    memgraph.execute("CREATE POINT INDEX ON :Node(point);")

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN WITH point({x:1, y:1}) as lb, point({x:2, y:2}) as ub MATCH (a:Node) WHERE point.withinbbox(a.point, lb, ub) = true return a;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert EXPECTED_PLAN_WITHINBBOX_WITH == actual_explain

    memgraph.execute("DROP POINT INDEX ON :Node(point);")


def test_ScanAllByPointWithinbbox_used_implicit_true(memgraph):
    memgraph.execute("CREATE POINT INDEX ON :Node(point);")

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN WITH point({x:1, y:1}) as lb, point({x:2, y:2}) as ub MATCH (a:Node) WHERE point.withinbbox(a.point, lb, ub) return a;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert EXPECTED_PLAN_WITHINBBOX_WITH == actual_explain

    memgraph.execute("DROP POINT INDEX ON :Node(id);")


def test_ScanAllByPointWithinbbox_used_implicit_false(memgraph):
    memgraph.execute("CREATE POINT INDEX ON :Node(point);")

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN WITH point({x:1, y:1}) as lb, point({x:2, y:2}) as ub MATCH (a:Node) WHERE NOT point.withinbbox(a.point, lb, ub) return a;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert EXPECTED_PLAN_WITHINBBOX_WITH == actual_explain

    memgraph.execute("DROP POINT INDEX ON :Node(id);")


def test_ScanAllByPointWithinbboxNoWith1(memgraph):
    memgraph.execute("CREATE POINT INDEX ON :Node(point);")

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN MATCH (a:Node) WHERE NOT point.withinbbox(a.point, point({x:1, y:1}), point({x:2, y:2})) return a;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert EXPECTED_PLAN_WITHINBBOX_NO_WITH == actual_explain

    memgraph.execute("DROP POINT INDEX ON :Node(id);")


def test_ScanAllByPointWithinbboxNoWith2(memgraph):
    memgraph.execute("CREATE POINT INDEX ON :Node(point);")

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN MATCH (a:Node) WHERE point.withinbbox(a.point, point({x:1, y:1}), point({x:2, y:2})) return a;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert EXPECTED_PLAN_WITHINBBOX_NO_WITH == actual_explain

    memgraph.execute("DROP POINT INDEX ON :Node(id);")


def test_ScanAllByPointWithinbboxNoWith3(memgraph):
    memgraph.execute("CREATE POINT INDEX ON :Node(point);")

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN MATCH (a:Node) WHERE true = point.withinbbox(a.point, point({x:1, y:1}), point({x:2, y:2})) return a;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert EXPECTED_PLAN_WITHINBBOX_NO_WITH == actual_explain

    memgraph.execute("DROP POINT INDEX ON :Node(id);")


def test_ScanAllByPointWithinbboxNoWith4(memgraph):
    memgraph.execute("CREATE POINT INDEX ON :Node(point);")

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN MATCH (a:Node) WHERE point.withinbbox(a.point, point({x:1, y:1}), point({x:2, y:2})) = true return a;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert EXPECTED_PLAN_WITHINBBOX_NO_WITH == actual_explain

    memgraph.execute("DROP POINT INDEX ON :Node(id);")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
