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


def test_label_index_hint(memgraph):
    memgraph.execute("CREATE (n:Label1:Label2 {prop: 1});")
    memgraph.execute("CREATE INDEX ON :Label1;")

    # TODO: Fix this test since it should only filter on :Label2 and prop
    expected_explain = [
        " * Produce {n}",
        " * Filter (n :Label1:Label2), {n.prop}",
        " * ScanAllByLabel (n :Label1)",
        " * Once",
    ]

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:Label1:Label2) WHERE n.prop = 1 return n;")
    ]

    assert expected_explain == actual_explain


def test_range_w_index(memgraph):
    memgraph.execute("CREATE INDEX ON :l(p);")
    memgraph.execute("UNWIND RANGE(1,20) as i CREATE (:l{p:i});")

    # Single Range

    expected_explain = [
        " * Produce {n}",
        " * ScanAllByLabelPropertyRange (n :l {p})",
        " * Once",
    ]

    actual_explain = [
        row["QUERY PLAN"] for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 10 >= n.p > 3 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"] for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 3 < n.p <= 10 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 3 < n.p AND n.p <= 10 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE n.p > 3 AND n.p <= 10 RETURN n;")
    ]

    assert expected_explain == actual_explain

    expected_result = [4, 5, 6, 7, 8, 9, 10]

    actual_result = [row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 10 >= n.p > 3 RETURN n.p;")]

    assert actual_result == actual_result

    actual_result = [row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 3 < n.p <= 10 RETURN n.p;")]

    assert actual_result == actual_result

    actual_result = [
        row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 3 < n.p AND n.p <= 10 RETURN n.p;")
    ]

    assert actual_result == actual_result

    actual_result = [
        row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE n.p > 3 AND n.p <= 10 RETURN n.p;")
    ]

    assert actual_result == actual_result

    # Range AND compare

    expected_explain = [
        " * Produce {n}",
        " * Filter {n.p}",
        " * ScanAllByLabelPropertyRange (n :l {p})",
        " * Once",
    ]

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 10 >= n.p > 3 AND n.p >= 6 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 3 < n.p <= 10 AND n.p >= 6 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 10 >= n.p  AND n.p > 3 AND n.p >= 6 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 6 <= n.p AND n.p <= 10 AND 3 < n.p RETURN n;")
    ]

    assert expected_explain == actual_explain

    expected_result = [6, 7, 8, 9, 10]

    actual_result = [
        row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 10 >= n.p > 3 AND n.p >= 6 RETURN n.p;")
    ]

    assert actual_result == expected_result

    actual_result = [
        row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 3 < n.p <= 10 AND n.p >= 6 RETURN n.p;")
    ]

    assert actual_result == expected_result

    actual_result = [
        row["n.p"]
        for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 10 >= n.p  AND n.p > 3 AND n.p >= 6 RETURN n.p;")
    ]

    assert actual_result == expected_result

    actual_result = [
        row["n.p"]
        for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 6 <= n.p AND n.p <= 10 AND 3 < n.p RETURN n.p;")
    ]

    assert actual_result == expected_result

    # Range AND Range

    expected_explain = [
        " * Produce {n}",
        " * Filter {n.p}",
        " * ScanAllByLabelPropertyRange (n :l {p})",
        " * Once",
    ]

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 10 >= n.p > 3 AND 8 > n.p > 6 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 3 < n.p < 10 AND 9 < n.p > 6 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN MATCH (n:l) WHERE 10 >= n.p  AND n.p > 3 AND n.p >= 6 AND 8 > n.p RETURN n;"
        )
    ]

    assert expected_explain == actual_explain

    actual_result = [
        row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 10 >= n.p > 3 AND 8 > n.p > 6 RETURN n.p;")
    ]

    assert actual_result == [7]

    actual_result = [
        row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 3 < n.p < 10 AND 9 < n.p > 6 RETURN n.p;")
    ]

    assert actual_result == []

    actual_result = [
        row["n.p"]
        for row in memgraph.execute_and_fetch(
            "MATCH (n:l) WHERE 10 >= n.p  AND n.p > 3 AND n.p >= 6 AND 8 > n.p RETURN n.p;"
        )
    ]

    assert actual_result == [6, 7]


def test_range_wo_index(memgraph):
    memgraph.execute("UNWIND RANGE(1,20) as i CREATE (:l{p:i});")

    # Single Range

    expected_explain = [
        " * Produce {n}",
        " * Filter (n :l), {n.p}",
        " * ScanAll (n)",
        " * Once",
    ]

    actual_explain = [
        row["QUERY PLAN"] for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 10 >= n.p > 3 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"] for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 3 < n.p <= 10 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 3 < n.p AND n.p <= 10 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE n.p > 3 AND n.p <= 10 RETURN n;")
    ]

    assert expected_explain == actual_explain

    expected_result = [4, 5, 6, 7, 8, 9, 10]

    actual_result = [row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 10 >= n.p > 3 RETURN n.p;")]

    assert actual_result == actual_result

    actual_result = [row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 3 < n.p <= 10 RETURN n.p;")]

    assert actual_result == actual_result

    actual_result = [
        row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 3 < n.p AND n.p <= 10 RETURN n.p;")
    ]

    assert actual_result == actual_result

    actual_result = [
        row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE n.p > 3 AND n.p <= 10 RETURN n.p;")
    ]

    assert actual_result == actual_result

    # Range AND compare

    expected_explain = [
        " * Produce {n}",
        " * Filter (n :l), {n.p}",
        " * ScanAll (n)",
        " * Once",
    ]

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 10 >= n.p > 3 AND n.p >= 6 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 3 < n.p <= 10 AND n.p >= 6 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 10 >= n.p  AND n.p > 3 AND n.p >= 6 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 6 <= n.p AND n.p <= 10 AND 3 < n.p RETURN n;")
    ]

    assert expected_explain == actual_explain

    expected_result = [6, 7, 8, 9, 10]

    actual_result = [
        row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 10 >= n.p > 3 AND n.p >= 6 RETURN n.p;")
    ]

    assert actual_result == expected_result

    actual_result = [
        row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 3 < n.p <= 10 AND n.p >= 6 RETURN n.p;")
    ]

    assert actual_result == expected_result

    actual_result = [
        row["n.p"]
        for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 10 >= n.p  AND n.p > 3 AND n.p >= 6 RETURN n.p;")
    ]

    assert actual_result == expected_result

    actual_result = [
        row["n.p"]
        for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 6 <= n.p AND n.p <= 10 AND 3 < n.p RETURN n.p;")
    ]

    assert actual_result == expected_result

    # Range AND Range

    expected_explain = [
        " * Produce {n}",
        " * Filter (n :l), {n.p}",
        " * ScanAll (n)",
        " * Once",
    ]

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 10 >= n.p > 3 AND 8 > n.p > 6 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:l) WHERE 3 < n.p < 10 AND 9 < n.p > 6 RETURN n;")
    ]

    assert expected_explain == actual_explain

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN MATCH (n:l) WHERE 10 >= n.p  AND n.p > 3 AND n.p >= 6 AND 8 > n.p RETURN n;"
        )
    ]

    assert expected_explain == actual_explain

    actual_result = [
        row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 10 >= n.p > 3 AND 8 > n.p > 6 RETURN n.p;")
    ]

    assert actual_result == [7]

    actual_result = [
        row["n.p"] for row in memgraph.execute_and_fetch("MATCH (n:l) WHERE 3 < n.p < 10 AND 9 < n.p > 6 RETURN n.p;")
    ]

    assert actual_result == []

    actual_result = [
        row["n.p"]
        for row in memgraph.execute_and_fetch(
            "MATCH (n:l) WHERE 10 >= n.p  AND n.p > 3 AND n.p >= 6 AND 8 > n.p RETURN n.p;"
        )
    ]

    assert actual_result == [6, 7]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
