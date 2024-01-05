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

import mgclient
import pytest
from common import memgraph


def test_label_index_hint(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2 {id: i}));")
    memgraph.execute("FOREACH (i IN range(1, 10) | CREATE (n:Label2 {id: i+50}));")
    memgraph.execute("CREATE INDEX ON :Label1;")
    memgraph.execute("CREATE INDEX ON :Label2;")

    expected_explain_no_hint = [
        " * Produce {n}",
        " * Filter (n :Label1:Label2)",
        " * ScanAllByLabel (n :Label1)",
        " * Once",
    ]
    expected_explain_with_hint = [row.replace("(n :Label1)", "(n :Label2)") for row in expected_explain_no_hint]

    explain_no_hint = [
        row["QUERY PLAN"] for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:Label1:Label2) RETURN n;")
    ]
    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label2 MATCH (n:Label1:Label2) RETURN n;")
    ]

    assert explain_no_hint == expected_explain_no_hint and explain_with_hint == expected_explain_with_hint


def test_label_index_hint_alternative_orderings(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2:Label3 {id: i}));")
    memgraph.execute("FOREACH (i IN range(1, 10) | CREATE (n:Label2 {id: i+50}));")
    memgraph.execute("CREATE INDEX ON :Label1;")
    memgraph.execute("CREATE INDEX ON :Label2;")

    expected_explain_with_hint = [
        " * Produce {n}",
        " * Filter (n :Label1:Label2)",
        " * ScanAllByLabel (n :Label2)",
        " * Once",
    ]
    expected_explain_with_hint_ordering_3 = expected_explain_with_hint[:]
    expected_explain_with_hint_ordering_3[1] = " * Filter (n :Label1:Label2:Label3)"  # since it matches 3 labels

    explain_with_hint_ordering_1 = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label2 MATCH (n:Label1:Label2) RETURN n;")
    ]
    explain_with_hint_ordering_2 = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label2 MATCH (n:Label2:Label1) RETURN n;")
    ]
    explain_with_hint_ordering_3 = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label2 MATCH (n:Label3:Label2:Label1) RETURN n;")
    ]

    assert (
        expected_explain_with_hint == explain_with_hint_ordering_1 == explain_with_hint_ordering_2
        and expected_explain_with_hint_ordering_3 == explain_with_hint_ordering_3
    )


def test_multiple_label_index_hints(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2 {id: i}));")
    memgraph.execute("FOREACH (i IN range(1, 10) | CREATE (n:Label2 {id: i+50}));")
    memgraph.execute("CREATE INDEX ON :Label0;")
    memgraph.execute("CREATE INDEX ON :Label2;")

    expected_explain_with_hint = [
        " * Produce {n}",
        " * Filter (n :Label1:Label2)",
        " * ScanAllByLabel (n :Label2)",
        " * Once",
    ]

    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label0, :Label2 MATCH (n:Label1:Label2) RETURN n;")
    ]

    assert explain_with_hint == expected_explain_with_hint


def test_multiple_applicable_label_index_hints(memgraph):
    # Out of all applicable index hints, the first one given in the query should be used
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2:Label3 {id: i}));")
    memgraph.execute("CREATE INDEX ON :Label1;")
    memgraph.execute("CREATE INDEX ON :Label2;")
    memgraph.execute("CREATE INDEX ON :Label3;")

    expected_explain_with_hint = [
        " * Produce {n}",
        " * Filter (n :Label2:Label3)",
        " * ScanAllByLabel (n :Label3)",
        " * Once",
    ]

    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label3, :Label2 MATCH (n:Label2:Label3) RETURN n;")
    ]

    assert explain_with_hint == expected_explain_with_hint


def test_multiple_applicable_label_index_hints_alternative_orderings(memgraph):
    # Out of all applicable index hints, the first one given in the query should be used
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2:Label3 {id: i}));")
    memgraph.execute("CREATE INDEX ON :Label1;")
    memgraph.execute("CREATE INDEX ON :Label2;")
    memgraph.execute("CREATE INDEX ON :Label3;")

    expected_explain_with_hint_1 = [
        " * Produce {n}",
        " * Filter (n :Label2:Label3)",
        " * ScanAllByLabel (n :Label3)",
        " * Once",
    ]
    expected_explain_with_hint_2 = [row.replace("(n :Label3)", "(n :Label2)") for row in expected_explain_with_hint_1]

    explain_with_hint_ordering_1a = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label3, :Label2 MATCH (n:Label2:Label3) RETURN n;")
    ]
    explain_with_hint_ordering_1b = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label3, :Label2 MATCH (n:Label3:Label2) RETURN n;")
    ]
    explain_with_hint_ordering_2a = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label2, :Label3 MATCH (n:Label2:Label3) RETURN n;")
    ]
    explain_with_hint_ordering_2b = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label2, :Label3 MATCH (n:Label3:Label2) RETURN n;")
    ]
    assert (expected_explain_with_hint_1 == explain_with_hint_ordering_1a == explain_with_hint_ordering_1b) and (
        expected_explain_with_hint_2 == explain_with_hint_ordering_2a == explain_with_hint_ordering_2b
    )


def test_label_property_index_hint(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i % 5}));")
    memgraph.execute("CREATE INDEX ON :Label(id1);")
    memgraph.execute("CREATE INDEX ON :Label(id2);")

    expected_explain_no_hint = [
        " * Produce {n}",
        " * Filter {n.id1}",
        " * ScanAllByLabelPropertyValue (n :Label {id2})",
        " * Once",
    ]
    expected_explain_with_hint = [
        row.replace("(n :Label {id2})", "(n :Label {id1})").replace(" * Filter {n.id1}", " * Filter {n.id2}")
        for row in expected_explain_no_hint
    ]

    explain_no_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;")
    ]
    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label(id1) MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;"
        )
    ]

    assert explain_no_hint == expected_explain_no_hint and explain_with_hint == expected_explain_with_hint


def test_label_property_index_hint_alternative_orderings(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i % 5}));")
    memgraph.execute("CREATE INDEX ON :Label(id1);")
    memgraph.execute("CREATE INDEX ON :Label(id2);")

    expected_explain_with_hint = [
        " * Produce {n}",
        " * Filter {n.id2}",
        " * ScanAllByLabelPropertyValue (n :Label {id1})",
        " * Once",
    ]

    explain_with_hint_ordering_1 = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label(id1) MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;"
        )
    ]
    explain_with_hint_ordering_2 = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label(id1) MATCH (n:Label) WHERE n.id1 = 3 AND n.id2 = 3 RETURN n;"
        )
    ]

    assert expected_explain_with_hint == explain_with_hint_ordering_1 == explain_with_hint_ordering_2


def test_multiple_label_property_index_hints(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i % 5}));")
    memgraph.execute("CREATE INDEX ON :Label(id0);")
    memgraph.execute("CREATE INDEX ON :Label(id1);")

    expected_explain_with_hint = [
        " * Produce {n}",
        " * Filter {n.id2}",
        " * ScanAllByLabelPropertyValue (n :Label {id1})",
        " * Once",
    ]

    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label(id0), :Label(id1) MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;"
        )
    ]
    explain_with_hint_alternative_ordering = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label(id0), :Label(id1) MATCH (n:Label) WHERE n.id1 = 3 AND n.id2 = 3 RETURN n;"
        )
    ]

    assert explain_with_hint == expected_explain_with_hint == explain_with_hint_alternative_ordering


def test_multiple_applicable_label_property_index_hints(memgraph):
    # Out of all applicable index hints, the first one given in the query should be used
    memgraph.execute("FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i % 5}));")
    memgraph.execute("CREATE INDEX ON :Label(id1);")
    memgraph.execute("CREATE INDEX ON :Label(id2);")

    expected_explain_with_hint = [
        " * Produce {n}",
        " * Filter {n.id2}",
        " * ScanAllByLabelPropertyValue (n :Label {id1})",
        " * Once",
    ]

    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label(id1), :Label(id2) MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;"
        )
    ]

    assert explain_with_hint == expected_explain_with_hint


def test_multiple_applicable_label_property_index_hints_alternative_orderings(memgraph):
    # Out of all applicable index hints, the first one given in the query should be used
    memgraph.execute("FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i % 5}));")
    memgraph.execute("CREATE INDEX ON :Label(id1);")
    memgraph.execute("CREATE INDEX ON :Label(id2);")

    expected_explain_with_hint_1 = [
        " * Produce {n}",
        " * Filter {n.id2}",
        " * ScanAllByLabelPropertyValue (n :Label {id1})",
        " * Once",
    ]
    expected_explain_with_hint_2 = [
        row.replace("(n :Label {id1})", "(n :Label {id2})").replace(" * Filter {n.id2}", " * Filter {n.id1}")
        for row in expected_explain_with_hint_1
    ]

    explain_with_hint_ordering_1a = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label(id1), :Label(id2) MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;"
        )
    ]
    explain_with_hint_ordering_1b = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label(id1), :Label(id2) MATCH (n:Label) WHERE n.id1 = 3 AND n.id2 = 3 RETURN n;"
        )
    ]
    explain_with_hint_ordering_2a = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label(id2), :Label(id1) MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;"
        )
    ]
    explain_with_hint_ordering_2b = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label(id2), :Label(id1) MATCH (n:Label) WHERE n.id1 = 3 AND n.id2 = 3 RETURN n;"
        )
    ]

    assert (expected_explain_with_hint_1 == explain_with_hint_ordering_1a == explain_with_hint_ordering_1b) and (
        expected_explain_with_hint_2 == explain_with_hint_ordering_2a == explain_with_hint_ordering_2b
    )


def test_union_applicable_in_left_branch(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2 {id: i}));")
    memgraph.execute("FOREACH (i IN range(1, 10) | CREATE (n:Label2 {id: i+50}));")
    memgraph.execute("CREATE INDEX ON :Label1;")
    memgraph.execute("CREATE INDEX ON :Label2;")

    expected_explain_with_hint = [
        " * Distinct",
        " * Union {n : n}",
        " |\\ ",
        " | * Produce {n}",
        " | * ScanAllByLabel (n :Label2)",
        " | * Once",
        " * Produce {n}",
        " * ScanAllByLabel (n :Label1)",
        " * Once",
    ]

    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label1 MATCH (n:Label1) RETURN n UNION MATCH (n:Label2) RETURN n;"
        )
    ]

    assert explain_with_hint == expected_explain_with_hint


def test_union_applicable_in_right_branch(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2 {id: i}));")
    memgraph.execute("FOREACH (i IN range(1, 10) | CREATE (n:Label2 {id: i+50}));")
    memgraph.execute("CREATE INDEX ON :Label1;")
    memgraph.execute("CREATE INDEX ON :Label2;")

    expected_explain_with_hint = [
        " * Distinct",
        " * Union {n : n}",
        " |\\ ",
        " | * Produce {n}",
        " | * ScanAllByLabel (n :Label1)",
        " | * Once",
        " * Produce {n}",
        " * ScanAllByLabel (n :Label2)",
        " * Once",
    ]

    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label1 MATCH (n:Label2) RETURN n UNION MATCH (n:Label1) RETURN n;"
        )
    ]

    assert explain_with_hint == expected_explain_with_hint


def test_union_applicable_in_both_branches(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2:Label3 {id: i}));")
    memgraph.execute("FOREACH (i IN range(1, 10) | CREATE (n:Label1:Label2 {id: i+50}));")
    memgraph.execute("CREATE INDEX ON :Label1;")
    memgraph.execute("CREATE INDEX ON :Label2;")
    memgraph.execute("CREATE INDEX ON :Label3;")

    expected_explain_with_hint = [
        " * Distinct",
        " * Union {n : n}",
        " |\\ ",
        " | * Produce {n}",
        " | * Filter (n :Label2:Label3)",
        " | * ScanAllByLabel (n :Label2)",
        " | * Once",
        " * Produce {n}",
        " * Filter (n :Label1:Label2)",
        " * ScanAllByLabel (n :Label1)",
        " * Once",
    ]

    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label1, :Label2 MATCH (n:Label1:Label2) RETURN n UNION MATCH (n:Label2:Label3) RETURN n;"
        )
    ]

    assert explain_with_hint == expected_explain_with_hint


def test_multiple_match_query(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2:Label3 {id: i}));")
    memgraph.execute("FOREACH (i IN range(1, 10) | CREATE (n:Label1:Label2 {id: i+50}));")
    memgraph.execute("CREATE INDEX ON :Label1;")
    memgraph.execute("CREATE INDEX ON :Label2;")
    memgraph.execute("CREATE INDEX ON :Label3;")

    # TODO: Fix this test since it has the filtering info wrong (filtering by label that's already indexed)
    expected_explain_with_hint = [
        " * Produce {n, m}",
        " * Cartesian {m : n}",
        " |\\ ",
        " | * Filter (n :Label1:Label2), {n.id}",
        " | * ScanAllByLabel (n :Label1)",
        " | * Once",
        " * Filter (m :Label2:Label3)",
        " * ScanAllByLabel (m :Label2)",
        " * Once",
    ]

    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label1, :Label2  MATCH (n:Label1:Label2) WHERE n.id = 1 MATCH (m:Label2:Label3) return n, m;"
        )
    ]

    assert explain_with_hint == expected_explain_with_hint


def test_nonexistent_label_index(memgraph):
    # In case of hinting at a nonexistent index, the query should execute without exceptions, and its output should be
    # the same as without that hint

    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2 {id: i}));")
    memgraph.execute("FOREACH (i IN range(1, 10) | CREATE (n:Label2 {id: i+50}));")
    memgraph.execute("CREATE INDEX ON :Label1;")

    try:
        explain_no_hint = [
            row["QUERY PLAN"] for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:Label1:Label2) RETURN n;")
        ]

        explain_with_hint = [
            row["QUERY PLAN"]
            for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label2 MATCH (n:Label1:Label2) RETURN n;")
        ]

        assert explain_with_hint == explain_no_hint
    except mgclient.DatabaseError:
        assert False


def test_nonexistent_label_property_index(memgraph):
    # In case of hinting at a nonexistent index, the query should execute without exceptions, and its output should be
    # the same as without that hint

    memgraph.execute("FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i % 5}));")
    memgraph.execute("CREATE INDEX ON :Label(id2);")

    try:
        explain_no_hint = [
            row["QUERY PLAN"] for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:Label1:Label2) RETURN n;")
        ]

        explain_with_hint = [
            row["QUERY PLAN"]
            for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label(id1) MATCH (n:Label1:Label2) RETURN n;")
        ]

        assert explain_with_hint == explain_no_hint
    except mgclient.DatabaseError:
        assert False


def test_index_hint_on_expand(memgraph):
    # Prefer expanding from the node with the given hint even if estimator estimates higher cost for that plan

    memgraph.execute("FOREACH (i IN range(1, 1000) | CREATE (n:Label1 {id: i}));")
    memgraph.execute("FOREACH (i IN range(1, 10) | CREATE (n:Label2 {id: i}));")
    memgraph.execute("CREATE INDEX ON :Label1;")
    memgraph.execute("CREATE INDEX ON :Label2;")

    expected_explain_without_hint = [
        " * Produce {n, m}",
        " * Filter (n :Label1)",
        " * Expand (m)<-[anon1:rel]-(n)",
        " * ScanAllByLabel (m :Label2)",
        " * Once",
    ]

    expected_explain_with_hint = [
        " * Produce {n, m}",
        " * Filter (m :Label2)",
        " * Expand (n)-[anon1:rel]->(m)",
        " * ScanAllByLabel (n :Label1)",
        " * Once",
    ]

    explain_without_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:Label1)-[:rel]->(m:Label2) RETURN n, m;")
    ]

    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label1 MATCH (n:Label1)-[:rel]->(m:Label2) RETURN n, m;"
        )
    ]

    assert explain_without_hint == expected_explain_without_hint and explain_with_hint == expected_explain_with_hint


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
