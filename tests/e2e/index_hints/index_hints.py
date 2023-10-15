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
        " * Filter",
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


def test_multiple_label_index_hints(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2 {id: i}));")
    memgraph.execute("FOREACH (i IN range(1, 10) | CREATE (n:Label2 {id: i+50}));")
    memgraph.execute("CREATE INDEX ON :Label0;")
    memgraph.execute("CREATE INDEX ON :Label2;")

    expected_explain_with_hint = [
        " * Produce {n}",
        " * Filter",
        " * ScanAllByLabel (n :Label2)",
        " * Once",
    ]

    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label0, :Label2 MATCH (n:Label1:Label2) RETURN n;")
    ]

    assert explain_with_hint == expected_explain_with_hint


def test_multiple_applicable_label_index_hints(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2:Label3 {id: i}));")
    memgraph.execute("CREATE INDEX ON :Label1;")
    memgraph.execute("CREATE INDEX ON :Label2;")
    memgraph.execute("CREATE INDEX ON :Label3;")

    expected_explain_with_hint = [
        " * Produce {n}",
        " * Filter",
        " * ScanAllByLabel (n :Label3)",
        " * Once",
    ]

    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label3, :Label2 MATCH (n:Label2:Label3) RETURN n;")
    ]

    assert explain_with_hint == expected_explain_with_hint


def test_label_property_index_hint(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i % 5}));")
    memgraph.execute("CREATE INDEX ON :Label(id1);")
    memgraph.execute("CREATE INDEX ON :Label(id2);")

    expected_explain_no_hint = [
        " * Produce {n}",
        " * Filter",
        " * ScanAllByLabelPropertyValue (n :Label {id2})",
        " * Once",
    ]
    expected_explain_with_hint = [
        row.replace("(n :Label {id2})", "(n :Label {id1})") for row in expected_explain_no_hint
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


def test_multiple_label_property_index_hints(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i % 5}));")
    memgraph.execute("CREATE INDEX ON :Label(id0);")
    memgraph.execute("CREATE INDEX ON :Label(id1);")

    expected_explain_with_hint = [
        " * Produce {n}",
        " * Filter",
        " * ScanAllByLabelPropertyValue (n :Label {id1})",
        " * Once",
    ]

    explain_with_hint = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch(
            "EXPLAIN USING INDEX :Label(id0), :Label(id1) MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;"
        )
    ]

    assert explain_with_hint == expected_explain_with_hint


def test_multiple_applicable_label_property_index_hints(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i % 5}));")
    memgraph.execute("CREATE INDEX ON :Label(id1);")
    memgraph.execute("CREATE INDEX ON :Label(id2);")

    expected_explain_with_hint = [
        " * Produce {n}",
        " * Filter",
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


def test_nonexistent_label_index(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label1:Label2 {id: i}));")
    memgraph.execute("FOREACH (i IN range(1, 10) | CREATE (n:Label2 {id: i+50}));")
    memgraph.execute("CREATE INDEX ON :Label1;")

    try:
        result = [
            row for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label2 MATCH (n:Label1:Label2) RETURN n;")
        ]
    except mgclient.DatabaseError as e:
        assert str(e) == "Index for label Label2 doesn't exist"
        return

    assert False


def test_nonexistent_label_property_index(memgraph):
    memgraph.execute("FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i}));")
    memgraph.execute("FOREACH (i IN range(1, 50) | CREATE (n:Label {id2: i % 5}));")
    memgraph.execute("CREATE INDEX ON :Label(id2);")

    try:
        result = [
            row
            for row in memgraph.execute_and_fetch("EXPLAIN USING INDEX :Label(id1) MATCH (n:Label1:Label2) RETURN n;")
        ]
    except mgclient.DatabaseError as e:
        assert str(e) == "Index for label Label and property id1 doesn't exist"
        return

    assert False


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
