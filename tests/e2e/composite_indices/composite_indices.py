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
from common import connect, execute_and_fetch_all, memgraph

QUERY_PLAN = "QUERY PLAN"


def test_basic_composite_label_property_index(memgraph):
    memgraph.execute("CREATE INDEX ON :Node(prop1, prop2);")
    memgraph.execute("CREATE (n:Node {prop1: 1, prop2: 2})")

    expected_explain = [
        f" * Produce {{n}}",
        f" * ScanAllByLabelPropertyCompositeValue (n :Node {{prop1, prop2}})",
        f" * Once",
    ]

    actual_explain = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node) WHERE n.prop1 = 1 AND n.prop2 = 2 RETURN n")
    )
    actual_explain = [x[QUERY_PLAN] for x in actual_explain]

    actual_explain = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node) WHERE n.prop1 < 1 AND n.prop2 = 2 RETURN n")
    )
    actual_explain = [x[QUERY_PLAN] for x in actual_explain]

    assert expected_explain == actual_explain

    actual_explain = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node) WHERE n.prop1 > 1 AND n.prop2 = 2 RETURN n")
    )
    actual_explain = [x[QUERY_PLAN] for x in actual_explain]

    assert expected_explain == actual_explain

    actual_explain = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node) WHERE n.prop1 <= 1 AND n.prop2 = 2 RETURN n")
    )
    actual_explain = [x[QUERY_PLAN] for x in actual_explain]

    assert expected_explain == actual_explain

    actual_explain = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node) WHERE n.prop1 >= 1 AND n.prop2 = 2 RETURN n")
    )
    actual_explain = [x[QUERY_PLAN] for x in actual_explain]

    assert expected_explain == actual_explain

    actual_explain = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node) WHERE n.prop1 < 1 AND n.prop2 < 2 RETURN n")
    )
    actual_explain = [x[QUERY_PLAN] for x in actual_explain]

    assert expected_explain == actual_explain

    actual_explain = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node) WHERE n.prop1 < 1 AND n.prop2 > 2 RETURN n")
    )
    actual_explain = [x[QUERY_PLAN] for x in actual_explain]

    assert expected_explain == actual_explain

    actual_explain = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node) WHERE n.prop1 is not null AND n.prop2 is not null RETURN n")
    )
    actual_explain = [x[QUERY_PLAN] for x in actual_explain]

    assert expected_explain == actual_explain

    actual_explain = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node) WHERE n.prop1 is not null AND n.prop2 = 2 RETURN n")
    )
    actual_explain = [x[QUERY_PLAN] for x in actual_explain]

    assert expected_explain == actual_explain

    actual_explain = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node) WHERE n.prop1 is not null AND n.prop2 < 2 RETURN n")
    )
    actual_explain = [x[QUERY_PLAN] for x in actual_explain]

    assert expected_explain == actual_explain

    actual_explain = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node) WHERE n.prop1 is not null AND n.prop2 > 2 RETURN n")
    )
    actual_explain = [x[QUERY_PLAN] for x in actual_explain]

    assert expected_explain == actual_explain


def test_free_memory_composite_indices(memgraph):
    memgraph.execute("CREATE INDEX ON :Node(prop1, prop2);")
    memgraph.execute("CREATE (n:Node {prop1: 1, prop2: 2})")

    index_count = list(memgraph.execute_and_fetch("SHOW INDEX INFO;"))[0]["count"]

    assert index_count == 1

    memgraph.execute("MATCH (n) SET n.prop1 = 3;")
    memgraph.execute("FREE MEMORY;")

    index_count = list(memgraph.execute_and_fetch("SHOW INDEX INFO;"))[0]["count"]
    assert index_count == 1


def test_index_info_with_2_transactions(memgraph):
    memgraph.execute("CREATE INDEX ON :Node(prop1, prop2);")
    memgraph.execute("CREATE (n:Node {prop1: 1, prop2: 2})")
    memgraph.execute("FREE MEMORY;")

    connection1 = connect()
    connection2 = connect()
    cursor1 = connection1.cursor()
    cursor2 = connection2.cursor()
    execute_and_fetch_all(cursor1, "MATCH (n) SET n.prop1 = 3")

    index_count = list(memgraph.execute_and_fetch("SHOW INDEX INFO;"))[0]["count"]
    assert index_count == 2

    execute_and_fetch_all(cursor2, "UNWIND range(1, 5) AS x CREATE (:Node {prop1: x, prop2: x})")
    index_count = list(memgraph.execute_and_fetch("SHOW INDEX INFO;"))[0]["count"]
    assert index_count == 7

    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor2, "MATCH (n) SET n.prop1 = 4")

    index_count = list(memgraph.execute_and_fetch("SHOW INDEX INFO;"))[0]["count"]
    assert index_count == 2

    connection1.commit()

    memgraph.execute("FREE MEMORY")

    index_count = list(memgraph.execute_and_fetch("SHOW INDEX INFO;"))[0]["count"]
    assert index_count == 1


def test_isolation_one_transaction_cant_see_other_with_property_composite_index(memgraph):
    memgraph.execute("CREATE INDEX ON :Node(prop1, prop2);")
    memgraph.execute("CREATE (n:Node {prop1: 1, prop2: 2})")
    memgraph.execute("FREE MEMORY;")

    connection1 = connect()
    cursor1 = connection1.cursor()
    execute_and_fetch_all(cursor1, "MATCH (n) SET n.prop1 = 3")

    results = list(memgraph.execute_and_fetch("MATCH (n) RETURN n.prop1 AS prop1, n.prop2 AS prop2;"))

    assert len(results) == 1

    properties = results[0]
    prop1, prop2 = properties["prop1"], properties["prop2"]

    assert prop1 == 1
    assert prop2 == 2

    execute_and_fetch_all(cursor1, "CREATE (n:Node) SET n.prop1 = 4, n.prop2 = 5")

    results = list(memgraph.execute_and_fetch("MATCH (n) RETURN n.prop1 AS prop1, n.prop2 AS prop2;"))

    assert len(results) == 1

    properties = results[0]
    prop1, prop2 = properties["prop1"], properties["prop2"]

    assert prop1 == 1
    assert prop2 == 2

    connection1.commit()

    results = list(memgraph.execute_and_fetch("MATCH (n) RETURN n.prop1 AS prop1, n.prop2 AS prop2;"))

    assert len(results) == 2

    properties = results[0]
    prop1, prop2 = properties["prop1"], properties["prop2"]

    assert prop1 == 3
    assert prop2 == 2

    properties = results[1]
    prop1, prop2 = properties["prop1"], properties["prop2"]

    assert prop1 == 4
    assert prop2 == 5


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
