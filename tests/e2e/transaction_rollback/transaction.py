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
from common import execute_and_fetch_all


def test_change_from_rollback(connection):
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    execute_and_fetch_all(cursor, "CREATE (n:Node1) CREATE (m:Node2) CREATE (k:Node3) CREATE (n)-[:Relationship]->(m);")
    connection.commit()

    execute_and_fetch_all(
        cursor,
        "MATCH (n:Node1)-[r:Relationship]->(m:Node2) MATCH (k:Node3) CALL transaction_rollback.set_from(r, k);",
    )
    connection.rollback()

    result = list(execute_and_fetch_all(cursor, f"MATCH (n)-[r]->(m) RETURN n, r, m"))
    assert len(result) == 1
    node_from, rel, node_to = result[0]
    assert list(node_from.labels)[0] == "Node1"
    assert list(node_to.labels)[0] == "Node2"


def test_change_to_rollback(connection):
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    execute_and_fetch_all(cursor, "CREATE (n:Node1) CREATE (m:Node2) CREATE (k:Node3) CREATE (n)-[:Relationship]->(m);")
    connection.commit()

    execute_and_fetch_all(
        cursor, "MATCH (n:Node1)-[r:Relationship]->(m:Node2) MATCH (k:Node3) CALL transaction_rollback.set_to(r, k);"
    )
    connection.rollback()

    result = list(execute_and_fetch_all(cursor, f"MATCH (n)-[r]->(m) RETURN n, r, m"))
    assert len(result) == 1
    node_from, rel, node_to = result[0]
    assert list(node_from.labels)[0] == "Node1"
    assert list(node_to.labels)[0] == "Node2"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
