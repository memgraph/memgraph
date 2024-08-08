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
from common import connect, connect_with_autocommit, execute_and_fetch_all, memgraph
from mgclient import DatabaseError


def test_create_trigger_on_create_periodic_commit(memgraph):
    QUERY_TRIGGER_CREATE = f"""
        CREATE TRIGGER CreateOnCreateTrigger
        ON () CREATE
        BEFORE COMMIT
        EXECUTE
        UNWIND createdVertices AS createdVertex
        CREATE (n:TriggerCreated)
    """

    # Setup queries
    memgraph.execute(QUERY_TRIGGER_CREATE)
    memgraph.execute("UNWIND range(1, 10) as x CALL { CREATE (n:PeriodicCommitCreated) } IN TRANSACTIONS OF 1 ROWS;")

    actual = list(memgraph.execute_and_fetch("MATCH (n:PeriodicCommitCreated) RETURN count(n) as cnt"))[0]["cnt"]
    assert actual == 10

    actual = list(memgraph.execute_and_fetch("MATCH (n:TriggerCreated) RETURN count(n) as cnt"))[0]["cnt"]
    assert actual == 10


def test_periodic_commit_uses_same_iterator_like_initial_transaction(memgraph):
    memgraph.execute("CREATE INDEX ON :Node(id)")
    memgraph.execute("CREATE (:Node {id: 1})")
    memgraph.execute("CREATE (:Node {id: 2})")

    memgraph.execute("USING PERIODIC COMMIT 1 UNWIND range(1, 3) as i MATCH (n:Node {id: i}) SET n.id = i + 2")
    results = list(memgraph.execute_and_fetch("MATCH (n) RETURN n.id AS id ORDER BY id"))

    assert results[0]["id"] == 3
    assert results[1]["id"] == 4


def test_periodic_commit_uses_same_iterator_like_initial_transaction_on_create(memgraph):
    memgraph.execute("CREATE INDEX ON :Node")

    memgraph.execute("CREATE (:Node {id: 1})")
    memgraph.execute("CREATE (:Node {id: 2})")
    memgraph.execute("MATCH (n:Node) CALL { CREATE (m:Node {id: 3}) } IN TRANSACTIONS OF 1 ROWS")

    results = list(memgraph.execute_and_fetch("MATCH (n) RETURN count(n) as cnt"))

    assert results[0]["cnt"] == 4


def test_periodic_commit_acid_guarantee_in_batches(memgraph):
    memgraph.execute("CREATE ({id: 1})")
    memgraph.execute("CREATE ({id: 2})")
    memgraph.execute("CREATE ({id: 3})")
    memgraph.execute("CREATE ({id: 4})")

    connection1 = connect()
    connection2 = connect_with_autocommit()
    cursor1 = connection1.cursor()
    cursor2 = connection2.cursor()

    execute_and_fetch_all(cursor1, "MATCH (n {id: 3}) SET n.id = 5")

    with pytest.raises(DatabaseError):
        execute_and_fetch_all(cursor2, "USING PERIODIC COMMIT 1 MATCH (n) SET n.id = n.id + 100")

    connection1.commit()

    results = list(memgraph.execute_and_fetch("MATCH (n) RETURN n.id as id"))
    assert len(results) == 4
    assert results[0]["id"] == 101
    assert results[1]["id"] == 102
    assert results[2]["id"] == 5
    assert results[3]["id"] == 4


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
