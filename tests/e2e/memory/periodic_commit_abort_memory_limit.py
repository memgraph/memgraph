# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""
A periodic commit that violates a constraint aborts its batch while the query memory limit is
active. The abort must roll the batch back completely and leave the instance running, whatever
the limit. The limit is swept so the refused allocation lands at different points, including
inside the abort.
"""

import sys

import mgclient
import pytest
from common import connect, execute_and_fetch_all

BOLT_PORT = 7687
LIMITS_KB = range(1024, 4096, 2)
NUM_EDGES = 2000

PERIODIC_COMMIT_QUERY = (
    "USING PERIODIC COMMIT 1000000 "
    "MATCH (h:Hub) UNWIND range(1, {n}) AS i CREATE (h)-[:E]->(m:M) "
    "WITH m, i WHERE i = {n} SET m:Bad "
    "QUERY MEMORY LIMIT {limit} KB"
)


def cursor():
    return connect(host="localhost", port=BOLT_PORT).cursor()


@pytest.fixture(autouse=True)
def hub_graph():
    setup_cursor = cursor()
    execute_and_fetch_all(setup_cursor, "MATCH (n) DETACH DELETE n")
    execute_and_fetch_all(setup_cursor, "CREATE CONSTRAINT ON (n:Bad) ASSERT EXISTS (n.p)")
    execute_and_fetch_all(setup_cursor, "CREATE (:Hub)")
    yield
    execute_and_fetch_all(setup_cursor, "DROP CONSTRAINT ON (n:Bad) ASSERT EXISTS (n.p)")
    execute_and_fetch_all(setup_cursor, "MATCH (n) DETACH DELETE n")


def test_periodic_commit_abort_survives_query_memory_limit():
    query_cursor = cursor()
    for limit_kb in LIMITS_KB:
        with pytest.raises(mgclient.DatabaseError) as e:
            execute_and_fetch_all(query_cursor, PERIODIC_COMMIT_QUERY.format(n=NUM_EDGES, limit=limit_kb))
        error = str(e.value)
        assert (
            "Memory limit exceeded" in error or "constraint violation" in error
        ), f"limit {limit_kb} KB: unexpected error: {error}"

    assert execute_and_fetch_all(cursor(), "MATCH (n) RETURN count(n)") == [(1,)]
    assert execute_and_fetch_all(cursor(), "MATCH ()-[e]->() RETURN count(e)") == [(0,)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
