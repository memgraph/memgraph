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
A read that hits its memory limit while caching long delta chains must fail with a memory limit
error and leave the instance running. The instance runs with --delta-chain-cache-threshold=1, so
every read that walks a delta chain fills the transaction's vertex info cache. The limit is swept
so the refused allocation lands at different points of the cache's growth.
"""

import sys

import mgclient
import pytest
from common import connect, connect_no_autocommit, execute_and_fetch_all

BOLT_PORT = 7687
LIMITS_KB = range(1, 129)
NUM_VERTICES = 2000

READ_QUERY = "MATCH (n:L) WITH n.p AS p, labels(n) AS l, properties(n) AS pr RETURN count(*) QUERY MEMORY LIMIT {} KB"
EDGE_READ_QUERY = "MATCH (n:L) RETURN sum(size([(n)-[]->(m) | m])) + sum(degree(n)) QUERY MEMORY LIMIT {} KB"


def connection():
    return connect(host="localhost", port=BOLT_PORT)


@pytest.fixture(autouse=True)
def graph():
    setup_cursor = connection().cursor()
    execute_and_fetch_all(setup_cursor, "MATCH (n) DETACH DELETE n")
    execute_and_fetch_all(
        setup_cursor, f"UNWIND range(1, {NUM_VERTICES}) AS i CREATE (:L {{id: i, p: 'x'}})-[:E]->(:M)"
    )
    yield
    execute_and_fetch_all(setup_cursor, "MATCH (n) DETACH DELETE n")


def read_after_concurrent_writes(writer_cursor, query, limit_kb):
    reader = connect_no_autocommit(host="localhost", port=BOLT_PORT)
    reader_cursor = reader.cursor()
    # Takes the reader's snapshot, so it has to walk the delta chains of the writes below.
    execute_and_fetch_all(reader_cursor, "MATCH (n) RETURN count(n)")
    execute_and_fetch_all(writer_cursor, f"MATCH (n:L) SET n.p = 'v{limit_kb}', n.q = {limit_kb}")
    try:
        execute_and_fetch_all(reader_cursor, query.format(limit_kb))
    except mgclient.DatabaseError as e:
        assert "Memory limit exceeded" in str(e), f"limit {limit_kb} KB: unexpected error: {e}"
    try:
        reader.rollback()
    except mgclient.DatabaseError:
        pass
    reader.close()


@pytest.mark.parametrize("query", [READ_QUERY, EDGE_READ_QUERY], ids=["properties_and_labels", "edges_and_degree"])
def test_delta_chain_cache_survives_query_memory_limit(query):
    writer_cursor = connection().cursor()
    for limit_kb in LIMITS_KB:
        read_after_concurrent_writes(writer_cursor, query, limit_kb)

    assert execute_and_fetch_all(connection().cursor(), "RETURN 1") == [(1,)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
