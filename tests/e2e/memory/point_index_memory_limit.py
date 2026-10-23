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
A write that hits its memory limit while the point index records the change must fail with a
memory limit error and leave the instance running. The limit is swept so the refused allocation
lands at different points of the change tracking.
"""

import sys

import mgclient
import pytest
from common import connect, execute_and_fetch_all

BOLT_PORT = 7687
LIMITS_KB = range(1, 65)
NUM_VERTICES = 4000

SET_POINT_QUERY = "MATCH (n:L) SET n.p = point({{x: n.id + 0.5, y: n.id + 0.5}}) QUERY MEMORY LIMIT {} KB"
CHANGE_LABEL_QUERY = "MATCH (n:L) REMOVE n:L SET n:L QUERY MEMORY LIMIT {} KB"
DETACH_DELETE_QUERY = "MATCH (n:L) WITH n LIMIT 50 DETACH DELETE n QUERY MEMORY LIMIT {} KB"


def cursor():
    return connect(host="localhost", port=BOLT_PORT).cursor()


@pytest.fixture(autouse=True)
def point_index_graph():
    # The setup session stays open during the sweep: where a refused allocation lands depends on
    # the heap layout, and this layout makes the sweep hit the change collector's growth.
    setup_cursor = cursor()
    execute_and_fetch_all(setup_cursor, "MATCH (n) DETACH DELETE n")
    execute_and_fetch_all(setup_cursor, "CREATE POINT INDEX ON :L(p)")
    execute_and_fetch_all(
        setup_cursor,
        f"UNWIND range(1, {NUM_VERTICES}) AS i CREATE (:L {{id: i, p: point({{x: toFloat(i), y: toFloat(i)}})}})",
    )
    yield
    execute_and_fetch_all(setup_cursor, "DROP POINT INDEX ON :L(p)")
    execute_and_fetch_all(setup_cursor, "MATCH (n) DETACH DELETE n")


@pytest.mark.parametrize(
    "query",
    [SET_POINT_QUERY, CHANGE_LABEL_QUERY, DETACH_DELETE_QUERY],
    ids=["set_point_property", "change_label", "detach_delete"],
)
def test_point_index_survives_query_memory_limit(query):
    for limit_kb in LIMITS_KB:
        try:
            execute_and_fetch_all(cursor(), query.format(limit_kb))
        except mgclient.DatabaseError as e:
            assert "Memory limit exceeded" in str(e), f"limit {limit_kb} KB: unexpected error: {e}"

    assert execute_and_fetch_all(cursor(), "RETURN 1") == [(1,)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
