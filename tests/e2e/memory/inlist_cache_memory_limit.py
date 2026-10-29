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
A query that hits its memory limit while filling the IN-list cache must fail with a memory limit
error and leave the instance running. The limit is swept so the refused allocation lands at
different points of the cache fill: while the set grows and while an element is copied into it.
"""

import sys

import mgclient
import pytest
from common import connect, execute_and_fetch_all

BOLT_PORT = 7687
LIMITS_KB = range(1, 65)

INT_LIST_QUERY = (
    "MATCH (n:N) WHERE n.v IN [1, 50, 191] OR n.v IN [49, 76, 129, 193] OR n.v IN [3, 7, 11] "
    "RETURN count(*) QUERY MEMORY LIMIT {} KB"
)
STRING_LIST_QUERY = (
    "MATCH (n:N) WHERE n.s IN ['value_1_padded_past_small_string', 'value_50_padded_past_small_string', "
    "'value_191_padded_past_small_string', 'value_49_padded_past_small_string'] "
    "OR n.s IN ['value_76_padded_past_small_string', 'value_129_padded_past_small_string', "
    "'value_193_padded_past_small_string'] "
    "RETURN count(*) QUERY MEMORY LIMIT {} KB"
)


@pytest.fixture(scope="module", autouse=True)
def graph():
    connection = connect(host="localhost", port=BOLT_PORT)
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    execute_and_fetch_all(
        cursor, "UNWIND range(1, 2000) AS i CREATE (:N {v: i, s: 'value_' + toString(i) + '_padded_past_small_string'})"
    )
    yield
    execute_and_fetch_all(connect(host="localhost", port=BOLT_PORT).cursor(), "MATCH (n) DETACH DELETE n")


def run_with_limit(query, limit_kb):
    cursor = connect(host="localhost", port=BOLT_PORT).cursor()
    try:
        execute_and_fetch_all(cursor, query.format(limit_kb))
    except mgclient.DatabaseError as e:
        assert "Memory limit exceeded" in str(e), f"limit {limit_kb} KB: unexpected error: {e}"


@pytest.mark.parametrize("query", [INT_LIST_QUERY, STRING_LIST_QUERY], ids=["int_list", "string_list"])
def test_inlist_cache_survives_query_memory_limit(query):
    for limit_kb in LIMITS_KB:
        run_with_limit(query, limit_kb)

    cursor = connect(host="localhost", port=BOLT_PORT).cursor()
    assert execute_and_fetch_all(cursor, "RETURN 1") == [(1,)]


@pytest.mark.parametrize("query", [INT_LIST_QUERY, STRING_LIST_QUERY], ids=["int_list", "string_list"])
def test_inlist_cache_survives_query_memory_limit_in_explicit_transaction(query):
    for limit_kb in LIMITS_KB:
        connection = mgclient.connect(host="localhost", port=BOLT_PORT)
        connection.autocommit = False
        cursor = connection.cursor()
        try:
            execute_and_fetch_all(cursor, query.format(limit_kb))
            connection.commit()
        except mgclient.DatabaseError as e:
            assert "Memory limit exceeded" in str(e), f"limit {limit_kb} KB: unexpected error: {e}"
            connection.rollback()

    cursor = connect(host="localhost", port=BOLT_PORT).cursor()
    assert execute_and_fetch_all(cursor, "RETURN 1") == [(1,)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
