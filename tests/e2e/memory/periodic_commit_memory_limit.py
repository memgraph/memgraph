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
A PERIODIC COMMIT query that hits its memory limit while starting the next batch's transaction
must fail with a memory limit error and leave the instance running. With automatic index creation
enabled, starting a transaction copies every existing label index, so many indexes make that the
largest allocation of the batch boundary. The limit is swept so the refused allocation lands at
different points of the query.
"""

import sys

import mgclient
import pytest
from common import connect, execute_and_fetch_all

BOLT_PORT = 7687
LIMITS_KB = range(1, 129)
NUM_INDEXES = 3000

PERIODIC_CREATE_QUERY = "USING PERIODIC COMMIT 5 UNWIND range(1, 200) AS i CREATE (:Base) QUERY MEMORY LIMIT {} KB"


def cursor():
    return connect(host="localhost", port=BOLT_PORT).cursor()


@pytest.fixture(autouse=True)
def many_label_indexes():
    setup_cursor = cursor()
    execute_and_fetch_all(setup_cursor, "MATCH (n) DETACH DELETE n")
    for i in range(NUM_INDEXES):
        execute_and_fetch_all(setup_cursor, f"CREATE INDEX ON :IDX{i}")
    yield
    for i in range(NUM_INDEXES):
        execute_and_fetch_all(setup_cursor, f"DROP INDEX ON :IDX{i}")
    execute_and_fetch_all(setup_cursor, "MATCH (n) DETACH DELETE n")


def test_periodic_commit_survives_query_memory_limit():
    for limit_kb in LIMITS_KB:
        try:
            execute_and_fetch_all(cursor(), PERIODIC_CREATE_QUERY.format(limit_kb))
        except mgclient.DatabaseError as e:
            assert "Memory limit exceeded" in str(e), f"limit {limit_kb} KB: unexpected error: {e}"

    assert execute_and_fetch_all(cursor(), "RETURN 1") == [(1,)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
