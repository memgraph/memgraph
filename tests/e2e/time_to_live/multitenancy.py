# Copyright 2022 Memgraph Ltd.
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
from functools import partial

import pytest
from common import connect, execute_and_fetch_all
from mg_utils import mg_sleep_and_assert


def assert_db(cursor, db):
    assert execute_and_fetch_all(cursor, "SHOW DATABASE")[0][0] == db


def get_n(cursor):
    return execute_and_fetch_all(cursor, "MATCH(n:TTL) RETURN COUNT(n)")[0][0]


def has_ttl_index(cursor):
    return any(row[1] == "TTL" and row[2] == ["ttl"] for row in execute_and_fetch_all(cursor, "SHOW INDEX INFO"))


def get_n_edges(cursor):
    return execute_and_fetch_all(cursor, "MATCH ()-[e]->() RETURN COUNT(e)")[0][0]


def test_ttl_on_default(connect):
    # Goal: to show that TTL is database specific
    # 0/ Setup TTL on default DB
    # 1/ Check that TTL is working
    # 2/ Create a new database
    # 3/ Check that TTL is not working

    memgraph = connect.cursor()

    # 0/
    assert_db(memgraph, "memgraph")
    memgraph.execute('ENABLE TTL EVERY "1s"')

    # 1/
    memgraph.execute("UNWIND RANGE(1,10) AS i CREATE (:TTL{ttl:0})")
    memgraph.execute("UNWIND RANGE(1,10) AS i CREATE ()-[:E{ttl:0}]->()")
    mg_sleep_and_assert(0, partial(get_n, memgraph))
    mg_sleep_and_assert(0, partial(get_n_edges, memgraph))

    # 2/
    memgraph.execute("CREATE DATABASE clean")
    memgraph.execute("USE DATABASE clean")
    assert_db(memgraph, "clean")

    # 3/
    memgraph.execute("UNWIND RANGE(1,10) AS i CREATE (:TTL{ttl:0})")
    memgraph.execute("UNWIND RANGE(1,10) AS i CREATE ()-[:E{ttl:0}]->()")
    mg_sleep_and_assert(10, partial(get_n, memgraph))
    mg_sleep_and_assert(10, partial(get_n_edges, memgraph), 3)


def test_ttl_on_clean(connect):
    # Goal: to show that TTL is database specific
    # 0/ Create a new database
    # 1/ Setup TTL on it
    # 2/ Check that TTL is working
    # 3/ Check that TTL is not working on default

    memgraph = connect.cursor()

    # 0/
    memgraph.execute("CREATE DATABASE clean")
    memgraph.execute("USE DATABASE clean")
    assert_db(memgraph, "clean")

    # 1/
    memgraph.execute('ENABLE TTL EVERY "1s"')

    # 2/
    memgraph.execute("UNWIND RANGE(1,10) AS i CREATE (:TTL{ttl:0})")
    memgraph.execute("UNWIND RANGE(1,10) AS i CREATE ()-[:E{ttl:0}]->()")
    mg_sleep_and_assert(0, partial(get_n, memgraph))
    mg_sleep_and_assert(0, partial(get_n_edges, memgraph))

    # 3/
    memgraph.execute("USE DATABASE memgraph")
    assert_db(memgraph, "memgraph")
    memgraph.execute("UNWIND RANGE(1,10) AS i CREATE (:TTL{ttl:0})")
    memgraph.execute("UNWIND RANGE(1,10) AS i CREATE ()-[:E{ttl:0}]->()")
    mg_sleep_and_assert(10, partial(get_n, memgraph), 3)
    mg_sleep_and_assert(10, partial(get_n_edges, memgraph), 3)


def test_ttl_after_rename(connect):
    # Goal: to show that TTL and the async indexer keep running on a renamed database
    # 0/ Setup TTL on a new database
    # 1/ Check that TTL is working
    # 2/ Rename the database
    # 3/ Check that TTL is still working
    # 4/ Re-enable TTL, so the async indexer rebuilds the TTL index
    # 5/ Check that TTL is still working

    memgraph = connect.cursor()

    try:
        # 0/
        memgraph.execute("CREATE DATABASE clean")
        memgraph.execute("USE DATABASE clean")
        memgraph.execute('ENABLE TTL EVERY "1s"')

        # 1/
        memgraph.execute("UNWIND RANGE(1,10) AS i CREATE (:TTL{ttl:0})")
        mg_sleep_and_assert(0, partial(get_n, memgraph))

        # 2/
        memgraph.execute("USE DATABASE memgraph")
        memgraph.execute("RENAME DATABASE clean TO renamed")
        memgraph.execute("USE DATABASE renamed")
        assert_db(memgraph, "renamed")

        # 3/
        memgraph.execute("UNWIND RANGE(1,10) AS i CREATE (:TTL{ttl:0})")
        mg_sleep_and_assert(0, partial(get_n, memgraph))

        # 4/
        memgraph.execute("DISABLE TTL")
        memgraph.execute('ENABLE TTL EVERY "1s"')
        mg_sleep_and_assert(True, partial(has_ttl_index, memgraph))

        # 5/
        memgraph.execute("UNWIND RANGE(1,10) AS i CREATE (:TTL{ttl:0})")
        mg_sleep_and_assert(0, partial(get_n, memgraph))
    finally:
        memgraph.execute("USE DATABASE memgraph")
        memgraph.execute("DROP DATABASE renamed")


def test_ttl_after_resume(connect):
    # Goal: to show that TTL runs again on a resumed database
    # 0/ Setup TTL on a new database
    # 1/ Check that TTL is working
    # 2/ Suspend and resume the database
    # 3/ Check that TTL is still working

    memgraph = connect.cursor()

    try:
        # 0/
        memgraph.execute("CREATE DATABASE clean")
        memgraph.execute("USE DATABASE clean")
        memgraph.execute('ENABLE TTL EVERY "1s"')

        # 1/
        memgraph.execute("UNWIND RANGE(1,10) AS i CREATE (:TTL{ttl:0})")
        mg_sleep_and_assert(0, partial(get_n, memgraph))

        # 2/
        memgraph.execute("USE DATABASE memgraph")
        memgraph.execute("SUSPEND DATABASE clean")
        memgraph.execute("RESUME DATABASE clean")
        memgraph.execute("USE DATABASE clean")

        # 3/
        memgraph.execute("UNWIND RANGE(1,10) AS i CREATE (:TTL{ttl:0})")
        mg_sleep_and_assert(0, partial(get_n, memgraph))
    finally:
        memgraph.execute("USE DATABASE memgraph")
        memgraph.execute("DROP DATABASE clean")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
