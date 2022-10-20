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

import typing
import mgclient
import sys
import pytest
import time


@pytest.fixture(autouse=True)
def connection():
    connection = connect()
    return connection


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def has_n_result_row(cursor: mgclient.Cursor, query: str, n: int):
    results = execute_and_fetch_all(cursor, query)
    return len(results) == n


def wait_for_shard_manager_to_initialize():
    # The ShardManager in memgraph takes some time to initialize
    # the shards, thus we cannot just run the queries right away
    time.sleep(3)


def test_vertex_creation_and_scanall(connection):
    wait_for_shard_manager_to_initialize()
    cursor = connection.cursor()

    assert has_n_result_row(cursor, "CREATE (n :label {property:1})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:2})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:3})", 0)

    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 3)
    assert has_n_result_row(cursor, "MATCH (n) RETURN *", 3)
    assert has_n_result_row(cursor, "MATCH (n :label) RETURN *", 3)

    assert has_n_result_row(cursor, "MATCH (n), (m) CREATE (n)-[:TO]->(m)", 0)

    results = execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN n,r,m")
    assert len(results) == 9
    for (n, r, m) in results:
        n_props = n.properties
        assert len(n_props) == 0, "n is not expected to have properties, update the test!"
        assert len(n.labels) == 0, "n is not expected to have labels, update the test!"

        assert r.type == "TO"

        m_props = m.properties
        assert m_props["property"] <= 3 and m_props["property"] >= 0, "Wrong key"
        assert len(m.labels) == 0, "m is not expected to have labels, update the test!"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
