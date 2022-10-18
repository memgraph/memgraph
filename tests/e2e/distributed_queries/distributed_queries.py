# Copyright 2021 Memgraph Ltd.
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

    assert has_n_result_row(cursor, "CREATE (n :label {property:1, asd:2})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:2, asd:2})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:3, asd:2})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:4, asd:2})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:5, asd:2})", 0)

    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 5)
    assert has_n_result_row(cursor, "MATCH (n) RETURN *", 5)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
