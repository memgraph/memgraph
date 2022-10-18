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
    yield connection


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def has_n_result_row(cursor: mgclient.Cursor, query: str, n: int):
    print("asd2222")
    results = execute_and_fetch_all(cursor, query)
    print(len(results))
    return len(results) == n


def has_one_result_row(cursor: mgclient.Cursor, query: str):
    return has_n_result_row(cursor, query, 1)


def test_vertex_creation_and_scanall(connection):
    # connection = connect()
    # yield connection
    # cursor = connection.cursor()
    # The ShardManager in memgraph takes some time to initialize the shards, thus we cannot just run the queries right away
    time.sleep(3)
    cursor = connection.cursor()

    some_dict = {}

    cursor.execute("CREATE (n :label {property:1})", {})
    # cursor.execute("MATCH (n) RETURN n", some_dict)
    # cursor.fetch_all()

    # has_n_result_row(cursor, "CREATE (n :label {property:1, asd:2})", 1)

    # assert has_n_result_row(cursor, "CREATE (n :label {property:1})", 0)
    # assert has_n_result_row(cursor, "CREATE (x :label {property:2})", 0)
    # assert has_n_result_row(cursor, "MATCH (n) RETURN *", 2)
    print("asd")
    # result = execute_and_fetch_all(
    #     cursor, "CALL write.create_vertex() YIELD v RETURN v")
    # vertex = result[0][0]
    # assert isinstance(vertex, mgclient.Node)
    # assert has_one_result_row(cursor, "MATCH (n) RETURN n")
    # assert vertex.labels == set()
    # assert vertex.properties == {}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
