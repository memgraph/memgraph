# Copyright 2024 Memgraph Ltd.
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
import pytest


@pytest.fixture(scope="module")
def cursor(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    cursor = connection.cursor()

    cursor.execute("FOREACH (i IN range(1, 1000) | CREATE (n:Node {id: i}));")
    cursor.execute("CREATE INDEX ON :Node(id);")
    cursor.execute("CREATE INDEX ON :Node")

    yield cursor

    cursor.execute("DROP INDEX ON :Node(id);")
    cursor.execute("DROP INDEX ON :Node;")
    cursor.execute("MATCH (n:Node) DELETE n;")


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = dict()) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()
