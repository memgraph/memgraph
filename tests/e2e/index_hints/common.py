# Copyright 2023 Memgraph Ltd.
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
from gqlalchemy import Memgraph


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


@pytest.fixture
def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "USE DATABASE memgraph")
    try:
        execute_and_fetch_all(cursor, "DROP DATABASE clean")
    except:
        pass
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    yield connection


@pytest.fixture
def memgraph(**kwargs) -> Memgraph:
    memgraph = Memgraph()

    yield memgraph

    # Dropping the index here works around the fact that GQLAlchemy will
    # fail on `drop_indexes` because it tries to hash the composite
    # index properties. Once GQLAlchemy correctly supports these, this
    # hack can be removed.
    memgraph.execute("DROP INDEX ON :Label(id0)")
    memgraph.execute("DROP INDEX ON :Label(id1)")
    memgraph.execute("DROP INDEX ON :Label(id2)")
    memgraph.execute("DROP INDEX ON :Label1(a.b, c.d)")
    memgraph.execute("DROP INDEX ON :Label2(a.b, c.d)")
    memgraph.execute("DROP INDEX ON :Label1(a)")
    memgraph.execute("DROP INDEX ON :Label1(a, b.c)")
    memgraph.execute("DROP INDEX ON :Label1(a, b.c, d.e.f)")

    memgraph.drop_database()
    memgraph.drop_indexes()
