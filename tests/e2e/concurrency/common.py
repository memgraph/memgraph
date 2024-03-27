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


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def execute_and_fetch_all_with_commit(
    connection: mgclient.Connection, query: str, params: dict = {}
) -> typing.List[tuple]:
    cursor = connection.cursor()
    cursor.execute(query, params)
    results = cursor.fetchall()
    connection.commit()
    return results


@pytest.fixture
def first_connection(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "USE DATABASE memgraph")
    try:
        execute_and_fetch_all(cursor, "DROP DATABASE clean")
    except:
        pass
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    connection.autocommit = False
    yield connection


@pytest.fixture
def second_connection(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "USE DATABASE memgraph")
    try:
        execute_and_fetch_all(cursor, "DROP DATABASE clean")
    except:
        pass
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    connection.autocommit = False
    yield connection
