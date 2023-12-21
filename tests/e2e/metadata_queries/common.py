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


@pytest.fixture(scope="module")
def cursor(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection.cursor()


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = dict()) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def are_results_equal(result1, result2):
    if len(result1) != len(result2):
        return False

    return sorted(result1) == sorted(result2)
