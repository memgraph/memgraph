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

import typing

import mgclient
import pytest


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


@pytest.fixture
def cursor() -> mgclient.Cursor:
    """A connection with the graph emptied, left as it was found.

    Torn down through the same client that set it up rather than through an
    object mapper, which needs a package the borrowed environment need not
    carry and whose failure would leave the graph behind for the next test.
    """
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")

    yield cursor

    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    for index_type, label, properties, _count in execute_and_fetch_all(cursor, "SHOW INDEX INFO"):
        if index_type != "label+property":
            continue
        # The properties come back as a list even where there is only one of
        # them, and a composite index is dropped by naming all of them.
        execute_and_fetch_all(cursor, f"DROP INDEX ON :{label}({', '.join(properties)})")
    connection.close()
