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

import sys
import typing

import mgclient
import pytest
from common import execute_and_fetch_all, has_n_result_row, has_one_result_row
from conftest import get_connection


def test_graph_mutability(connection):
    cursor = connection.cursor()
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)
    print("here")

    def test_mutability(is_write: bool):
        module = "write" if is_write else "read"

        result = list(
            execute_and_fetch_all(
                cursor, f"CALL {module}.graph_is_mutable() " "YIELD mutable, init_called RETURN mutable, init_called"
            )
        )
        assert result == [(False, True)]

        execute_and_fetch_all(cursor, "CREATE ()")
        result = list(
            execute_and_fetch_all(
                cursor,
                "MATCH (n) "
                f"CALL {module}.underlying_graph_is_mutable(n) "
                "YIELD mutable, init_called RETURN mutable, init_called",
            )
        )
        assert result == [(False, True)]

        execute_and_fetch_all(cursor, "CREATE ()-[:TYPE]->()")
        result = list(
            execute_and_fetch_all(
                cursor,
                "MATCH (n)-[e]->(m) "
                f"CALL {module}.underlying_graph_is_mutable(e) "
                "YIELD mutable, init_called RETURN mutable, init_called",
            )
        )

        assert result == [(False, True)]

    test_mutability(False)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
    # test_graph_mutability(connection=get_connection())
