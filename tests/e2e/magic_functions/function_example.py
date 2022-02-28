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
from common import execute_and_fetch_all, has_n_result_row


def test_return_argument(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "CREATE (n:Label {id: 1});")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 1)
    result = execute_and_fetch_all(
        cursor, "MATCH (n) RETURN read.return_function_argument(n) AS argument;"
    )
    vertex = result[0][0]
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 1)
    assert isinstance(vertex, mgclient.Node)
    assert vertex.labels == set(["Label"])
    assert vertex.properties == {"id": 1}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
