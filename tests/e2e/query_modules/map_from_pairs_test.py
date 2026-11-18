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

import sys

import pytest
from common import connect, execute_and_fetch_all


def test_null_value_in_inner_pair_is_stored():
    cursor = connect().cursor()

    query = 'RETURN map.from_pairs([["a", 1], ["b", null]]) AS map'
    try:
        result = execute_and_fetch_all(cursor, query)
    except Exception as exc:
        assert False, f"map.from_pairs rejected a null value inside an inner pair: {exc}"

    assert result == [({"a": 1, "b": None},)]


def test_null_value_in_inner_pair_reaches_the_write_path():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE (:WP {a: 1, b: 9})")

    query = 'MATCH (n:WP) SET n += map.from_pairs([["b", null]]) RETURN n.a AS a, n.b AS b'
    try:
        result = execute_and_fetch_all(cursor, query)
    except Exception as exc:
        assert False, f"map.from_pairs rejected a null value in the SET += write path: {exc}"

    assert result == [(1, None)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
