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

import sys
import time
import typing

import mgclient
import pytest

from common import *


def test_vertex_creation_and_scanall(connection):
    wait_for_shard_manager_to_initialize()
    cursor = connection.cursor()

    assert has_n_result_row(cursor, "CREATE (n :label {property:1})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:2})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:3})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:4})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:10})", 0)

    results = execute_and_fetch_all(cursor, "MATCH (n) WITH COLLECT(n) as nn RETURN SIZE(nn)")
    assert len(results) == 1
    assert results[0][0] == 5

    results = execute_and_fetch_all(cursor, "MATCH (n) WITH COLLECT(n.property) as nn RETURN ALL(i IN nn WHERE i > 0)")
    assert len(results) == 1
    assert results[0][0] == True

    results = execute_and_fetch_all(cursor, """RETURN CONTAINS("Pineapple", "P")""")
    assert len(results) == 1
    assert results[0][0] == True

    results = execute_and_fetch_all(cursor, """RETURN ENDSWITH("Pineapple", "e")""")
    assert len(results) == 1
    assert results[0][0] == True

    results = execute_and_fetch_all(cursor, """RETURN LEFT("Pineapple", 1)""")
    assert len(results) == 1
    assert results[0][0] == "P"

    results = execute_and_fetch_all(cursor, """RETURN RIGHT("Pineapple", 1)""")
    assert len(results) == 1
    assert results[0][0] == "e"

    results = execute_and_fetch_all(cursor, """RETURN REVERSE("Apple")""")
    assert len(results) == 1
    assert results[0][0] == "elppA"

    results = execute_and_fetch_all(cursor, """RETURN REPLACE("Apple", "A", "a")""")
    assert len(results) == 1
    assert results[0][0] == "apple"

    results = execute_and_fetch_all(cursor, """RETURN TOLOWER("Apple")""")
    assert len(results) == 1
    assert results[0][0] == "apple"

    results = execute_and_fetch_all(cursor, """RETURN TOUPPER("Apple")""")
    assert len(results) == 1
    assert results[0][0] == "APPLE"

    results = execute_and_fetch_all(cursor, """RETURN TRIM("   Apple")""")
    assert len(results) == 1
    assert results[0][0] == "Apple"

    results = execute_and_fetch_all(cursor, """RETURN SPLIT("Apple.Apple", ".")""")
    assert len(results) == 1
    assert results[0][0] == ["Apple", "Apple"]

    results = execute_and_fetch_all(cursor, """RETURN LOG10(100)""")
    assert len(results) == 1
    assert results[0][0] == 2

    results = execute_and_fetch_all(cursor, """RETURN SQRT(4)""")
    assert len(results) == 1
    assert results[0][0] == 2


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
