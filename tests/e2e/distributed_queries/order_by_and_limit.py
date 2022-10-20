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
import time
from common import *


def test_order_by_and_limit(connection):
    wait_for_shard_manager_to_initialize()
    cursor = connection.cursor()

    assert has_n_result_row(cursor, "CREATE (n :label {property:1})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:2})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:3})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:4})", 0)

    assert has_n_result_row(cursor, "MATCH (n) WITH collect(n) AS nd UNWIND nd AS result RETURN result", 4)

    results = execute_and_fetch_all(cursor, "MATCH (n) RETURN n ORDER BY n.property DESC")
    assert len(results) == 4
    i = 4
    for n in results:
        n_props = n[0].properties
        assert len(n_props) == 1
        assert n_props["property"] == i
        i = i - 1

    result = execute_and_fetch_all(cursor, "MATCH (n) RETURN n ORDER BY n.property LIMIT 1")
    assert len(result) == 1
    assert result[0][0].properties["property"] == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
