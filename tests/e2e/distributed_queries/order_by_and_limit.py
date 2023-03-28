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


def test_order_by_and_limit(connection):
    wait_for_shard_manager_to_initialize()
    cursor = connection.cursor()

    results = execute_and_fetch_all(
        cursor,
        "UNWIND [{property:1}, {property:3}, {property:2}] AS map RETURN map ORDER BY map.property DESC",
    )
    assert len(results) == 3
    i = 3
    for map in results:
        assert len(map) == 1
        assert map[0]["property"] == i
        i = i - 1

    result = execute_and_fetch_all(
        cursor,
        "UNWIND [{property:1}, {property:3}, {property:2}] AS map RETURN map ORDER BY map.property LIMIT 1",
    )
    assert len(result) == 1
    assert result[0][0]["property"] == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
