# Copyright 2024 Memgraph Ltd.
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
from common import cursor, execute_and_fetch_all


def test_removing_obsolete_indexes(cursor):
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    expected_index_info = {
        ("label", "Node", None, 1000),
        ("label+property", "Node", "id", 1000),
    }
    assert set(index_info) == expected_index_info

    cursor.execute("MATCH (n) SET n.id = n.id + 1000;")

    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    expected_index_info = {
        ("label", "Node", None, 1000),
        ("label+property", "Node", "id", 2000),
    }
    assert set(index_info) == expected_index_info

    cursor.execute("FREE MEMORY;")
    index_info = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    expected_index_info = {
        ("label", "Node", None, 1000),
        ("label+property", "Node", "id", 1000),
    }
    assert set(index_info) == expected_index_info


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
