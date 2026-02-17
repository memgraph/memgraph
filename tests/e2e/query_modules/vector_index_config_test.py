# Copyright 2025 Memgraph Ltd.
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
from common import execute_and_fetch_all


def test_create_vector_index_with_config_function_default(connection):
    """CREATE VECTOR INDEX with WITH CONFIG vector_index_config.default_config()."""
    cursor = connection.cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE VECTOR INDEX idx_vec ON :Label(vec) WITH CONFIG vector_index_config.default_config();",
    )
    info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(info) == 1
    assert info[0][0] == "idx_vec"
    assert info[0][1] == "Label"
    assert info[0][2] == "vec"
    execute_and_fetch_all(cursor, "DROP VECTOR INDEX idx_vec;")


def test_create_vector_index_with_config_function_params(connection):
    """CREATE VECTOR INDEX with WITH CONFIG vector_index_config.config(dimension, capacity, ...)."""
    cursor = connection.cursor()
    execute_and_fetch_all(
        cursor,
        "CREATE VECTOR INDEX idx_vec2 ON :Label2(vec) WITH CONFIG vector_index_config.config(4, 100);",
    )
    info = execute_and_fetch_all(cursor, "SHOW VECTOR INDEX INFO;")
    assert len(info) == 1
    assert info[0][0] == "idx_vec2"
    assert info[0][1] == "Label2"
    assert info[0][2] == "vec"
    execute_and_fetch_all(cursor, "DROP VECTOR INDEX idx_vec2;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
