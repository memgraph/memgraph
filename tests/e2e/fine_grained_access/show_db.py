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

import sys

import common
import pytest
from mgclient import DatabaseError


def test_show_databases_w_user():
    admin_connection = common.connect(username="admin", password="test")
    user_connection = common.connect(username="user", password="test")
    user2_connection = common.connect(username="user2", password="test")
    user3_connection = common.connect(username="user3", password="test")

    assert common.execute_and_fetch_all(admin_connection.cursor(), "SHOW DATABASES") == [
        ("db1",),
        ("db2",),
        ("memgraph",),
    ]
    assert common.execute_and_fetch_all(admin_connection.cursor(), "SHOW DATABASE") == [("memgraph",)]

    assert common.execute_and_fetch_all(user_connection.cursor(), "SHOW DATABASES") == [("db1",), ("memgraph",)]
    assert common.execute_and_fetch_all(user_connection.cursor(), "SHOW DATABASE") == [("memgraph",)]

    assert common.execute_and_fetch_all(user2_connection.cursor(), "SHOW DATABASES") == [("db2",)]
    assert common.execute_and_fetch_all(user2_connection.cursor(), "SHOW DATABASE") == [("db2",)]

    assert common.execute_and_fetch_all(user3_connection.cursor(), "SHOW DATABASES") == [("db1",), ("db2",)]
    assert common.execute_and_fetch_all(user3_connection.cursor(), "SHOW DATABASE") == [("db1",)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
