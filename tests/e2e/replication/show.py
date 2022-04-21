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
import sys
from itertools import zip_longest

import pytest
import mgclient
from common import execute_and_fetch_all


def test_show_replication_role(connection):
    cursor = connection.cursor()
    data = execute_and_fetch_all(cursor, "SHOW REPLICATION ROLE;")

    assert cursor.description[0].name == "replication role"
    assert data[0][0] == "main"


def test_show_replicas(connection):
    cursor = connection.cursor()
    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS")

    expected_column_names = ["name", "socket_address", "sync_mode", "timeout"]
    actual_column_names = list(map(lambda x: x.name, cursor.description))
    for expected, actual in zip_longest(expected_column_names, actual_column_names):
        assert expected == actual

    expecte_data = (
        ("replica_1", "127.0.0.1:10001", "sync", 0),
        ("replica_2", "127.0.0.1:10002", "sync", 1.0),
        ("replica_3", "127.0.0.1:10003", "async", None),
    )
    for expected, actual in zip_longest(expecte_data, actual_data):
        assert expected == actual


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
