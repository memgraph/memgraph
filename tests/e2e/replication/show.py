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

import pytest
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert_collection


@pytest.mark.parametrize(
    "port, role",
    [(7687, "main"), (7688, "replica"), (7689, "replica"), (7690, "replica")],
)
def test_show_replication_role(port, role, connection):
    cursor = connection(port, role).cursor()
    data = execute_and_fetch_all(cursor, "SHOW REPLICATION ROLE;")
    assert cursor.description[0].name == "replication role"
    assert data[0][0] == role


def test_show_replicas(connection):
    cursor = connection(7687, "main").cursor()
    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    expected_column_names = {
        "name",
        "socket_address",
        "sync_mode",
        "data_info",
    }
    actual_column_names = {x.name for x in cursor.description}
    assert actual_column_names == expected_column_names

    expected_data = [
        ("replica_1", "127.0.0.1:10001", "sync", {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}}),
        ("replica_2", "127.0.0.1:10002", "sync", {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}}),
        ("replica_3", "127.0.0.1:10003", "async", {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}}),
    ]
    assert all([x in actual_data for x in expected_data])


def test_show_replicas_while_inserting_data(connection):
    # Goal is to check the timestamp are correctly computed from the information we get from replicas.
    # 0/ Check original state of replicas.
    # 1/ Add some data on main.
    # 2/ Check state of replicas.
    # 3/ Execute a read only query.
    # 4/ Check that the states have not changed.

    # 0/
    cursor = connection(7687, "main").cursor()
    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    expected_column_names = {
        "name",
        "socket_address",
        "sync_mode",
        "data_info",
    }
    actual_column_names = {x.name for x in cursor.description}
    assert actual_column_names == expected_column_names

    expected_data = [
        ("replica_1", "127.0.0.1:10001", "sync", {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}}),
        ("replica_2", "127.0.0.1:10002", "sync", {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}}),
        ("replica_3", "127.0.0.1:10003", "async", {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}}),
    ]
    assert all([x in actual_data for x in expected_data])

    # 1/
    execute_and_fetch_all(cursor, "CREATE (n1:Number {name: 'forty_two', value:42});")

    # 2/
    expected_data = [
        ("replica_1", "127.0.0.1:10001", "sync", {"memgraph": {"ts": 4, "behind": 0, "status": "ready"}}),
        ("replica_2", "127.0.0.1:10002", "sync", {"memgraph": {"ts": 4, "behind": 0, "status": "ready"}}),
        ("replica_3", "127.0.0.1:10003", "async", {"memgraph": {"ts": 4, "behind": 0, "status": "ready"}}),
    ]

    def retrieve_data():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 3/
    res = execute_and_fetch_all(cursor, "MATCH (node) return node;")
    assert len(res) == 1

    # 4/
    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS;")
    assert all([x in actual_data for x in expected_data])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
