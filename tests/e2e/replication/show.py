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

import pytest
import time

from common import execute_and_fetch_all


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
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    expected_column_names = {
        "name",
        "socket_address",
        "sync_mode",
        "timeout",
        "current_timestamp_of_replica",
        "number_of_timestamp_behind_master",
        "state",
    }
    actual_column_names = {x.name for x in cursor.description}
    assert expected_column_names == actual_column_names

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 2.0, 0, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "sync", 1.0, 0, 0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", None, 0, 0, "ready"),
    }
    assert expected_data == actual_data


def test_show_replicas_while_inserting_data(connection):
    # Goal is to check the timestamp are correctly computed from the information we get from replicas.
    # 0/ Check original state of replicas.
    # 1/ Add some data on main.
    # 2/ Check state of replicas.
    # 3/ Execute a read only query.
    # 4/ Check that the states have not changed.

    # 0/
    cursor = connection(7687, "main").cursor()
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    expected_column_names = {
        "name",
        "socket_address",
        "sync_mode",
        "timeout",
        "current_timestamp_of_replica",
        "number_of_timestamp_behind_master",
        "state",
    }
    actual_column_names = {x.name for x in cursor.description}
    assert expected_column_names == actual_column_names

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 2.0, 0, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "sync", 1.0, 0, 0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", None, 0, 0, "ready"),
    }
    assert expected_data == actual_data

    # 1/
    execute_and_fetch_all(cursor, "CREATE (n1:Number {name: 'forty_two', value:42});")
    time.sleep(1)

    # 2/
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 2.0, 4, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "sync", 1.0, 4, 0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", None, 4, 0, "ready"),
    }
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    assert expected_data == actual_data

    # 3/
    res = execute_and_fetch_all(cursor, "MATCH (node) return node;")
    assert 1 == len(res)

    # 4/
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    assert expected_data == actual_data


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
