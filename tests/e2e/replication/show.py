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

import mgclient
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

    expected_column_names = {"name", "socket_address", "sync_mode", "timeout"}
    actual_column_names = {x.name for x in cursor.description}
    assert expected_column_names == actual_column_names

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0),
        ("replica_2", "127.0.0.1:10002", "sync", 1.0),
        ("replica_3", "127.0.0.1:10003", "async", None),
    }
    assert expected_data == actual_data


def test_add_replicas_with_identical_name(connection):
    cursor = connection(7687, "main").cursor()

    # 1/ We just check that the test was correctly setup
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    expected_column_names = {"name", "socket_address", "sync_mode", "timeout"}
    actual_column_names = {x.name for x in cursor.description}
    assert expected_column_names == actual_column_names

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0.0),
        ("replica_2", "127.0.0.1:10002", "sync", 1.0),
        ("replica_3", "127.0.0.1:10003", "async", None),
    }
    assert expected_data == actual_data

    # 2/ We try to add another replica with a already existing name. We expect an exception.
    # Since we want to re-use the same ip:port than for replica_2, we first need to drop it.
    execute_and_fetch_all(cursor, "DROP REPLICA replica_2;")
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC WITH TIMEOUT 1 TO '127.0.0.1:10002'")

    # 3/ We re-register replica_2 again.
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 SYNC WITH TIMEOUT 1 TO '127.0.0.1:10002'")
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    assert expected_data == actual_data


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
