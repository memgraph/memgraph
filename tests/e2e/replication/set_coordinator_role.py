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

import pytest
from common import connect_default, execute_and_fetch_all


# Coordinator doesn't have a port associated with it.
def test_coordinator_role_port_throws():
    cursor = connect_default().cursor()
    try:
        execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO COORDINATOR WITH PORT 1011;")
        assert False
    except Exception as e:
        assert str(e) == "Coordinator shouldn't have port as an integer literal!"


def test_coordinator_role_no_port():
    cursor = connect_default().cursor()
    execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO COORDINATOR;")
    res = execute_and_fetch_all(cursor, "SHOW REPLICATION ROLE;")
    assert cursor.description[0].name == "replication role"
    assert res[0][0] == "coordinator"


def test_registering_only_replicas_on_coordinator():
    # cursor = connect_default().cursor()
    # execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO COORDINATOR;")
    # execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    # execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002';")
    # execute_and_fetch_all(cursor, "SHOW REPLICATION CLUSTER;")
    # TODO: (andi) Test that the replicas are registered
    pass


def test_registering_only_main_on_coordinator():
    pass
    # cursor = connect_default().cursor()
    # execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO COORDINATOR;")
    # execute_and_fetch_all(cursor, "REGISTER MAIN TO '127.0.0.1:10005';")
    # execute_and_fetch_all(cursor, "SHOW REPLICATION CLUSTER;")
    # TODO: (andi) Test that the main is registered


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
