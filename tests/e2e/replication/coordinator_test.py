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


def test_registering_only_replicas_on_coordinator():
    cursor = connect_default().cursor()
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002';")
    # execute_and_fetch_all(cursor, "SHOW REPLICATION CLUSTER;")
    # TODO: (andi) Test that the replicas are registered
    execute_and_fetch_all(cursor, "DROP REPLICA replica_1")
    execute_and_fetch_all(cursor, "DROP REPLICA replica_2")


def test_registering_only_main_on_coordinator():
    pass
    # cursor = connect_default().cursor()
    # execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO COORDINATOR;")
    # execute_and_fetch_all(cursor, "REGISTER MAIN TO '127.0.0.1:10005';")
    # execute_and_fetch_all(cursor, "SHOW REPLICATION CLUSTER;")
    # TODO: (andi) Test that the main is registered


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
