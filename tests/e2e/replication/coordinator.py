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
from common import execute_and_fetch_all


def test_disable_cypher_queries(connection):
    cursor = connection(7690, "coordinator").cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "CREATE (n:TestNode {prop: 'test'})")
    assert str(e.value) == "Coordinator can run only replication queries!"


def test_coordinator_cannot_be_replica_role(connection):
    cursor = connection(7690, "coordinator").cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")
    assert str(e.value) == "Coordinator cannot become a replica!"


def test_coordinator_cannot_run_show_repl_role(connection):
    cursor = connection(7690, "coordinator").cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SHOW REPLICATION ROLE;")
    assert str(e.value) == "Coordinator doesn't have a replication role!"


def test_coordinator_show_replication_cluster(connection):
    cursor = connection(7690, "coordinator").cursor()
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICATION CLUSTER;"))

    expected_column_names = {"name", "socket_address"}
    actual_column_names = {x.name for x in cursor.description}
    assert actual_column_names == expected_column_names

    expected_data = {
        ("main", "127.0.0.1:10013"),
        ("replica_1", "127.0.0.1:10011"),
        ("replica_2", "127.0.0.1:10012"),
    }
    assert actual_data == expected_data


def test_coordinator_cannot_call_show_replicas(connection):
    cursor = connection(7690, "coordinator").cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SHOW REPLICAS;")
    assert str(e.value) == "Coordinator cannot call SHOW REPLICAS! Use SHOW REPLICATION CLUSTER instead."


@pytest.mark.parametrize(
    "port, role",
    [(7687, "main"), (7688, "replica"), (7689, "replica")],
)
def test_main_and_relicas_cannot_call_show_repl_cluster(port, role, connection):
    cursor = connection(port, role).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SHOW REPLICATION CLUSTER;")
    assert str(e.value) == "Only coordinator can call SHOW REPLICATION CLUSTER!"


@pytest.mark.parametrize(
    "port, role",
    [(7687, "main"), (7688, "replica"), (7689, "replica")],
)
def test_main_and_replicas_cannot_register_coord_server(port, role, connection):
    cursor = connection(port, role).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "REGISTER REPLICA COORDINATOR SERVER ON replica_1 TO '127.0.0.1:10005';")
    assert str(e.value) == "Only coordinator can register coordinator server!"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
