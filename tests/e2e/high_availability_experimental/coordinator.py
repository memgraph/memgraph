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
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert


def test_disable_cypher_queries(connection):
    cursor = connection(7690, "coordinator").cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "CREATE (n:TestNode {prop: 'test'})")
    assert str(e.value) == "Coordinator can run only coordinator queries!"


def test_coordinator_cannot_be_replica_role(connection):
    cursor = connection(7690, "coordinator").cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")
    assert str(e.value) == "Coordinator can run only coordinator queries!"


def test_coordinator_cannot_run_show_repl_role(connection):
    cursor = connection(7690, "coordinator").cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SHOW REPLICATION ROLE;")
    assert str(e.value) == "Coordinator can run only coordinator queries!"


def test_coordinator_show_replication_cluster(connection):
    cursor = connection(7690, "coordinator").cursor()

    def retrieve_data():
        return sorted(list(execute_and_fetch_all(cursor, "SHOW REPLICATION CLUSTER;")))

    expected_data = [
        ("instance_1", "127.0.0.1:10011", True, "replica"),
        ("instance_2", "127.0.0.1:10012", True, "replica"),
        ("instance_3", "127.0.0.1:10013", True, "main"),
    ]
    mg_sleep_and_assert(expected_data, retrieve_data)


def test_coordinator_cannot_call_show_replicas(connection):
    cursor = connection(7690, "coordinator").cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SHOW REPLICAS;")
    assert str(e.value) == "Coordinator can run only coordinator queries!"


@pytest.mark.parametrize(
    "port, role",
    [(7687, "main"), (7688, "replica"), (7689, "replica")],
)
def test_main_and_replicas_cannot_call_show_repl_cluster(port, role, connection):
    cursor = connection(port, role).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SHOW REPLICATION CLUSTER;")
    assert str(e.value) == "Only coordinator can run SHOW REPLICATION CLUSTER."


@pytest.mark.parametrize(
    "port, role",
    [(7687, "main"), (7688, "replica"), (7689, "replica")],
)
def test_main_and_replicas_cannot_register_coord_server(port, role, connection):
    cursor = connection(port, role).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(
            cursor,
            "REGISTER INSTANCE instance_1 ON '127.0.0.1:10001' WITH '127.0.0.1:10011';",
        )
    assert str(e.value) == "Only coordinator can register coordinator server!"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
