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
from common import execute_and_fetch_all


@pytest.mark.parametrize("port, role", [(7687, "main"), (7688, "replica"), (7689, "replica"), (7690, "coordinator")])
def test_replication_cluster_is_up(port, role, connection):
    cursor = connection(port, role).cursor()
    data = execute_and_fetch_all(cursor, "SHOW REPLICATION ROLE;")
    assert cursor.description[0].name == "replication role"
    assert data[0][0] == role


@pytest.mark.parametrize("port, role", [(7688, "replica"), (7689, "replica")])
def test_replica_cannot_register_replica(port, role, connection):
    cursor = connection(port, role).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001'")
    assert str(e.value) == "Replica can't register another replica!"


@pytest.mark.parametrize("port, role", [(7688, "replica"), (7689, "replica")])
def test_replica_cannot_become_coordinator(port, role, connection):
    cursor = connection(port, role).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO COORDINATOR;")
    assert str(e.value) == "Couldn't set replication role to coordinator!"


@pytest.mark.parametrize("port, role", [(7687, "main"), (7688, "replica"), (7689, "replica")])
def test_main_and_replica_cannot_register_main(port, role, connection):
    cursor = connection(port, role).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "REGISTER MAIN TO '127.0.0.1:10001';")
    assert str(e.value) == "Only coordinator can register main instance!"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
