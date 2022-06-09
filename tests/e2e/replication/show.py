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

from pickle import FALSE
import sys

import os
import pytest
import signal
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
    # Goal of this test is to check the SHOW REPLICAS command.
    # 1/ We check that all replicas have the correct state: they should all be ready.
    # 2/ We drop one replica. It should not appear anymore in the SHOW REPLICAS command.
    # 3/ We kill another replica. It should become invalid in the SHOW REPLICAS command.
    cursor = connection(7687, "main").cursor()
    EXPECTED_COLUMN_NAMES = {"name", "socket_address", "sync_mode", "timeout", "state"}

    # 1/
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    expected_column_names = {"name", "socket_address", "sync_mode", "timeout", "state"}
    actual_column_names = {x.name for x in cursor.description}
    assert EXPECTED_COLUMN_NAMES == actual_column_names
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "sync", 1.0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", None, "ready"),
    }
    assert expected_data == actual_data

    # 2/
    execute_and_fetch_all(cursor, "DROP REPLICA replica_2")
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, "ready"),
        ("replica_3", "127.0.0.1:10003", "async", None, "ready"),
    }
    assert expected_data == actual_data

    # 3/
    for line in os.popen("ps ax | grep -E '7688|7690' | grep -E 'replica1.log|replica3.log' | grep -v grep"):
        try:
            fields = line.split()
            pid = fields[0]
            os.kill(int(pid), signal.SIGKILL)
        except OSError as ex:
            assert FALSE

    time.sleep(2)
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, "invalid"),
        ("replica_3", "127.0.0.1:10003", "async", None, "invalid"),
    }
    assert expected_data == actual_data


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
