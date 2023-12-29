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


def test_coordinator_role_port_throws():
    cursor = connect_default().cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO COORDINATOR WITH PORT 1011;")
    assert str(e.value) == "Port shouldn't be specified when setting replication role to coordinator!"


def test_coordinator_role_no_port():
    cursor = connect_default().cursor()
    execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO COORDINATOR;")
    res = execute_and_fetch_all(cursor, "SHOW REPLICATION ROLE;")
    assert cursor.description[0].name == "replication role"
    assert res[0][0] == "coordinator"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
