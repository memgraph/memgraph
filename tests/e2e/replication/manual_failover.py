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


@pytest.mark.parametrize(
    "port, role",
    [(7687, "main"), (7688, "replica"), (7689, "replica"), (7690, "coordinator")],
)
def test_replication_cluster_is_up(port, role, connection):
    cursor = connection(port, role).cursor()
    data = execute_and_fetch_all(cursor, "SHOW REPLICATION ROLE;")
    assert cursor.description[0].name == "replication role"
    assert data[0][0] == role


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
