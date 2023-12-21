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
import time

import pytest
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert


# BUGFIX: for issue https://github.com/memgraph/memgraph/issues/1515
def test_replication_handles_delete_when_multiple_edges_of_same_type(connection):
    # Goal is to check the timestamp are correctly computed from the information we get from replicas.
    # 0/ Check original state of replicas.
    # 1/ Add nodes and edges to MAIN, then delete the edges.
    # 2/ Check state of replicas.

    # 0/
    conn = connection(7687, "main")
    conn.autocommit = True
    cursor = conn.cursor()
    actual_data = set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 0, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 0, 0, "ready"),
    }
    assert actual_data == expected_data

    # 1/
    execute_and_fetch_all(cursor, "CREATE (a)-[r:X]->(b) CREATE (a)-[:X]->(b) DELETE r;")

    # 2/
    expected_data = {
        ("replica_1", "127.0.0.1:10001", "sync", 2, 0, "ready"),
        ("replica_2", "127.0.0.1:10002", "async", 2, 0, "ready"),
    }

    def retrieve_data():
        return set(execute_and_fetch_all(cursor, "SHOW REPLICAS;"))

    actual_data = mg_sleep_and_assert(expected_data, retrieve_data)
    assert actual_data == expected_data


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
