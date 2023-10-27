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

import default_config
import mgclient
import pytest

default_storage_info_dict = {
    "name": "memgraph",
    "vertex_count": 0,
    "edge_count": 0,
    "average_degree": 0,
    "memory_res": "",  # machine dependent
    "disk_usage": "",  # machine dependent
    "memory_tracked": "",  # machine dependent
    "allocation_limit": "",  # machine dependent
    "global_isolation_level": "SNAPSHOT_ISOLATION",
    "session_isolation_level": "",
    "next_session_isolation_level": "",
    "storage_mode": "IN_MEMORY_TRANSACTIONAL",
}


def apply_queries_and_check_for_storage_info(cursor, setup_query_list, expected_values):
    for query in setup_query_list:
        cursor.execute(query)

    cursor.execute("SHOW STORAGE INFO")
    config = cursor.fetchall()

    for conf in config:
        conf_name = conf[0]
        if conf_name in expected_values:
            assert expected_values[conf_name] == conf[1]


def test_does_default_config_match():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True

    cursor = connection.cursor()
    cursor.execute("SHOW STORAGE INFO")
    config = cursor.fetchall()

    # The default value of these is dependent on the given machine.
    machine_dependent_configurations = ["memory_res", "disk_usage", "memory_tracked", "allocation_limit"]

    # Number of different data-points returned by SHOW STORAGE INFO
    assert len(config) == 12

    for conf in config:
        conf_name = conf[0]
        if conf_name in machine_dependent_configurations:
            continue
        assert default_storage_info_dict[conf_name] == conf[1]


def test_info_change():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True

    cursor = connection.cursor()

    # Check for vertex and edge changes
    setup_query_list = [
        "CREATE(n{id: 1}),(m{id: 2})",
        "MATCH(n),(m) WHERE n.id = 1 AND m.id = 2 CREATE (n)-[r:relation]->(m)",
    ]
    expected_values = {
        "vertex_count": 2,
        "edge_count": 1,
    }

    apply_queries_and_check_for_storage_info(cursor, setup_query_list, expected_values)

    # Check for isolation level changes
    setup_query_list = [
        "SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        "SET NEXT TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
    ]

    expected_values = {
        "global_isolation_level": "READ_UNCOMMITTED",
        "session_isolation_level": "READ_COMMITTED",
        "next_session_isolation_level": "READ_COMMITTED",
    }

    apply_queries_and_check_for_storage_info(cursor, setup_query_list, expected_values)

    # Check for storage mode change
    setup_query_list = ["STORAGE MODE IN_MEMORY_ANALYTICAL"]

    expected_values = {"storage_mode": "IN_MEMORY_ANALYTICAL"}

    apply_queries_and_check_for_storage_info(cursor, setup_query_list, expected_values)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
