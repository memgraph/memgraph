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

import re
import sys

import mgclient
import pytest

default_storage_info_dict = {
    "vm_max_map_count": 0,  # machine dependent
    "memory_res": "",  # machine dependent
    "peak_memory_res": "",  # machine dependent
    "disk_usage": "",  # machine dependent
    "memory_tracked": "",  # machine dependent
    "memory_limit": "",  # machine dependent
    "license_memory_limit": "",  # license dependent
    "query+graph_memory_tracked": "",  # machine dependent
    "vector_index_memory_tracked": "",  # machine dependent
    "global_isolation_level": "SNAPSHOT_ISOLATION",
    "session_isolation_level": "",
    "next_session_isolation_level": "",
    "global_storage_mode": "IN_MEMORY_TRANSACTIONAL",
}


def test_does_default_config_match():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True

    cursor = connection.cursor()
    cursor.execute("SHOW STORAGE INFO")
    config = cursor.fetchall()

    # The default value of these is dependent on the given machine.
    machine_dependent_configurations = [
        "memory_res",
        "peak_memory_res",
        "disk_usage",
        "memory_tracked",
        "memory_limit",
        "license_memory_limit",
        "vm_max_map_count",
        "query+graph_memory_tracked",
        "vector_index_memory_tracked",
    ]
    # Number of different data-points returned by SHOW STORAGE INFO
    assert len(config) == len(default_storage_info_dict)

    for conf in config:
        conf_name = conf[0]
        if conf_name in machine_dependent_configurations:
            continue
        assert default_storage_info_dict[conf_name] == conf[1]


def test_info_change():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True

    cursor = connection.cursor()

    # Check for vertex and edge changes (DB-specific fields)
    setup_query_list = [
        "CREATE(n{id: 1}),(m{id: 2})",
        "MATCH(n),(m) WHERE n.id = 1 AND m.id = 2 CREATE (n)-[r:relation]->(m)",
    ]
    for query in setup_query_list:
        cursor.execute(query)

    cursor.execute("SHOW STORAGE INFO ON CURRENT DATABASE")
    config = cursor.fetchall()
    db_values = {row[0]: row[1] for row in config}
    assert db_values.get("vertex_count") == 2
    assert db_values.get("edge_count") == 1

    # Check for isolation level changes (DB-scoped mutations)
    for query in [
        "SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        "SET NEXT TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
    ]:
        cursor.execute(query)

    cursor.execute("SHOW STORAGE INFO ON CURRENT DATABASE")
    config = cursor.fetchall()
    db_values = {row[0]: row[1] for row in config}
    assert db_values.get("storage_isolation_level") == "READ_UNCOMMITTED"

    # session and next_session isolation levels are returned by the bare query only,
    # so fetch them separately.
    cursor.execute("SHOW STORAGE INFO")
    global_values = {row[0]: row[1] for row in cursor.fetchall()}
    assert global_values.get("session_isolation_level") == "READ_COMMITTED"
    assert global_values.get("next_session_isolation_level") == "READ_COMMITTED"

    # Check for storage mode change (DB-scoped mutation)
    cursor.execute("STORAGE MODE IN_MEMORY_ANALYTICAL")
    cursor.execute("SHOW STORAGE INFO ON CURRENT DATABASE")
    config = cursor.fetchall()
    db_values = {row[0]: row[1] for row in config}
    assert db_values.get("storage_mode") == "IN_MEMORY_ANALYTICAL"


def test_show_storage_info_on_database():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()

    cursor.execute("SHOW STORAGE INFO ON DATABASE memgraph")
    config = {row[0]: row[1] for row in cursor.fetchall()}

    expected_fields = [
        "name",
        "database_uuid",
        "storage_mode",
        "vertex_count",
        "edge_count",
        "average_degree",
        "unreleased_delta_objects",
        "disk_usage",
        "graph_memory_tracked",
        "query_memory_tracked",
        "vector_index_memory_tracked",
        "tenant_memory_tracked",
        "tenant_peak_memory_tracked",
        "tenant_memory_limit",
        "storage_isolation_level",
    ]

    missing = [f for f in expected_fields if f not in config]
    assert not missing, f"Missing fields in SHOW STORAGE INFO ON DATABASE: {missing}"

    # Session-level fields must NOT be present.
    assert "session_isolation_level" not in config
    assert "next_session_isolation_level" not in config
    assert "memory_tracked" not in config

    assert config["name"] == "memgraph"
    assert config["storage_mode"] in ("IN_MEMORY_TRANSACTIONAL", "IN_MEMORY_ANALYTICAL")
    assert config["tenant_memory_limit"] != "unlimited"


def test_show_storage_info_bare_vs_on_current_database():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()

    cursor.execute("SHOW STORAGE INFO")
    bare = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("SHOW STORAGE INFO ON CURRENT DATABASE")
    current = {row[0]: row[1] for row in cursor.fetchall()}

    # Bare/global variant must contain instance-level fields
    assert "memory_tracked" in bare
    assert "vm_max_map_count" in bare
    assert "query+graph_memory_tracked" in bare

    # ON CURRENT DATABASE variant must contain DB-level fields
    assert "name" in current
    assert "vertex_count" in current
    assert "edge_count" in current
    assert "graph_memory_tracked" in current
    assert "query_memory_tracked" in current

    # They must NOT contain each other's exclusive fields
    assert "vertex_count" not in bare
    assert "name" not in bare
    assert "memory_tracked" not in current
    assert "vm_max_map_count" not in current


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
