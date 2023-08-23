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
from common import connect, execute_and_fetch_all


def test_import_mode_on_off():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")


def test_import_mode_disabled_for_in_memory_storages():
    cursor = connect().cursor()
    try:
        execute_and_fetch_all(cursor, "EDGE IMPORT MODE ON")
        assert False
    except:
        execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")


def test_creating_vertices():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 2})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n) RETURN n"))) == 2
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n) RETURN n"))) == 2
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n) RETURN n"))) == 2
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n) RETURN n"))) == 2
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")


def test_creating_edges():
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (u:User {id: 2})")
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE ACTIVE")
    execute_and_fetch_all(cursor, "MATCH (n:User {id: 1}), (m:User {id: 2}) CREATE (n)-[r:FRIENDS {id: 3}]->(m)")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN n, r, m"))) == 1
    execute_and_fetch_all(cursor, "EDGE IMPORT MODE INACTIVE")
    assert len(list(execute_and_fetch_all(cursor, "MATCH (n)-[r]->(m) RETURN n, r, m"))) == 1
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
