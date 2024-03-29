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
from common import connect, execute_and_fetch_all


def test_creating_edges_by_loading_vertices_from_index(connect):
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    execute_and_fetch_all(cursor, "CREATE (:User {id: 1, completion_percentage: 14, gender: 'man', age: 26})")
    execute_and_fetch_all(cursor, "CREATE (:User {id: 2, completion_percentage: 15, gender: 'man', age: 30})")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :User(id)")
    execute_and_fetch_all(cursor, "MATCH (n:User {id: 1}), (m:User {id: 2}) CREATE (n)-[e: Friend]->(m)")
    # n and m will be in the index cache
    # edge will be created and put it into the main memory edge cache
    # we just iterate over vertices from main memory cache. -> this cache is empty
    # iterating over edges is done by using out_edges of each vertex
    assert len(execute_and_fetch_all(cursor, "MATCH (n:User {id: 1})-[e: Friend]->(m:User {id: 2}) RETURN e")) == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
