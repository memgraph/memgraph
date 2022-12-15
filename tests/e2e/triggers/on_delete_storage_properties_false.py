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


@pytest.mark.parametrize("ba_commit", ["BEFORE COMMIT", "AFTER COMMIT"])
def test_create_on_delete_before_commit(ba_commit):
    """
    Args:
        ba_commit (str): BEFORE OR AFTER commit
    """
    cursor = connect().cursor()
    QUERY_TRIGGER_CREATE = f"""
        CREATE TRIGGER DeleteTriggerEdgesCount
        ON --> DELETE
        {ba_commit}
        EXECUTE
        CREATE (n:DeletedEdge {{count: size(deletedEdges)}})
    """
    create_trigger_res = execute_and_fetch_all(cursor, QUERY_TRIGGER_CREATE)
    print(create_trigger_res)

    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 1})")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 2})")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 3})")
    execute_and_fetch_all(cursor, "CREATE (n:Node {id: 4})")
    res = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN n")
    assert len(res) == 4
    res2 = execute_and_fetch_all(cursor, "MATCH (n:DeletedEdge) RETURN n")
    assert len(res2) == 0
    QUERY_CREATE_EDGE = """
        MATCH (n:Node {id: 1}), (m:Node {id: 2})
        CREATE (n)-[r:TYPE]->(m);
    """
    execute_and_fetch_all(cursor, QUERY_CREATE_EDGE)
    # QUERY_RETURN_EDGE = """
    #     MATCH ()-[r:TYPE]->()
    #     RETURN r;
    # """
    # print(execute_and_fetch_all(cursor, QUERY_RETURN_EDGE))
    QUERY_DELETE_EDGE = """
        MATCH ()-[r:TYPE]->()
        DELETE r;
    """
    execute_and_fetch_all(cursor, QUERY_DELETE_EDGE)
    # See if trigger was triggered
    nodes = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN n")
    assert len(res) == 4
    deleted_edges = execute_and_fetch_all(cursor, "MATCH (n:DeletedEdge) RETURN n")
    assert len(deleted_edges) == 1
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    execute_and_fetch_all(cursor, "DROP TRIGGER DeleteTriggerEdgesCount")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
