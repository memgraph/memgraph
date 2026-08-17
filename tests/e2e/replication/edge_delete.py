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
from common import connect, execute_and_fetch_all
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_collection

REPLICA_BOLT_PORTS = [7688, 7689]


def replica_cursors():
    return [connect(host="localhost", port=port).cursor() for port in REPLICA_BOLT_PORTS]


def assert_on_replicas(cursors, query, expected):
    """Waits for every replica to converge on `expected` for `query`."""
    for cursor in cursors:
        mg_sleep_and_assert(expected, lambda cursor=cursor: execute_and_fetch_all(cursor, query))


# BUGFIX: for issue https://github.com/memgraph/memgraph/issues/1515
def test_replication_handles_delete_when_multiple_edges_of_same_type(connection, pytestconfig):
    # Goal is to check the timestamp are correctly computed from the information we get from replicas.
    # 0/ Check original state of replicas.
    # 1/ Add nodes and edges to MAIN, then delete the edges.
    # 2/ Check state of replicas.

    with_props = pytestconfig.getoption("with_props")
    print("Running with properties: ", with_props)

    # 0/
    conn = connection(7687, "main")
    conn.autocommit = True
    cursor = conn.cursor()
    actual_data = execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            "127.0.0.1:10002",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    assert all([x in actual_data for x in expected_data])

    # 1/
    if with_props:
        execute_and_fetch_all(
            cursor, "CREATE (a)-[r:X{p:1, str:'1234567890'}]->(b) CREATE (a)-[:X{p:2, str:'1234567890'}]->(b) DELETE r;"
        )
    else:
        execute_and_fetch_all(cursor, "CREATE (a)-[r:X]->(b) CREATE (a)-[:X]->(b) DELETE r;")

    # 2/
    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            "127.0.0.1:10002",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
    ]

    def retrieve_data():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])


def test_replication_deletes_all_edges_of_a_dense_vertex(connection):
    # Replicas apply a transaction's edge deletions as one batch. A single hub whose whole fan-out is deleted in
    # one transaction is the case that batch exists for, so assert the replicas end up with the hub and its
    # leaves intact and no edges left.
    conn = connection(7687, "main")
    cursor = conn.cursor()
    replicas = replica_cursors()

    execute_and_fetch_all(cursor, "CREATE (:Hub {id: 0});")
    execute_and_fetch_all(cursor, "MATCH (h:Hub) UNWIND range(1, 500) AS i CREATE (h)-[:X]->(:Leaf {id: i});")
    execute_and_fetch_all(cursor, "MATCH (:Hub)-[r:X]->(:Leaf) DELETE r;")

    assert_on_replicas(replicas, "MATCH ()-[r]->() RETURN count(r);", [(0,)])
    assert_on_replicas(replicas, "MATCH (n) RETURN count(n);", [(501,)])


def test_replication_deletes_edges_and_their_vertices_in_one_transaction(connection):
    # DETACH DELETE emits the edge deletions and the vertex deletions in the same transaction. Deleting a vertex
    # requires it to already be detached, so this covers the batch being flushed before the vertex deltas run.
    conn = connection(7687, "main")
    cursor = conn.cursor()
    replicas = replica_cursors()

    execute_and_fetch_all(cursor, "CREATE (:Hub {id: 0});")
    execute_and_fetch_all(cursor, "MATCH (h:Hub) UNWIND range(1, 300) AS i CREATE (h)-[:X]->(:Leaf {id: i});")
    execute_and_fetch_all(cursor, "MATCH (n:Hub) DETACH DELETE n;")

    assert_on_replicas(replicas, "MATCH ()-[r]->() RETURN count(r);", [(0,)])
    assert_on_replicas(replicas, "MATCH (n:Hub) RETURN count(n);", [(0,)])
    assert_on_replicas(replicas, "MATCH (n:Leaf) RETURN count(n);", [(300,)])


def test_replication_deletes_a_subset_of_parallel_edges_and_self_loops(connection, pytestconfig):
    # Batched deletion erases edges from an endpoint's adjacency by gid. Parallel edges between one pair and
    # self-loops are where a gid-blind implementation would take out the wrong edge, so delete half of each and
    # assert the replicas keep exactly the intended survivors.
    with_props = pytestconfig.getoption("with_props")

    conn = connection(7687, "main")
    cursor = conn.cursor()
    replicas = replica_cursors()

    edge_props = " {i: i}" if with_props else ""
    execute_and_fetch_all(cursor, "CREATE (:A {id: 1}), (:B {id: 2});")
    execute_and_fetch_all(cursor, f"MATCH (a:A), (b:B) UNWIND range(1, 100) AS i CREATE (a)-[:PAR{edge_props}]->(b);")
    execute_and_fetch_all(cursor, f"MATCH (a:A) UNWIND range(1, 100) AS i CREATE (a)-[:LOOP{edge_props}]->(a);")

    if with_props:
        execute_and_fetch_all(cursor, "MATCH ()-[r:PAR]->() WHERE r.i % 2 = 0 DELETE r;")
        execute_and_fetch_all(cursor, "MATCH ()-[r:LOOP]->() WHERE r.i % 2 = 0 DELETE r;")
    else:
        execute_and_fetch_all(cursor, "MATCH ()-[r:PAR]->() WITH r LIMIT 50 DELETE r;")
        execute_and_fetch_all(cursor, "MATCH ()-[r:LOOP]->() WITH r LIMIT 50 DELETE r;")

    assert_on_replicas(replicas, "MATCH ()-[r:PAR]->() RETURN count(r);", [(50,)])
    assert_on_replicas(replicas, "MATCH ()-[r:LOOP]->() RETURN count(r);", [(50,)])

    if with_props:
        # Only with edge properties is an individual edge identifiable, which is what pins down that the
        # surviving edges are the ones that were meant to survive rather than just the right count.
        expected_survivors = [(i,) for i in range(1, 100, 2)]
        assert_on_replicas(replicas, "MATCH ()-[r:PAR]->() RETURN r.i ORDER BY r.i;", expected_survivors)
        assert_on_replicas(replicas, "MATCH ()-[r:LOOP]->() RETURN r.i ORDER BY r.i;", expected_survivors)


def test_replication_sets_properties_and_deletes_edges_in_one_transaction(connection, pytestconfig):
    # Edge property writes and edge deletions land in the same transaction here. The property writes must still
    # be visible on the surviving edges once the deletion batch has been applied.
    if not pytestconfig.getoption("with_props"):
        pytest.skip("Requires properties on edges")

    conn = connection(7687, "main")
    cursor = conn.cursor()
    replicas = replica_cursors()

    execute_and_fetch_all(cursor, "CREATE (:Hub {id: 0});")
    execute_and_fetch_all(cursor, "MATCH (h:Hub) UNWIND range(1, 200) AS i CREATE (h)-[:X {i: i}]->(:Leaf {id: i});")
    execute_and_fetch_all(cursor, "MATCH ()-[r:X]->() SET r.tag = r.i * 10 WITH r WHERE r.i > 50 DELETE r;")

    assert_on_replicas(replicas, "MATCH ()-[r:X]->() RETURN count(r);", [(50,)])
    assert_on_replicas(replicas, "MATCH ()-[r:X]->() RETURN sum(r.tag);", [(sum(i * 10 for i in range(1, 51)),)])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"] + sys.argv[1:]))
