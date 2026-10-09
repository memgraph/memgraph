# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import os
import sys

import interactive_mg_runner
import pytest
from common import execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

BOLT_PORTS = {"main": 7687, "replica": 7688}
REPLICATION_PORTS = {"replica": 10001}
NARROWING = "--experimental-enabled=commit-lock-narrowing"
file = "replicate_mixed_narrowing"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def cluster(test_name, main_narrowing: bool, replica_narrowing: bool):
    return {
        "replica": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica']}",
                "--log-level=TRACE",
                *([NARROWING] if replica_narrowing else []),
            ],
            "log_file": f"{get_logs_path(file, test_name)}/replica.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
                *([NARROWING] if main_narrowing else []),
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
            "setup_queries": [
                f"REGISTER REPLICA replica SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica']}';",
            ],
        },
    }


def assert_narrowing_is(cursor, expected: bool, who: str):
    """The premise of this test. Without it a flag that silently failed to apply would leave both
    parameterisations running the same configuration, and the comparison below would prove nothing."""
    rows = execute_and_fetch_all(cursor, "SHOW CONFIG;")
    experimental = [r for r in rows if r[0] == "experimental_enabled"]
    assert experimental, "no experimental_enabled row in SHOW CONFIG, so the flag cannot be confirmed"
    value = str(experimental[0][2] or "")
    enabled = "commit-lock-narrowing" in value
    assert enabled == expected, f"{who} should have narrowing {'on' if expected else 'off'}, SHOW CONFIG says {value!r}"


def write_a_graph(cursor):
    """Several committed transactions, so the replica applies a stream rather than one batch."""
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Node(id);")
    for i in range(50):
        execute_and_fetch_all(cursor, f"CREATE (:Node {{id: {i}, name: 'n{i}'}});")
    execute_and_fetch_all(
        cursor,
        "MATCH (a:Node), (b:Node) WHERE a.id = b.id - 1 CREATE (a)-[:NEXT {w: a.id}]->(b);",
    )
    execute_and_fetch_all(cursor, "MATCH (n:Node) WHERE n.id % 10 = 0 SET n.tagged = true;")
    execute_and_fetch_all(cursor, "MATCH (n:Node) WHERE n.id % 17 = 0 DETACH DELETE n;")


def graph_contents(cursor):
    """Everything the replica must agree with the main about, read through Cypher."""
    vertices = execute_and_fetch_all(cursor, "MATCH (n:Node) RETURN n.id, n.name, n.tagged ORDER BY n.id;")
    edges = execute_and_fetch_all(cursor, "MATCH (a:Node)-[e:NEXT]->(b:Node) RETURN a.id, e.w, b.id ORDER BY a.id;")
    return vertices, edges


@pytest.mark.parametrize(
    "main_narrowing,replica_narrowing",
    [(True, False), (False, True)],
    ids=["main_on_replica_off", "main_off_replica_on"],
)
def test_mixed_narrowing_replicates_identically(connection, test_name, main_narrowing, replica_narrowing):
    """The flag is per-instance and startup-only, so a cluster can run with it set on some members and
    not others. What a member commits must not depend on that: a replica has to end up with the graph
    its main has, whichever side is narrowing."""
    interactive_mg_runner.start_all(cluster(test_name, main_narrowing, replica_narrowing), keep_directories=False)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    replica_cursor = connection(BOLT_PORTS["replica"], "replica").cursor()
    assert_narrowing_is(main_cursor, main_narrowing, "main")
    assert_narrowing_is(replica_cursor, replica_narrowing, "replica")

    write_a_graph(main_cursor)

    expected_vertices, expected_edges = graph_contents(main_cursor)
    assert len(expected_vertices) > 0, "the writes did not land on main, so the comparison is vacuous"
    assert len(expected_edges) > 0, "no edges on main, so the comparison would not cover them"

    mg_sleep_and_assert(
        (expected_vertices, expected_edges),
        lambda: graph_contents(replica_cursor),
    )


@pytest.mark.parametrize(
    "main_narrowing,replica_narrowing",
    [(True, False), (False, True)],
    ids=["main_on_replica_off", "main_off_replica_on"],
)
def test_promoted_replica_keeps_the_graph(connection, test_name, main_narrowing, replica_narrowing):
    """Promotion raises the storage clock past the last durable timestamp, and under narrowing it must
    carry the boundary a transaction freezes its snapshot from along with it. A promoted instance has to
    still see everything it replicated, and accept writes on top of it."""
    interactive_mg_runner.start_all(cluster(test_name, main_narrowing, replica_narrowing), keep_directories=False)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()
    replica_cursor = connection(BOLT_PORTS["replica"], "replica").cursor()
    assert_narrowing_is(replica_cursor, replica_narrowing, "replica")

    write_a_graph(main_cursor)
    expected = graph_contents(main_cursor)
    mg_sleep_and_assert(expected, lambda: graph_contents(replica_cursor))

    # The replica becomes a main, which is where the clock jumps.
    execute_and_fetch_all(replica_cursor, "SET REPLICATION ROLE TO MAIN;")

    # The first read after promotion is the one whose snapshot boundary the jump could strand.
    assert graph_contents(replica_cursor) == expected, "promotion lost sight of replicated data"

    execute_and_fetch_all(replica_cursor, "CREATE (:Node {id: 1000, name: 'after'});")
    after = execute_and_fetch_all(replica_cursor, "MATCH (n:Node {id: 1000}) RETURN n.name;")
    assert after == [("after",)], "a write on the promoted instance was not visible to a later read"
