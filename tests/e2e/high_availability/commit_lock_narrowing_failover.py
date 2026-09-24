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

"""
commit_lock_narrowing_failover.py — e2e assertions for PR #4685 (commit-lock narrowing, flag-ON).

Property under test:
  Under commit-lock narrowing a transaction that becomes client-visible on MAIN (COMMIT returned
  success to the client) must survive a MAIN failover; a transaction that never finalised (2PC
  preparation failed because a STRICT_SYNC replica was unreachable) must never appear on any
  instance, even after the surviving replica is promoted to MAIN.

Honest scope limitation:
  No mid-2PC crash injection hook exists in the binary; we therefore assert the two observable
  cluster endpoints, not the microscopic Prepare->Finalize gap.  That narrower invariant is
  unit-test territory.  The assertions here are valid regardless — if the commit-lock narrowing
  introduced a lost-update or a phantom the visible vertex count would diverge from expectation.
"""

import os
import sys
from functools import partial

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path, get_vertex_count, show_instances
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_eval_function, mg_sleep_and_assert_until_role_change

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "commit_lock_narrowing_failover"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


@pytest.fixture
def test_name(request):
    return request.node.name


def get_instances_description_no_setup(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_3",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--management-port=10121",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--bolt-port",
                "7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--management-port=10122",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--bolt-port",
                "7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--management-port=10123",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [],
        },
    }


def get_default_setup_queries():
    return [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
        "REGISTER INSTANCE instance_1 AS STRICT_SYNC WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 AS STRICT_SYNC WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 AS STRICT_SYNC WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]


def setup_cluster(test_name, setup_queries):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)
    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    for query in setup_queries:
        execute_and_fetch_all(coord_cursor_3, query)
    return inner_instances_description


def _find_promoted_instance(instances_rows, killed_instance):
    """Return (name, bolt_port) of the data instance now reported as MAIN, excluding the killed one.

    SHOW INSTANCES rows (after ignore_elapsed_time_from_results strip) are 6-tuples:
      (name, bolt_server, coordinator_server, management_server, health_status, role)
    Data instances have an empty coordinator_server; coordinators have a non-empty one.
    """
    for row in instances_rows:
        name, bolt_server, coordinator_server, _, health, role = row
        if role == "main" and health == "up" and coordinator_server == "" and name != killed_instance:
            port = int(bolt_server.split(":")[1])
            return name, port
    return None, None


def test_committed_txn_survives_strict_sync_failover(test_name):
    """Endpoint A — no lost read.

    A transaction whose COMMIT succeeded on a STRICT_SYNC cluster (all replicas finalised) must
    remain visible after the MAIN is hard-killed and a replica is promoted.  Under commit-lock
    narrowing the row is written to MAIN storage before the commit-lock is released; the replica
    finalise happens inside the lock, so the row is durable on both sides when the client sees
    success.  A lost-update bug would surface here as vertex_count == 0 on the new MAIN.
    """
    inner_instances_description = setup_cluster(test_name, get_default_setup_queries())

    main_cursor = connect(host="localhost", port=7689).cursor()
    execute_and_fetch_all(main_cursor, "CREATE (n:Node {id: 1})")
    # Under STRICT_SYNC, a successful COMMIT means every strict-sync replica has finalised the txn.
    mg_sleep_and_assert(1, partial(get_vertex_count, main_cursor))

    # Hard-kill the MAIN; the coordinator must elect a new one from instance_1 / instance_2.
    # keep_directories=False because instance_3 is never restarted in this test.
    interactive_mg_runner.kill(inner_instances_description, "instance_3", keep_directories=False)

    coord_cursor = connect(host="localhost", port=7692).cursor()

    # Wait until the coordinator reports a data instance (not instance_3) that is "up" and "main".
    # Row layout after ignore_elapsed_time_from_results: [name, bolt_server, coord_server, mgmt_server, health, role].
    # Data instances have coord_server == ""; coordinators have a non-empty coord_server.
    elected_rows = mg_sleep_and_assert_eval_function(
        lambda rows: any(r[5] == "main" and r[4] == "up" and r[2] == "" and r[0] != "instance_3" for r in rows),
        partial(show_instances, coord_cursor),
    )

    new_main_name, new_main_port = _find_promoted_instance(elected_rows, "instance_3")
    assert new_main_port is not None, "Coordinator did not elect a new MAIN after instance_3 was killed"

    # Confirm the promoted instance sees itself as main before querying it.
    new_main_cursor = connect(host="localhost", port=new_main_port).cursor()
    mg_sleep_and_assert_until_role_change(
        lambda: execute_and_fetch_all(new_main_cursor, "SHOW REPLICATION ROLE;")[0][0],
        "main",
    )

    # The committed node must be present on the new MAIN.
    mg_sleep_and_assert(1, partial(get_vertex_count, new_main_cursor))


def test_unfinalized_txn_absent_after_failover(test_name):
    """Endpoint B — no phantom / presumed-abort.

    A transaction that failed to replicate to a STRICT_SYNC replica (2PC prepare aborted) must
    never become visible, even after the surviving replica is promoted to MAIN.  Under commit-lock
    narrowing, a txn that did not finalise on all strict-sync replicas is rolled back and its
    storage deltas are discarded via DoToMainPromotion -> DestroyReplAccessor.  A phantom bug
    would surface here as vertex_count > 0 on the new MAIN.

    Note: the assertion holds whether or not instance_2 received a PrepareRpc before instance_1
    was detected as unreachable.  In both cases the prepared state is discarded on promotion.
    """
    inner_instances_description = setup_cluster(test_name, get_default_setup_queries())

    # Kill one STRICT_SYNC replica; the next COMMIT on MAIN cannot reach all strict-sync replicas.
    # keep_directories=False because instance_1 is never restarted in this test.
    interactive_mg_runner.kill(inner_instances_description, "instance_1", keep_directories=False)

    main_cursor = connect(host="localhost", port=7689).cursor()
    with pytest.raises(Exception) as exc_info:
        execute_and_fetch_all(main_cursor, "CREATE (n:Node {id: 2})")
    assert "Failed to replicate to STRICT_SYNC replica" in str(exc_info.value)

    # The aborted txn must leave no trace on the current MAIN.
    mg_sleep_and_assert(0, partial(get_vertex_count, main_cursor))

    # Now also kill the MAIN; only instance_2 (port 7688) remains and will be promoted.
    # keep_directories=False because instance_3 is never restarted in this test.
    interactive_mg_runner.kill(inner_instances_description, "instance_3", keep_directories=False)

    coord_cursor = connect(host="localhost", port=7692).cursor()

    # Wait until the coordinator promotes instance_2 to MAIN.
    mg_sleep_and_assert_eval_function(
        lambda rows: any(r[0] == "instance_2" and r[5] == "main" and r[4] == "up" for r in rows),
        partial(show_instances, coord_cursor),
    )

    instance2_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert_until_role_change(
        lambda: execute_and_fetch_all(instance2_cursor, "SHOW REPLICATION ROLE;")[0][0],
        "main",
    )

    # The never-finalised txn must be absent on the promoted instance.
    mg_sleep_and_assert(0, partial(get_vertex_count, instance2_cursor))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
