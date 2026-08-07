# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.


import glob
import os
import sys
import time
from functools import partial
from multiprocessing import Pool

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path, get_vertex_count
from mg_utils import (
    mg_sleep_and_assert,
    mg_sleep_and_assert_collection,
    mg_sleep_and_assert_eval_function,
    mg_sleep_and_assert_multiple,
    mg_sleep_and_assert_until_role_change,
    wait_for_status_change,
)

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "strict_sync"


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
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


# Uses only STRICT_SYNC replicas
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


# Uses STRICT_SYNC and ASYNC replicas
def get_mixed_setup_queries():
    return [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
        "REGISTER INSTANCE instance_1 AS STRICT_SYNC WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 AS ASYNC WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 AS STRICT_SYNC WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]


def test_uc_replication(test_name):
    inner_instances_description = setup_cluster(test_name, get_default_setup_queries())
    instance3_cursor = connect(host="localhost", port=7689).cursor()
    execute_and_fetch_all(instance3_cursor, "CREATE CONSTRAINT ON (n:Node) ASSERT n.id IS UNIQUE")

    # 1. batch passes normally
    create_query = """MATCH (n:Node) WITH coalesce(max(n.id), 0) as max_idx
                    FOREACH (i in range(max_idx + 1, max_idx + 5000) | CREATE (:Node {id: i}))
                    RETURN max_idx + 5000 as id"""

    execute_and_fetch_all(instance3_cursor, create_query)
    mg_sleep_and_assert(5000, partial(get_vertex_count, instance3_cursor))

    # Instance 2
    instance2_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(5000, partial(get_vertex_count, instance2_cursor))
    # Instance 1
    instance1_cursor = connect(host="localhost", port=7687).cursor()
    mg_sleep_and_assert(5000, partial(get_vertex_count, instance1_cursor))

    # Kill instance_1 so that the next txn aborts
    interactive_mg_runner.kill(inner_instances_description, "instance_1")

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance3_cursor, create_query)
    assert "Failed to replicate to STRICT_SYNC replica" in str(e.value)

    # Start instance_1
    interactive_mg_runner.start(inner_instances_description, "instance_1")

    # Wait until the query passes
    run_until_success(instance3_cursor, create_query)

    # Everything should be normal here, validation should pass normally on instance_2


# The test tests that STRICT_SYNC replicas cannot be used together with SYNC replicas
@pytest.mark.parametrize("first_suffix, second_suffix", [("AS STRICT_SYNC", ""), ("", "AS STRICT_SYNC")])
def test_strict_sync_and_sync_forbidden(test_name, first_suffix, second_suffix):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)
    coord_cursor_3 = connect(host="localhost", port=7692).cursor()
    execute_and_fetch_all(
        coord_cursor_3,
        f"REGISTER INSTANCE instance_1 {first_suffix} WITH CONFIG {{'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'}};",
    )

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(
            coord_cursor_3,
            f"REGISTER INSTANCE instance_2 {second_suffix} WITH CONFIG {{'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'}};",
        )
    assert "Cluster cannot consists of both STRICT_SYNC and SYNC replicas" in str(e.value)


# Executes setup queries and returns cluster info
def setup_cluster(test_name, setup_queries):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)
    coord_cursor_3 = connect(host="localhost", port=7692).cursor()

    for query in setup_queries:
        execute_and_fetch_all(coord_cursor_3, query)
    return inner_instances_description


# Instance is restarted in this test
def check_if_data_preserved_after_restart(inner_instances_description, instance_name, bolt_port):
    interactive_mg_runner.kill(inner_instances_description, instance_name)
    interactive_mg_runner.start(inner_instances_description, instance_name)
    instance_cursor = connect(host="localhost", port=bolt_port).cursor()
    mg_sleep_and_assert(1, partial(get_vertex_count, instance_cursor))


# Tolerate if replica is down, it should come up
def run_until_success(cursor, query):
    start_time = time.time()
    while True:
        if time.time() - start_time >= 5:
            assert "Taking too long for replica to commit"
        try:
            execute_and_fetch_all(cursor, query)
            break
        except Exception as e:
            if "Failed to replicate to STRICT_SYNC replica" in str(e):
                time.sleep(1)
                continue
            assert "Unknown error"


# We test the behavior in which replica was 1st down: during that time, commits don't pass on MAIN
# Replica comes up, main should be able to commit
# Replica and main both go down, main restarts first => it should see a txn committed
def test_replica_down_up_works(test_name):
    inner_instances_description = setup_cluster(test_name, get_default_setup_queries())

    # Replica goes down
    interactive_mg_runner.kill(inner_instances_description, "instance_2")

    # Should abort
    instance3_cursor = connect(host="localhost", port=7689).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance3_cursor, "CREATE (n:Node)")
    assert "Failed to replicate to STRICT_SYNC replica" in str(e.value)

    # Replica comes back
    interactive_mg_runner.start(inner_instances_description, "instance_2")

    # Should commit
    run_until_success(instance3_cursor, "CREATE (n:Node)")

    # Replica and main up
    check_if_data_preserved_after_restart(inner_instances_description, "instance_3", 7689)
    check_if_data_preserved_after_restart(inner_instances_description, "instance_2", 7688)


# Tests that when all replicas are UP, 2PC should work
# After instances restart, they should still see the same data as upon committing
def test_commit_works(test_name):
    inner_instances_description = setup_cluster(test_name, get_default_setup_queries())
    # Create data on MAIN
    instance3_cursor = connect(host="localhost", port=7689).cursor()
    execute_and_fetch_all(instance3_cursor, "CREATE (n:Node)")
    res = dict(execute_and_fetch_all(instance3_cursor, "show storage info on current database"))

    def get_unreleased_delta_obj(instance_cursor):
        res = dict(execute_and_fetch_all(instance_cursor, "show storage info on current database"))
        return res["unreleased_delta_objects"]

    mg_sleep_and_assert(0, partial(get_unreleased_delta_obj, instance3_cursor))

    # Check if replicated on 1st replica
    instance_1_cursor = connect(host="localhost", port=7687).cursor()
    mg_sleep_and_assert(1, partial(get_vertex_count, instance_1_cursor))
    mg_sleep_and_assert(0, partial(get_unreleased_delta_obj, instance_1_cursor))

    # Check if replicated on 2nd replica
    instance_2_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(1, partial(get_vertex_count, instance_2_cursor))
    mg_sleep_and_assert(0, partial(get_unreleased_delta_obj, instance_2_cursor))

    check_if_data_preserved_after_restart(inner_instances_description, "instance_1", 7687)
    check_if_data_preserved_after_restart(inner_instances_description, "instance_2", 7688)
    check_if_data_preserved_after_restart(inner_instances_description, "instance_3", 7689)


def test_async_commit_works(test_name):
    inner_instances_description = setup_cluster(test_name, get_mixed_setup_queries())
    # Create data on MAIN
    instance3_cursor = connect(host="localhost", port=7689).cursor()
    execute_and_fetch_all(instance3_cursor, "CREATE (n:Node)")

    # Check if replicated on 1st replica
    instance1_cursor = connect(host="localhost", port=7687).cursor()
    mg_sleep_and_assert(1, partial(get_vertex_count, instance1_cursor))

    # Check if replicated on 2nd replica
    instance_2_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(1, partial(get_vertex_count, instance_2_cursor))

    check_if_data_preserved_after_restart(inner_instances_description, "instance_2", 7688)

    # Kill ASYNC replica and check that 2PC still works
    interactive_mg_runner.kill(inner_instances_description, "instance_2")
    execute_and_fetch_all(instance3_cursor, "CREATE (n:Node)")
    assert get_vertex_count(instance3_cursor) == 2
    assert get_vertex_count(instance1_cursor) == 2

    # Restart ASYNC replica, it should receive the data
    interactive_mg_runner.start(inner_instances_description, "instance_2")
    instance2_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(2, partial(get_vertex_count, instance2_cursor))


# One replica is down before commit starts on MAIN, hence in-memory state should be preserved and commit should fail
def test_replica_down_before_commit(test_name):
    inner_instances_description = setup_cluster(test_name, get_default_setup_queries())

    # Replica goes down
    interactive_mg_runner.kill(inner_instances_description, "instance_1")

    # Try to commit transaction on the current main
    instance3_cursor = connect(host="localhost", port=7689).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance3_cursor, "CREATE (n:Node)")
    assert "Failed to replicate to STRICT_SYNC replica" in str(e.value)
    # Commit shouldn't be visible on the current main
    mg_sleep_and_assert(0, partial(get_vertex_count, instance3_cursor))

    # Restart replica 1
    interactive_mg_runner.start(inner_instances_description, "instance_1")
    instance1_cursor = connect(host="localhost", port=7687).cursor()
    mg_sleep_and_assert(0, partial(get_vertex_count, instance1_cursor))

    # Check data on replica 2
    instance2_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(0, partial(get_vertex_count, instance2_cursor))


# One of replicas was down during the commit hence the txn will get aborted
# Test that the other replica which was alive all the time and which receive PrepareRpc
# won't contain any data after the restart.
def test_replica_after_restart_no_committed_data(test_name):
    inner_instances_description = setup_cluster(test_name, get_default_setup_queries())

    # Instance 1 dies
    interactive_mg_runner.kill(inner_instances_description, "instance_1")

    # Try to commit transaction on the current main
    instance3_cursor = connect(host="localhost", port=7689).cursor()
    with pytest.raises(Exception) as e:
        execute_and_fetch_all(instance3_cursor, "CREATE (n:Node)")
    assert "Failed to replicate to STRICT_SYNC replica" in str(e.value)

    # Data restart shouldn't change the fact that txn got aborted
    instance2_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(0, partial(get_vertex_count, instance2_cursor))
    interactive_mg_runner.kill(inner_instances_description, "instance_2")
    interactive_mg_runner.start(inner_instances_description, "instance_2")
    instance2_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(0, partial(get_vertex_count, instance2_cursor))


# Used in the function below which tests that MT works with STRICT_SYNC replicas
def task(db):
    main_cursor = connect(host="localhost", port=7689).cursor()
    execute_and_fetch_all(main_cursor, f"USE DATABASE {db};")
    get_query = lambda id_: "CREATE (n:Node {id:" + str(id_) + "});"

    for i in range(100):
        execute_and_fetch_all(main_cursor, get_query(i))


def test_mt_strict_sync_commit(test_name):
    setup_cluster(test_name, get_default_setup_queries())

    main_cursor = connect(host="localhost", port=7689).cursor()
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE DATABASE C;")

    with Pool(processes=4) as pool:
        res_a = pool.apply_async(task, ("A",))
        res_b = pool.apply_async(task, ("B",))
        res_c = pool.apply_async(task, ("C",))
        res_mg = pool.apply_async(task, ("memgraph",))
        res_a.get(timeout=5)
        res_b.get(timeout=5)
        res_c.get(timeout=5)
        res_mg.get(timeout=5)

    # A
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    assert get_vertex_count(main_cursor) == 100
    # B
    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    assert get_vertex_count(main_cursor) == 100
    # C
    execute_and_fetch_all(main_cursor, "USE DATABASE C;")
    assert get_vertex_count(main_cursor) == 100
    # memgraph
    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    assert get_vertex_count(main_cursor) == 100


def get_labeled_vertex_count(cursor, label):
    return execute_and_fetch_all(cursor, f"MATCH (n:{label}) RETURN count(n)")[0][0]


def get_main_log_path(test_name):
    # Memgraph's log sink appends a "_<YYYY-MM-DD>" date suffix to the filename, so the exact name
    # below never exists on disk -- glob for it instead.
    log_dir = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "logs", get_logs_path(file, test_name))
    matches = glob.glob(os.path.join(log_dir, "instance_3*.log"))
    assert matches, f"No instance_3 log found in {log_dir} (glob pattern: 'instance_3*.log')"
    return max(matches, key=os.path.getmtime)


def read_log_since(log_path, offset):
    with open(log_path, "r") as f:
        f.seek(offset)
        return f.read()


def log_contains_any(log_path, offset, substrings):
    content = read_log_since(log_path, offset)
    return any(substring in content for substring in substrings)


# Regression test: an AFTER COMMIT trigger must not fire when the txn aborted (STRICT_SYNC replica
# down => 2PC prepare fails); Commit() used to enqueue the trigger task unconditionally.
def test_after_commit_trigger_does_not_fire_for_aborted_txn(test_name):
    trigger_name = "audit_trigger"
    inner_instances_description = setup_cluster(test_name, get_default_setup_queries())
    main_cursor = connect(host="localhost", port=7689).cursor()

    # Trigger must be DELETE-typed: TriggerContext::AdaptForAccessor re-resolves objects by Gid post-abort
    # and prunes created_vertices_ (a CREATE trigger's body would never run, making this test vacuous),
    # but deliberately leaves deleted_vertices_ unpruned, so a DELETE trigger's body still runs.
    execute_and_fetch_all(main_cursor, "CREATE (n:Node {id: 1})")
    execute_and_fetch_all(
        main_cursor, f"CREATE TRIGGER {trigger_name} ON () DELETE AFTER COMMIT EXECUTE CREATE (:Audit)"
    )
    mg_sleep_and_assert(1, partial(get_labeled_vertex_count, main_cursor, "Node"))

    main_log_path = get_main_log_path(test_name)
    # Ignore everything logged by the healthy setup above; only look at what the aborted txn produces.
    start_offset = os.path.getsize(main_log_path)

    # This test never restarts instance_1, so its data directory must be dropped now or it poisons the next run.
    interactive_mg_runner.kill(inner_instances_description, "instance_1", keep_directories=False)

    with pytest.raises(Exception) as e:
        execute_and_fetch_all(main_cursor, "MATCH (n:Node) DETACH DELETE n")
    assert "Failed to replicate to STRICT_SYNC replica" in str(e.value)

    # The delete must not have taken effect -- this is the precondition proving we are really in the
    # aborted-transaction state, not just observing a no-op.
    assert get_labeled_vertex_count(main_cursor, "Node") == 1

    # Assert on MAIN's log, not graph data: the trigger's own CREATE (:Audit) commit also aborts under
    # STRICT_SYNC, so a MATCH (:Audit) count reads 0 with or without the fix and proves nothing. The
    # signal is the log line Commit() emits when it dispatches (unfixed) or skips (fixed) the trigger task.
    # Trigger task runs asynchronously; wait (bounded) so the test can't pass by looking too early.
    mg_sleep_and_assert(
        True,
        partial(
            log_contains_any,
            main_log_path,
            start_offset,
            ("Skipping 1 AFTER COMMIT trigger(s)", f"Trigger '{trigger_name}' replication:"),
        ),
        max_duration=10,
    )

    log_content = read_log_since(main_log_path, start_offset)
    assert f"Trigger '{trigger_name}' replication:" not in log_content
    # Kept as a separate assertion from the one above: a future wording change to one line shouldn't be
    # able to mask a regression in the other.
    assert "Skipping 1 AFTER COMMIT trigger(s)" in log_content


# Regression guard: on a healthy cluster the trigger must still fire and its write must still persist --
# suppressing it for a committed txn would be worse than the bug being fixed. Passes on the unfixed
# binary too (verified); it catches over-suppression, the test above is the one that catches the abort bug.
def test_after_commit_trigger_fires_for_committed_txn(test_name):
    setup_cluster(test_name, get_default_setup_queries())
    main_cursor = connect(host="localhost", port=7689).cursor()

    execute_and_fetch_all(main_cursor, "CREATE (n:Node {id: 1})")
    execute_and_fetch_all(main_cursor, "CREATE TRIGGER audit_trigger ON () DELETE AFTER COMMIT EXECUTE CREATE (:Audit)")
    mg_sleep_and_assert(1, partial(get_labeled_vertex_count, main_cursor, "Node"))

    execute_and_fetch_all(main_cursor, "MATCH (n:Node) DETACH DELETE n")
    mg_sleep_and_assert(0, partial(get_labeled_vertex_count, main_cursor, "Node"))

    mg_sleep_and_assert(1, partial(get_labeled_vertex_count, main_cursor, "Audit"))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
