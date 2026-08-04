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

# Bulk import on an HA data instance: switch the single MAIN to IN_MEMORY_ANALYTICAL, import, switch
# back (which writes the snapshot), then register the replicas again. See specs/ha-analytical-import.md.

import os
import sys
from functools import partial

import interactive_mg_runner
import mgclient
import pytest
from common import (
    connect,
    execute_and_fetch_all,
    get_data_path,
    get_logs_path,
    get_vertex_count,
    show_instances,
    show_replicas,
)
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "analytical_import"

REGISTER_INSTANCE_2 = (
    "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', "
    "'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};"
)

CLUSTER_WITH_REPLICA = [
    ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
    ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
    ("instance_2", "localhost:7688", "", "localhost:10012", "up", "replica"),
]

CLUSTER_WITHOUT_REPLICA = [
    ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "leader"),
    ("instance_1", "localhost:7687", "", "localhost:10011", "up", "main"),
]


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def get_instances_description(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=true",
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
                "--replication-restore-state-on-startup=true",
                "--data-recovery-on-startup=true",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--coordinator-hostname=localhost",
                "--management-port=10121",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [
                "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
                REGISTER_INSTANCE_2,
                "SET INSTANCE instance_1 TO MAIN",
            ],
        },
    }


def start_cluster(test_name: str):
    """Starts a coordinator, a MAIN (instance_1) and one replica (instance_2) and returns their cursors."""
    instances = get_instances_description(test_name)
    interactive_mg_runner.start_all(instances, keep_directories=False)

    coord_cursor = connect(host="localhost", port=7690).cursor()
    mg_sleep_and_assert(CLUSTER_WITH_REPLICA, partial(show_instances, coord_cursor))

    return instances, coord_cursor, connect(host="localhost", port=7687).cursor()


def import_while_analytical(main_cursor, count: int):
    """The target workload: analytical for the duration of the import, transactional again afterwards.

    No manual CREATE SNAPSHOT in between -- the switch back writes one that is stamped with a
    post-import timestamp, whereas a manual snapshot taken while analytical carries a stale one.
    """
    execute_and_fetch_all(main_cursor, "STORAGE MODE IN_MEMORY_ANALYTICAL")
    execute_and_fetch_all(main_cursor, f"UNWIND RANGE(1, {count}) AS i CREATE (:Imported {{id: i}});")
    execute_and_fetch_all(main_cursor, "STORAGE MODE IN_MEMORY_TRANSACTIONAL")


def test_analytical_import_with_empty_replica(test_name):
    """Scenario A: nothing was ingested anywhere before the switch.

    An empty replica reports timestamp 0 and already took the snapshot path before decision 9, so this
    is not the guard for that fix -- its job is to prove the gates work end to end and that the
    recovery override does not regress the empty case.
    """
    instances, coord_cursor, main_cursor = start_cluster(test_name)

    # Decision 1: entering analytical requires zero registered replicas, instance-wide.
    execute_and_fetch_all(coord_cursor, "UNREGISTER INSTANCE instance_2")
    mg_sleep_and_assert(CLUSTER_WITHOUT_REPLICA, partial(show_instances, coord_cursor))

    import_while_analytical(main_cursor, 100)
    assert get_vertex_count(main_cursor) == 100

    execute_and_fetch_all(coord_cursor, REGISTER_INSTANCE_2)
    mg_sleep_and_assert(CLUSTER_WITH_REPLICA, partial(show_instances, coord_cursor))

    replica_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(100, partial(get_vertex_count, replica_cursor))


def test_analytical_import_with_stale_replica(test_name):
    """Scenario B: the replica already holds data and is left strictly behind before the switch.

    Unregistering never wipes the replica, so this is the natural path through the workload rather than
    a corner case. The extra writes after the unregister are mandatory: they leave the replica strictly
    behind the end of the finalized WAL chain, which is the only layout in which WAL-only recovery is
    chosen and the import would be lost. Without them the snapshot is shipped by accident and the test
    would pass even with the fix removed.
    """
    instances, coord_cursor, main_cursor = start_cluster(test_name)
    replica_cursor = connect(host="localhost", port=7688).cursor()

    execute_and_fetch_all(main_cursor, "UNWIND RANGE(1, 10) AS i CREATE (:Before {id: i});")
    mg_sleep_and_assert(10, partial(get_vertex_count, replica_cursor))

    execute_and_fetch_all(coord_cursor, "UNREGISTER INSTANCE instance_2")
    mg_sleep_and_assert(CLUSTER_WITHOUT_REPLICA, partial(show_instances, coord_cursor))

    # Leaves the replica behind the end of the WAL chain while still inside its range.
    execute_and_fetch_all(main_cursor, "UNWIND RANGE(1, 5) AS i CREATE (:AfterUnregister {id: i});")

    import_while_analytical(main_cursor, 100)
    assert get_vertex_count(main_cursor) == 115

    # Re-registered without wiping its data directory: recovery has to notice that the snapshot holds
    # data no WAL file does and ship the snapshot.
    execute_and_fetch_all(coord_cursor, REGISTER_INSTANCE_2)
    mg_sleep_and_assert(CLUSTER_WITH_REPLICA, partial(show_instances, coord_cursor))

    replica_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(115, partial(get_vertex_count, replica_cursor))
    assert get_vertex_count(replica_cursor) == get_vertex_count(main_cursor)


def test_analytical_import_with_stale_replica_and_main_restart(test_name):
    """Scenario B2: scenario B with the main restarted between the import and the re-registration.

    This pins the requirement that ruled out every design keyed on in-memory state: the recovery
    decision has to be derivable from the durability files alone.
    """
    instances, coord_cursor, main_cursor = start_cluster(test_name)
    replica_cursor = connect(host="localhost", port=7688).cursor()

    execute_and_fetch_all(main_cursor, "UNWIND RANGE(1, 10) AS i CREATE (:Before {id: i});")
    mg_sleep_and_assert(10, partial(get_vertex_count, replica_cursor))

    execute_and_fetch_all(coord_cursor, "UNREGISTER INSTANCE instance_2")
    mg_sleep_and_assert(CLUSTER_WITHOUT_REPLICA, partial(show_instances, coord_cursor))

    execute_and_fetch_all(main_cursor, "UNWIND RANGE(1, 5) AS i CREATE (:AfterUnregister {id: i});")
    import_while_analytical(main_cursor, 100)

    interactive_mg_runner.kill(instances, "instance_1")
    interactive_mg_runner.start(instances, "instance_1")
    mg_sleep_and_assert(CLUSTER_WITHOUT_REPLICA, partial(show_instances, coord_cursor))

    main_cursor = connect(host="localhost", port=7687).cursor()
    mg_sleep_and_assert(115, partial(get_vertex_count, main_cursor))

    execute_and_fetch_all(coord_cursor, REGISTER_INSTANCE_2)
    mg_sleep_and_assert(CLUSTER_WITH_REPLICA, partial(show_instances, coord_cursor))

    replica_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(115, partial(get_vertex_count, replica_cursor))
    assert get_vertex_count(replica_cursor) == get_vertex_count(main_cursor)


def test_register_instance_while_main_is_analytical(test_name):
    """Scenario C: REGISTER INSTANCE lands while the main is analytical.

    The Raft commit is the success criterion, so the query still returns success; the main rejects the
    registration RPC without mutating any replication state, and the coordinator's reconciliation loop
    heals the cluster once the main is transactional again.
    """
    instances, coord_cursor, main_cursor = start_cluster(test_name)

    execute_and_fetch_all(coord_cursor, "UNREGISTER INSTANCE instance_2")
    mg_sleep_and_assert(CLUSTER_WITHOUT_REPLICA, partial(show_instances, coord_cursor))

    execute_and_fetch_all(main_cursor, "STORAGE MODE IN_MEMORY_ANALYTICAL")
    execute_and_fetch_all(main_cursor, "UNWIND RANGE(1, 100) AS i CREATE (:Imported {id: i});")

    # Succeeds on the coordinator ...
    execute_and_fetch_all(coord_cursor, REGISTER_INSTANCE_2)
    # ... but nothing is attached on the main, and the rejection left no half-registered client behind.
    assert show_replicas(main_cursor) == []

    execute_and_fetch_all(main_cursor, "STORAGE MODE IN_MEMORY_TRANSACTIONAL")

    # The reconciliation loop retries the registration RPC on every ping.
    mg_sleep_and_assert(1, lambda: len(show_replicas(main_cursor)))
    mg_sleep_and_assert(CLUSTER_WITH_REPLICA, partial(show_instances, coord_cursor))

    replica_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(100, partial(get_vertex_count, replica_cursor))


def test_cannot_enter_analytical_with_registered_replica(test_name):
    """Decision 1's gate is operator-facing: the error names what blocks the switch."""
    instances, coord_cursor, main_cursor = start_cluster(test_name)

    with pytest.raises(mgclient.DatabaseError) as e:
        execute_and_fetch_all(main_cursor, "STORAGE MODE IN_MEMORY_ANALYTICAL")
    assert "Cannot switch to analytical mode while replicas are registered" in str(e.value)
    assert "instance_2" in str(e.value)

    # The rejected switch left the instance untouched.
    execute_and_fetch_all(main_cursor, "CREATE (:StillTransactional)")
    replica_cursor = connect(host="localhost", port=7688).cursor()
    mg_sleep_and_assert(1, partial(get_vertex_count, replica_cursor))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
