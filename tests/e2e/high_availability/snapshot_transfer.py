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

# A replica that was down while the main kept writing and then took a snapshot is recovered by the main
# sending it that snapshot over RPC: with a snapshot retention count of 1, the snapshot removes the main's
# earlier WAL files, so WAL files alone can no longer bring the replica up to date. The replica's disk is
# full for the first transfers (simulated by tests/e2e/no_space_preload.so, which fails writes to snapshot
# files with ENOSPC while its trigger file exists): the replica must survive them, and the main's retry must
# succeed once there is space again.

import glob
import os
import shutil
import sys
from functools import partial

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path, show_instances
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "snapshot_transfer"
PRELOAD_LIBRARY = os.path.normpath(os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "no_space_preload.so"))

INITIAL_NODES = 1000
WRITES_WHILE_REPLICA_DOWN = 20
NODES_PER_WRITE = 100


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_instances():
    interactive_mg_runner.kill_all()
    yield
    interactive_mg_runner.kill_all()


def data_instance(name: str, bolt_port: int, management_port: int, test_name: str):
    return {
        "args": [
            "--bolt-port",
            str(bolt_port),
            "--log-level",
            "TRACE",
            "--management-port",
            str(management_port),
            "--replication-restore-state-on-startup=true",
            "--data-recovery-on-startup=true",
            # Every write fills a WAL file, and with a single retained snapshot creating one deletes the WAL
            # files it covers, so the main is left without the WAL files the stopped replica would need. With
            # a higher retention count the main keeps every WAL file until enough snapshots accumulate.
            "--storage-wal-file-size-kib=1",
            "--storage-snapshot-retention-count=1",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/{name}.log",
        "data_directory": f"{get_data_path(file, test_name)}/{name}",
        "setup_queries": [],
    }


def coordinator(coordinator_id: int, test_name: str, setup_queries=None):
    return {
        "args": [
            "--bolt-port",
            str(7689 + coordinator_id),
            "--log-level=TRACE",
            f"--coordinator-id={coordinator_id}",
            f"--coordinator-port={10110 + coordinator_id}",
            "--coordinator-hostname=localhost",
            f"--management-port={10120 + coordinator_id}",
        ],
        "log_file": f"{get_logs_path(file, test_name)}/coordinator_{coordinator_id}.log",
        "data_directory": f"{get_data_path(file, test_name)}/coordinator_{coordinator_id}",
        "setup_queries": setup_queries or [],
    }


def get_memgraph_instances_description(test_name: str):
    return {
        "instance_1": data_instance("instance_1", 7688, 10011, test_name),
        "instance_2": data_instance("instance_2", 7689, 10012, test_name),
        "instance_3": data_instance("instance_3", 7687, 10013, test_name),
        "coordinator_1": coordinator(1, test_name),
        "coordinator_2": coordinator(2, test_name),
        "coordinator_3": coordinator(
            3,
            test_name,
            [
                "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
                "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
                "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
                "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
                "SET INSTANCE instance_3 TO MAIN",
            ],
        ),
    }


def vertex_count(cursor):
    return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")[0][0]


def log_contains(log_file: str, *fragments: str):
    # The log is a daily file, so its name carries the date: instance_1.log is written as instance_1_<date>.log.
    stem, _ = os.path.splitext(os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "logs", log_file))
    for path in glob.glob(f"{glob.escape(stem)}*.log"):
        with open(path) as f:
            if any(all(fragment in line for fragment in fragments) for line in f):
                return True
    return False


def test_replica_recovers_from_snapshot_sent_by_main(test_name, monkeypatch):
    assert os.path.isfile(PRELOAD_LIBRARY), f"Missing {PRELOAD_LIBRARY}"
    instances = get_memgraph_instances_description(test_name)
    data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(file, test_name))
    shutil.rmtree(data_directory, ignore_errors=True)

    interactive_mg_runner.start_all(instances, keep_directories=False)

    coord_cursor = connect(host="localhost", port=7692).cursor()
    expected_cluster = [
        ("coordinator_1", "localhost:7690", "localhost:10111", "localhost:10121", "up", "follower"),
        ("coordinator_2", "localhost:7691", "localhost:10112", "localhost:10122", "up", "follower"),
        ("coordinator_3", "localhost:7692", "localhost:10113", "localhost:10123", "up", "leader"),
        ("instance_1", "localhost:7688", "", "localhost:10011", "up", "replica"),
        ("instance_2", "localhost:7689", "", "localhost:10012", "up", "replica"),
        ("instance_3", "localhost:7687", "", "localhost:10013", "up", "main"),
    ]
    mg_sleep_and_assert(expected_cluster, partial(show_instances, coord_cursor))

    main_cursor = connect(host="localhost", port=7687).cursor()
    execute_and_fetch_all(main_cursor, f"UNWIND range(1, {INITIAL_NODES}) AS i CREATE (:Node {{id: i}})")
    mg_sleep_and_assert(INITIAL_NODES, partial(vertex_count, connect(host="localhost", port=7688).cursor()))

    interactive_mg_runner.kill(instances, "instance_1", keep_directories=True)

    for _ in range(WRITES_WHILE_REPLICA_DOWN):
        execute_and_fetch_all(main_cursor, f"UNWIND range(1, {NODES_PER_WRITE}) AS i CREATE (:Late {{id: i}})")
    snapshot_path = execute_and_fetch_all(main_cursor, "CREATE SNAPSHOT")[0][0]
    snapshot_name = os.path.basename(snapshot_path)
    expected_nodes = INITIAL_NODES + WRITES_WHILE_REPLICA_DOWN * NODES_PER_WRITE

    # The trigger lives outside the data directory, which the replica cleans on startup.
    trigger = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", f"{file}_{test_name}_trigger")
    open(trigger, "w").close()
    monkeypatch.setenv("LD_PRELOAD", PRELOAD_LIBRARY)
    monkeypatch.setenv("MG_TEST_NO_SPACE_TRIGGER", trigger)
    try:
        interactive_mg_runner.start(instances, "instance_1")
        monkeypatch.delenv("LD_PRELOAD")

        # Receiving the snapshot runs out of space: the transfer is dropped instead of crashing the replica.
        mg_sleep_and_assert(
            True,
            partial(log_contains, instances["instance_1"]["log_file"], "Couldn't write received file", snapshot_name),
        )
        assert interactive_mg_runner.MEMGRAPH_INSTANCES["instance_1"].is_running()
        assert vertex_count(connect(host="localhost", port=7688).cursor()) == INITIAL_NODES
    finally:
        os.remove(trigger)

    # With space again, the main's next attempt delivers the snapshot.
    mg_sleep_and_assert(expected_nodes, partial(vertex_count, connect(host="localhost", port=7688).cursor()))

    # The data reached the replica through the snapshot, not through WAL files.
    assert log_contains(
        instances["instance_3"]["log_file"], "Sending the latest snapshot file", snapshot_name, "instance_1"
    )
    assert log_contains(instances["instance_1"]["log_file"], "Snapshot from", snapshot_name, "loaded successfully")
    replica_data_directory = os.path.join(data_directory, "instance_1")
    assert os.path.isfile(os.path.join(replica_data_directory, "snapshots", snapshot_name))
    received_dir = os.path.join(replica_data_directory, "tmp", "snapshots")
    assert not os.path.isdir(received_dir) or not os.listdir(received_dir)

    # The replica keeps replicating normally afterwards.
    execute_and_fetch_all(main_cursor, "CREATE (:AfterRecovery)")
    mg_sleep_and_assert(expected_nodes + 1, partial(vertex_count, connect(host="localhost", port=7688).cursor()))
    mg_sleep_and_assert(expected_nodes + 1, partial(vertex_count, connect(host="localhost", port=7689).cursor()))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
