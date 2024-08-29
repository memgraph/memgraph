# Copyright 2024 Memgraph Ltd.
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
import tempfile

import interactive_mg_runner
import mgclient
import pytest
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert, mg_sleep_and_assert_collection

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

MEMGRAPH_INSTANCES_DESCRIPTION = {
    "replica_1": {
        "args": ["--bolt-port", "7688", "--log-level=TRACE"],
        "log_file": "replica1.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
    },
    "replica_2": {
        "args": ["--bolt-port", "7689", "--log-level=TRACE"],
        "log_file": "replica2.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
    },
    "replica_3": {
        "args": ["--bolt-port", "7690", "--log-level=TRACE"],
        "log_file": "replica3.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10003;"],
    },
    "replica_4": {
        "args": ["--bolt-port", "7691", "--log-level=TRACE"],
        "log_file": "replica4.log",
        "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10004;"],
    },
    "main": {
        "args": ["--bolt-port", "7687", "--log-level=TRACE"],
        "log_file": "main.log",
        "setup_queries": [
            "REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:10001';",
            "REGISTER REPLICA replica_2 SYNC TO '127.0.0.1:10002';",
            "REGISTER REPLICA replica_3 ASYNC TO '127.0.0.1:10003';",
            "REGISTER REPLICA replica_4 ASYNC TO '127.0.0.1:10004';",
        ],
    },
}


def test_attempt_to_create_indexes_on_main_when_sync_replica_is_down(connection):
    # Goal of this test is to check creation of new indexes/constraints when a sync replica is down.
    # 0/ Start main and sync replicas.
    # 1/ Check status of replicas.
    # 2/ Add some indexes to main and check it is propagated to the sync_replicas.
    # 3/ Kill a sync replica.
    # 4/ Add some more indexes to main. It should be added to main and replica2
    # 5/ Check the status of replicas.
    # 6/ Restart the replica that was killed and check that it is up to date with main.

    data_directory = tempfile.TemporaryDirectory()
    CONFIGURATION = {
        "sync_replica1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "sync_replica1.log",
            "setup_queries": [],
            "data_directory": f"{data_directory.name}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": "sync_replica2.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": "main.log",
            # Need to do it manually
            "setup_queries": [],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION)

    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, as we are setting replica manually because
    # of restart. Restart on replica would set role again.
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

    # 1/
    expected_data = [
        (
            "sync_replica1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "sync_replica2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
    assert all([x in actual_data for x in expected_data])

    # 2/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE INDEX ON :Number(value);")

    QUERY_TO_CHECK = "SHOW INDEX INFO;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 1
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 3/
    interactive_mg_runner.kill(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "invalid"),
        ("sync_replica2", "sync", 0, "ready"),
    ]

    def retrieve_data():
        replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
        return [
            (replica_name, mode, info["memgraph"]["behind"], info["memgraph"]["status"])
            for replica_name, ip, mode, sys_info, info in replicas
        ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 4/
    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("CREATE INDEX ON :Number(value2);")

    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 5/
    expected_data = [
        (
            "sync_replica1",
            "127.0.0.1:10001",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 0, "behind": 0, "status": "invalid"}},
        ),
        (
            "sync_replica2",
            "127.0.0.1:10002",
            "sync",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 6, "behind": 0, "status": "ready"}},
        ),
    ]
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)
    actual_data = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
    assert all([x in actual_data for x in expected_data])

    # 6/
    interactive_mg_runner.start(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "ready"),
        ("sync_replica2", "sync", 0, "ready"),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)


def test_trigger_on_create_before_commit_with_offline_sync_replica(connection):
    # 0/ Start all.
    # 1/ Create the trigger
    # 2/ Create a node. We expect two nodes created (our Not_Magic and the Magic created by trigger).
    # 3/ Check the nodes
    # 4/ We remove all nodes.
    # 5/ Kill a replica and check that it's offline.
    # 6/ Create new node.
    # 7/ Check that we have two nodes.
    # 8/ Re-start the replica and check it's online and that it has two nodes.

    data_directory = tempfile.TemporaryDirectory()
    CONFIGURATION = {
        "sync_replica1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "sync_replica1.log",
            # Need to do it manually since we kill this replica
            "setup_queries": [],
            "data_directory": f"{data_directory.name}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": "sync_replica2.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": "main.log",
            # Need to do it manually since we kill replica
            "setup_queries": [],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION)

    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, as we are setting replica manually because
    # of restart. Restart on replica would set role again.
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

    # 1/
    QUERY_CREATE_TRIGGER = """
        CREATE TRIGGER exampleTrigger
        ON CREATE BEFORE COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger'});
    """
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW TRIGGERS;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    # 2/
    QUERY_CREATE_NODE = "CREATE (p:Number {name:'Not_Magic'})"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 3/
    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 4/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (n) DETACH DELETE n;")

    # 5/
    interactive_mg_runner.kill(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "invalid"),
        ("sync_replica2", "sync", 0, "ready"),
    ]

    def retrieve_data():
        replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
        return [
            (replica_name, mode, info["memgraph"]["behind"], info["memgraph"]["status"])
            for replica_name, ip, mode, sys_info, info in replicas
        ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 6/
    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 7/
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 8/
    interactive_mg_runner.start(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "ready"),
        ("sync_replica2", "sync", 0, "ready"),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)


def test_trigger_on_update_before_commit_with_offline_sync_replica(connection):
    # 0/ Start all.
    # 1/ Create the trigger
    # 2/ Create a node.
    # 3/ Update the node: we expect another node to be created
    # 4/ Check the nodes
    # 5/ We remove all nodes and create new node again.
    # 6/ Kill a replica and check that it's offline.
    # 7/ Update the node.
    # 8/ Check that we have two nodes.
    # 9/ Re-start the replica and check it's online and that it has two nodes.

    data_directory = tempfile.TemporaryDirectory()
    CONFIGURATION = {
        "sync_replica1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "sync_replica1.log",
            # Need to do it manually
            "setup_queries": [],
            "data_directory": f"{data_directory.name}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": "sync_replica2.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": "main.log",
            "setup_queries": [],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION)

    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, as we are setting replica manually because
    # of restart. Restart on replica would set role again.
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

    # 1/
    QUERY_CREATE_TRIGGER = """
        CREATE TRIGGER exampleTrigger
        ON UPDATE BEFORE COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger'});
    """
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW TRIGGERS;")
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"

    # 2/
    QUERY_CREATE_NODE = "CREATE (p:Number {name:'Not_Magic', value:0})"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 3/
    QUERY_TO_UPDATE = "MATCH (node:Number {name:'Not_Magic'}) SET node.value=1 return node;"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_UPDATE)

    # 4/
    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 5/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (n) DETACH DELETE n;")
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 6/
    interactive_mg_runner.kill(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "invalid"),
        ("sync_replica2", "sync", 0, "ready"),
    ]

    def retrieve_data():
        replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
        return [
            (replica_name, mode, info["memgraph"]["behind"], info["memgraph"]["status"])
            for replica_name, ip, mode, sys_info, info in replicas
        ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 7/
    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_UPDATE)

    # 8/
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 9/
    interactive_mg_runner.start(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "ready"),
        ("sync_replica2", "sync", 0, "ready"),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 2
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)


def test_trigger_on_delete_before_commit_with_offline_sync_replica(connection):
    # 0/ Start all.
    # 1/ Create the trigger
    # 2/ Create a node.
    # 3/ Delete the node: we expect another node to be created
    # 4/ Check that we have one node.
    # 5/ We remove all triggers and all nodes and create new trigger and node again.
    # 6/ Kill a replica and check that it's offline.
    # 7/ Delete the node.
    # 8/ Check that we have one node.
    # 9/ Re-start the replica and check it's online and that it has one node, and the correct one.

    data_directory = tempfile.TemporaryDirectory()
    CONFIGURATION = {
        "sync_replica1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "sync_replica1.log",
            # we need to set it manually
            "setup_queries": [],
            "data_directory": f"{data_directory.name}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": "sync_replica2.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": "main.log",
            "setup_queries": [],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION)

    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, as we are setting replica manually because
    # of restart. Restart on replica would set role again.
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

    # 1/
    QUERY_CREATE_TRIGGER = """
        CREATE TRIGGER exampleTrigger
        ON DELETE BEFORE COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger'});
    """
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW TRIGGERS;")
    assert len(res_from_main) == 1, f"Incorrect result: {res_from_main}"

    # 2/
    QUERY_CREATE_NODE = "CREATE (p:Number {name:'Not_Magic', value:0})"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 3/
    QUERY_TO_DELETE = "MATCH (n) DETACH DELETE n;"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_DELETE)

    # 4/
    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 1, f"Incorrect result: {res_from_main}"
    assert res_from_main[0][0].properties["name"] == "Node_created_by_trigger"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 5/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("DROP TRIGGER exampleTrigger;")
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (n) DETACH DELETE n;")
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER)
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 6/
    interactive_mg_runner.kill(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "invalid"),
        ("sync_replica2", "sync", 0, "ready"),
    ]

    def retrieve_data():
        replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
        return [
            (replica_name, mode, info["memgraph"]["behind"], info["memgraph"]["status"])
            for replica_name, ip, mode, sys_info, info in replicas
        ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 7/
    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_DELETE)

    # 8/
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 1, f"Incorect result: {res_from_main}"
    assert res_from_main[0][0].properties["name"] == "Node_created_by_trigger"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 9/
    interactive_mg_runner.start(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "ready"),
        ("sync_replica2", "sync", 0, "ready"),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 1
    assert res_from_main[0][0].properties["name"] == "Node_created_by_trigger"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)


def test_trigger_on_create_before_and_after_commit_with_offline_sync_replica(connection):
    # 0/ Start all.
    # 1/ Create the triggers
    # 2/ Create a node. We expect three nodes created (1 node created + the two created by triggers).
    # 3/ Check the nodes
    # 4/ We remove all nodes.
    # 5/ Kill a replica and check that it's offline.
    # 6/ Create new node.
    # 7/ Check that we have three nodes.
    # 8/ Re-start the replica and check it's online and that it has three nodes.

    data_directory = tempfile.TemporaryDirectory()
    CONFIGURATION = {
        "sync_replica1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "sync_replica1.log",
            # we need to set it manually
            "setup_queries": [],
            "data_directory": f"{data_directory.name}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": "sync_replica2.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": "main.log",
            "setup_queries": [],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION)

    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, as we are setting replica manually because
    # of restart. Restart on replica would set role again.
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

    # 1/
    QUERY_CREATE_TRIGGER_BEFORE = """
        CREATE TRIGGER exampleTriggerBefore
        ON CREATE BEFORE COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger_before'});
    """
    QUERY_CREATE_TRIGGER_AFTER = """
        CREATE TRIGGER exampleTriggerAfter
        ON CREATE AFTER COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger_after'});
    """
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER_BEFORE)
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER_AFTER)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW TRIGGERS;")
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"

    # 2/
    QUERY_CREATE_NODE = "CREATE (p:Number {name:'Not_Magic'})"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 3/
    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 3, f"Incorect result: {res_from_main}"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 4/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (n) DETACH DELETE n;")

    # 5/
    interactive_mg_runner.kill(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "invalid"),
        ("sync_replica2", "sync", 0, "ready"),
    ]

    def retrieve_data():
        replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
        return [
            (replica_name, mode, info["memgraph"]["behind"], info["memgraph"]["status"])
            for replica_name, ip, mode, sys_info, info in replicas
        ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 6/
    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 7/
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 3
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 8/
    interactive_mg_runner.start(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "ready"),
        ("sync_replica2", "sync", 0, "ready"),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 3
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)


def test_triggers_on_create_before_commit_with_offline_sync_replica(connection):
    # 0/ Start all.
    # 1/ Create the two triggers
    # 2/ Create a node. We expect three nodes.
    # 3/ Check the nodes
    # 4/ We remove all nodes.
    # 5/ Kill a replica and check that it's offline.
    # 6/ Create new node.
    # 7/ Check that we have three nodes.
    # 8/ Re-start the replica and check it's online and that it has two nodes.

    data_directory = tempfile.TemporaryDirectory()
    CONFIGURATION = {
        "sync_replica1": {
            "args": [
                "--bolt-port",
                "7688",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "sync_replica1.log",
            # we need to set it manually
            "setup_queries": [],
            "data_directory": f"{data_directory.name}/sync_replica1",
        },
        "sync_replica2": {
            "args": ["--bolt-port", "7689", "--log-level=TRACE"],
            "log_file": "sync_replica2.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10002;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": "main.log",
            "setup_queries": [],
        },
    }

    # 0/
    interactive_mg_runner.start_all(CONFIGURATION)

    sync_replica1_cursor = connection(7688, "sync_replica1").cursor()
    execute_and_fetch_all(sync_replica1_cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

    cursor = connection(7687, "main").cursor()

    # We want to execute manually and not via the configuration, as we are setting replica manually because
    # of restart. Restart on replica would set role again.
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica1 SYNC TO '127.0.0.1:10001';")
    execute_and_fetch_all(cursor, "REGISTER REPLICA sync_replica2 SYNC TO '127.0.0.1:10002';")

    # 1/
    QUERY_CREATE_TRIGGER_FIRST = """
        CREATE TRIGGER exampleTriggerFirst
        ON CREATE BEFORE COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger_first'});
    """
    QUERY_CREATE_TRIGGER_SECOND = """
        CREATE TRIGGER exampleTriggerSecond
        ON CREATE BEFORE COMMIT EXECUTE
        CREATE (p:Number {name:'Node_created_by_trigger_second'});
    """
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER_FIRST)
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_TRIGGER_SECOND)
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW TRIGGERS;")
    assert len(res_from_main) == 2, f"Incorect result: {res_from_main}"

    # 2/
    QUERY_CREATE_NODE = "CREATE (p:Number {name:'Not_Magic'})"
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 3/
    QUERY_TO_CHECK = "MATCH (node) return node;"
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 3, f"Incorect result: {res_from_main}"
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 4/
    interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("MATCH (n) DETACH DELETE n;")

    # 5/
    interactive_mg_runner.kill(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "invalid"),
        ("sync_replica2", "sync", 0, "ready"),
    ]

    def retrieve_data():
        replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
        return [
            (replica_name, mode, info["memgraph"]["behind"], info["memgraph"]["status"])
            for replica_name, ip, mode, sys_info, info in replicas
        ]

    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 6/
    with pytest.raises(mgclient.DatabaseError):
        interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_CREATE_NODE)

    # 7/
    def get_number_of_nodes():
        return len(interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK))

    mg_sleep_and_assert(3, get_number_of_nodes)

    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 3
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)

    # 8/
    interactive_mg_runner.start(CONFIGURATION, "sync_replica1")
    expected_data = [
        ("sync_replica1", "sync", 0, "ready"),
        ("sync_replica2", "sync", 0, "ready"),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])
    res_from_main = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query(QUERY_TO_CHECK)
    assert len(res_from_main) == 3
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica1"].query(QUERY_TO_CHECK)
    assert res_from_main == interactive_mg_runner.MEMGRAPH_INSTANCES["sync_replica2"].query(QUERY_TO_CHECK)


def test_replication_not_messed_up_by_CreateSnapshot(connection):
    # Goal of this test is to check the replica can not run CreateSnapshot
    # 1/ CREATE SNAPSHOT should raise a DatabaseError

    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    cursor = connection(7688, "replica_1").cursor()

    # 1/
    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "CREATE SNAPSHOT;")


def test_replication_not_messed_up_by_ShowIndexInfo(connection):
    # Goal of this test is to check the replicas timestamp and hence ability to recieve MAINs writes
    # is uneffected by SHOW INDEX INFO

    # 1/ Run SHOW INDEX INFO; multiple times on REPLICA
    # 2/ Send a write from MAIN
    # 3/ Check REPLICA processed the write

    BASIC_MEMGRAPH_INSTANCES_DESCRIPTION = {
        "replica_1": {
            "args": ["--bolt-port", "7688", "--log-level=TRACE"],
            "log_file": "replica1.log",
            "setup_queries": ["SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"],
        },
        "main": {
            "args": ["--bolt-port", "7687", "--log-level=TRACE"],
            "log_file": "main.log",
            "setup_queries": [
                "REGISTER REPLICA replica_1 ASYNC TO '127.0.0.1:10001';",
            ],
        },
    }

    interactive_mg_runner.start_all(BASIC_MEMGRAPH_INSTANCES_DESCRIPTION)

    cursor = connection(7688, "replica_1").cursor()

    # 1/
    # This query use to incorrectly change REPLICA storage timestamp
    # run this multiple times to try and get into error case of MAIN timestamp < REPLICA timestamp
    for _ in range(20):
        execute_and_fetch_all(cursor, "SHOW INDEX INFO;")

    cursor = connection(7687, "main").cursor()

    # 2/
    execute_and_fetch_all(cursor, "CREATE ();")

    def retrieve_data():
        replicas = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].query("SHOW REPLICAS;")
        return replicas

    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "async",
            {"ts": 0, "behind": None, "status": "ready"},
            {"memgraph": {"ts": 2, "behind": 0, "status": "ready"}},
        ),
    ]
    actual_data = mg_sleep_and_assert_collection(expected_data, retrieve_data)
    assert all([x in actual_data for x in expected_data])

    # 3/
    cursor = connection(7688, "replica_1").cursor()
    result = execute_and_fetch_all(cursor, "MATCH () RETURN count(*);")

    assert len(result) == 1
    assert result[0][0] == 1  # The one node was replicated from MAIN to REPLICA


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
