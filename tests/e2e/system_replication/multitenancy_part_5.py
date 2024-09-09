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
from multitenancy_common import (
    BOLT_PORTS,
    REPLICATION_PORTS,
    create_memgraph_instances_with_role_recovery,
    do_manual_setting_up,
    show_replicas_func,
)

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


def test_multitenancy_drop_while_replica_using(connection):
    # Goal: show that the replica can handle a transaction on a database being dropped (will persist until tx finishes)
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA
    # 4/ Start A transaction on replica 1, Use A on replica2
    # 5/ Check that the drop replicated
    # 6/ Validate that the transaction is still active and working and that the replica2 is not pointing to anything

    # 0/
    data_directory = tempfile.TemporaryDirectory()
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(data_directory.name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    do_manual_setting_up(connection)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")

    # 3/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 1, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 1, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(main_cursor))

    # 4/
    replica1_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    replica2_cursor = connection(BOLT_PORTS["replica_2"], "replica").cursor()

    execute_and_fetch_all(replica1_cursor, "USE DATABASE A;")
    execute_and_fetch_all(replica1_cursor, "BEGIN")
    execute_and_fetch_all(replica2_cursor, "USE DATABASE A;")

    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE A;")

    # 5/
    # TODO Remove this once there is a replica state for the system
    execute_and_fetch_all(main_cursor, "CREATE DATABASE B;")
    execute_and_fetch_all(main_cursor, "USE DATABASE B;")

    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 8, "behind": None, "status": "ready"},
            {"B": {"ts": 0, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 8, "behind": None, "status": "ready"},
            {"B": {"ts": 0, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert(expected_data, show_replicas_func(main_cursor))

    # 6/
    assert execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN count(*);")[0][0] == 1
    execute_and_fetch_all(replica1_cursor, "COMMIT")
    failed = False
    try:
        execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN n;")
    except mgclient.DatabaseError:
        failed = True
    assert failed

    failed = False
    try:
        execute_and_fetch_all(replica2_cursor, "MATCH(n) RETURN n;")
    except mgclient.DatabaseError:
        failed = True
    assert failed


def test_multitenancy_drop_and_recreate_while_replica_using(connection):
    # Goal: show that the replica can handle a transaction on a database being dropped and the same name reused
    # Original storage should persist in a nameless state until tx is over
    # needs replicating over
    # 0/ Setup replication
    # 1/ MAIN CREATE DATABASE A
    # 2/ Write to MAIN A
    # 3/ Validate replication of changes to A have arrived at REPLICA
    # 4/ Start A transaction on replica 1, Use A on replica2
    # 5/ Check that the drop/create replicated
    # 6/ Validate that the transaction is still active and working and that the replica2 is not pointing to anything

    # 0/
    data_directory = tempfile.TemporaryDirectory()
    MEMGRAPH_INSTANCES_DESCRIPTION = create_memgraph_instances_with_role_recovery(data_directory.name)
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION)

    do_manual_setting_up(connection)

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")

    # 2/
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")

    # 3/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 1, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 3, "behind": None, "status": "ready"},
            {"A": {"ts": 1, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert_collection(expected_data, show_replicas_func(main_cursor))

    # 4/
    replica1_cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    replica2_cursor = connection(BOLT_PORTS["replica_2"], "replica").cursor()

    execute_and_fetch_all(replica1_cursor, "USE DATABASE A;")
    execute_and_fetch_all(replica1_cursor, "BEGIN")
    execute_and_fetch_all(replica2_cursor, "USE DATABASE A;")

    execute_and_fetch_all(main_cursor, "USE DATABASE memgraph;")
    execute_and_fetch_all(main_cursor, "DROP DATABASE A;")

    # 5/
    execute_and_fetch_all(main_cursor, "CREATE DATABASE A;")
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")

    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"ts": 8, "behind": None, "status": "ready"},
            {"A": {"ts": 0, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"ts": 8, "behind": None, "status": "ready"},
            {"A": {"ts": 0, "behind": 0, "status": "ready"}, "memgraph": {"ts": 0, "behind": 0, "status": "ready"}},
        ),
    ]
    mg_sleep_and_assert(expected_data, show_replicas_func(main_cursor))

    # 6/
    assert execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN count(*);")[0][0] == 1
    execute_and_fetch_all(replica1_cursor, "COMMIT")
    failed = False
    try:
        execute_and_fetch_all(replica1_cursor, "MATCH(n) RETURN n;")
    except mgclient.DatabaseError:
        failed = True
    assert failed

    failed = False
    try:
        execute_and_fetch_all(replica2_cursor, "MATCH(n) RETURN n;")
    except mgclient.DatabaseError:
        failed = True
    assert failed


if __name__ == "__main__":
    interactive_mg_runner.cleanup_directories_on_exit()
    sys.exit(pytest.main([__file__, "-rA"]))
