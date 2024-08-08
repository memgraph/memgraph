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

import atexit
import os
import shutil
import sys
import tempfile
import time
from functools import partial
from typing import Any, Dict

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


BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}


def show_replicas_func(cursor):
    def func():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    return func


def test_periodic_commit_replication(connection):
    # Goal: That periodic commit query is replicated to REPLICAs
    # 0/ Setup replication
    # 1/ MAIN query: USING PERIODIC COMMIT 1 UNWIND range(1, 3) AS x CREATE (:A {id: x})"
    # 2/ Validate data has arrived at REPLICA
    # 3/ Validate you can't periodically commit to replica

    # 0/
    cursor = connection(7687, "main").cursor()

    # 1/
    execute_and_fetch_all(cursor, "USING PERIODIC COMMIT 1 UNWIND range(1, 3) AS x CREATE (:A {id: x})")

    # 2/
    expected_data = [
        (
            "replica_1",
            "127.0.0.1:10001",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 6}},
        ),
        (
            "replica_2",
            "127.0.0.1:10002",
            "async",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 6}},
        ),
    ]

    mg_sleep_and_assert_collection(expected_data, show_replicas_func(cursor))

    def get_periodic_commit_data(cursor):
        return execute_and_fetch_all(cursor, f"MATCH (n) RETURN n.id AS id;")

    cursor_replica = connection(7688, "replica").cursor()
    replica_1_data = get_periodic_commit_data(cursor_replica)
    assert replica_1_data == [(1,), (2,), (3,)]

    cursor_replica_2 = connection(7689, "replica").cursor()
    replica_2_data = get_periodic_commit_data(cursor_replica_2)
    assert replica_2_data == [(1,), (2,), (3,)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
