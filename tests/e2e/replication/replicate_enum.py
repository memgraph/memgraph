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


def test_enum_replication(connection):
    # Goal: That Enum definition is replicated to REPLICAs
    # 0/ Setup replication
    # 1/ MAIN CREATE ENUM
    # 2/ Validate Enum has arrived at REPLICA

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level=TRACE",
            ],
            "log_file": "replica1.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
            ],
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
            ],
            "log_file": "replica2.log",
            "setup_queries": [
                f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};",
            ],
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": "main.log",
            "setup_queries": [
                f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
                f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';",
            ],
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(cursor, "CREATE ENUM Location VALUES { York, Zagreb };")

    # 2/
    expected_data = [
        (
            "replica_1",
            f"127.0.0.1:{REPLICATION_PORTS['replica_1']}",
            "sync",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 2}},
        ),
        (
            "replica_2",
            f"127.0.0.1:{REPLICATION_PORTS['replica_2']}",
            "async",
            {"behind": None, "status": "ready", "ts": 0},
            {"memgraph": {"behind": 0, "status": "ready", "ts": 2}},
        ),
    ]

    mg_sleep_and_assert_collection(expected_data, show_replicas_func(cursor))

    def get_enums(cursor):
        return execute_and_fetch_all(cursor, f"SHOW ENUMS;")

    cursor_replica = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    replica_1_enums = get_enums(cursor_replica)
    assert replica_1_enums == [("Location", ["York", "Zagreb"])]

    cursor_replica2 = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    replica_2_enums = get_enums(cursor_replica2)
    assert replica_2_enums == [("Location", ["York", "Zagreb"])]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
