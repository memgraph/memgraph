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

import os
import sys
import time
from functools import partial
from typing import Any, Dict

import interactive_mg_runner
import mgclient
import pytest
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}


def test_ttl_replication(connection):
    # Goal: Execute TTL on MAIN and check results on REPLICA
    # 0/ Setup replication
    # 1/ MAIN Create dataset
    # 2/ MAIN Configure TTL
    # 3/ Validate that TTL is working on MAIN
    # 4/ Validate that nodes have been deleted on REPLICA as well

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
    execute_and_fetch_all(
        cursor, "UNWIND RANGE(0,100) AS d CREATE (:TTL{ttl:timestamp() + timestamp(duration({second:d}))});"
    )

    # 2/
    execute_and_fetch_all(cursor, 'EXECUTE TTL EVERY "1s";')

    # 3/
    def n_deltas(cursor):
        return execute_and_fetch_all(cursor, f"MATCH (n) RETURN count(n) < 95;")

    mg_sleep_and_assert([(True,)], partial(n_deltas, cursor))
    cursor_replica = connection(BOLT_PORTS["replica_1"], "replica").cursor()

    # 4/
    mg_sleep_and_assert([(True,)], partial(n_deltas, cursor_replica))
    cursor_replica2 = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    mg_sleep_and_assert([(True,)], partial(n_deltas, cursor_replica2))


def test_ttl_on_replica(connection):
    # Goal: Check that TTL can be configured on REPLICA,
    #       but is executed only when the instance is MAIN
    # 0/ Setup MAIN
    # 1/ MAIN Create dataset
    # 2/ MAIN Configure TTL
    # 3/ Switch MAIN to REPLICA
    # 4/ Verify that TTL is not running
    # 5/ Switch REPLICA back to MAIN
    # 6/ Verify that TTL is running

    MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL = {
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": "main.log",
        },
    }

    # 0/
    interactive_mg_runner.start_all(MEMGRAPH_INSTANCES_DESCRIPTION_MANUAL)
    cursor = connection(BOLT_PORTS["main"], "main").cursor()

    # 1/
    execute_and_fetch_all(
        cursor, "UNWIND RANGE(0,100) AS d CREATE (:TTL{ttl:timestamp() + timestamp(duration({second:d}))});"
    )

    # 2/
    execute_and_fetch_all(cursor, 'EXECUTE TTL EVERY "1s";')
    time.sleep(2)

    # 3/
    execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO REPLICA WITH PORT 10000;")

    # 4/
    current_n_replica = execute_and_fetch_all(cursor, "MATCH(n) RETURN count(n);")[0][0]
    time.sleep(3)
    assert current_n_replica < 100
    assert current_n_replica == execute_and_fetch_all(cursor, "MATCH(n) RETURN count(n);")[0][0]

    # 5/
    execute_and_fetch_all(cursor, "SET REPLICATION ROLE TO MAIN;")

    # 6/
    time.sleep(3)
    current_n_main = execute_and_fetch_all(cursor, "MATCH(n) RETURN count(n);")[0][0]
    assert current_n_main < current_n_replica


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
