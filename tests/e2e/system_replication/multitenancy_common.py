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


import tempfile
from typing import Any, Dict

from common import execute_and_fetch_all

BOLT_PORTS = {"main": 7687, "replica_1": 7688, "replica_2": 7689}
REPLICATION_PORTS = {"replica_1": 10001, "replica_2": 10002}

TEMP_DIR = tempfile.TemporaryDirectory().name


def set_eq(actual, expected):
    return len(actual) == len(expected) and all([x in actual for x in expected])


def create_memgraph_instances_with_role_recovery(data_directory: Any) -> Dict[str, Any]:
    return {
        "replica_1": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_1']}",
                "--log-level",
                "TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "replica1.log",
            "data_directory": f"{data_directory}/replica_1",
        },
        "replica_2": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['replica_2']}",
                "--log-level=TRACE",
                "--replication-restore-state-on-startup",
                "true",
                "--data-recovery-on-startup",
                "false",
            ],
            "log_file": "replica2.log",
            "data_directory": f"{data_directory}/replica_2",
        },
        "main": {
            "args": [
                "--bolt-port",
                f"{BOLT_PORTS['main']}",
                "--log-level=TRACE",
            ],
            "log_file": "main.log",
            "setup_queries": [],
        },
    }


def do_manual_setting_up(connection):
    replica_1_cursor = connection(BOLT_PORTS["replica_1"], "replica_1").cursor()
    execute_and_fetch_all(
        replica_1_cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};"
    )

    replica_2_cursor = connection(BOLT_PORTS["replica_2"], "replica_2").cursor()
    execute_and_fetch_all(
        replica_2_cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};"
    )

    main_cursor = connection(BOLT_PORTS["main"], "main").cursor()

    execute_and_fetch_all(
        main_cursor, f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';"
    )
    execute_and_fetch_all(
        main_cursor, f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';"
    )


MEMGRAPH_INSTANCES_DESCRIPTION_WITH_RECOVERY = {
    "replica_1": {
        "args": [
            "--bolt-port",
            f"{BOLT_PORTS['replica_1']}",
            "--log-level=TRACE",
            "--replication-restore-state-on-startup",
            "--data-recovery-on-startup",
        ],
        "log_file": "replica1.log",
        "data_directory": TEMP_DIR + "/replica1",
    },
    "replica_2": {
        "args": [
            "--bolt-port",
            f"{BOLT_PORTS['replica_2']}",
            "--log-level=TRACE",
            "--replication-restore-state-on-startup",
            "--data-recovery-on-startup",
        ],
        "log_file": "replica2.log",
        "data_directory": TEMP_DIR + "/replica2",
    },
    "main": {
        "args": [
            "--bolt-port",
            f"{BOLT_PORTS['main']}",
            "--log-level=TRACE",
            "--replication-restore-state-on-startup",
            "--data-recovery-on-startup",
        ],
        "log_file": "main.log",
        "data_directory": TEMP_DIR + "/main",
    },
}


def safe_execute(function, *args):
    try:
        function(*args)
    except:
        pass


def setup_replication(connection):
    # Setup replica1
    cursor = connection(BOLT_PORTS["replica_1"], "replica").cursor()
    execute_and_fetch_all(cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};")
    # Setup replica2
    cursor = connection(BOLT_PORTS["replica_2"], "replica").cursor()
    execute_and_fetch_all(cursor, f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_2']};")
    # Setup main
    cursor = connection(BOLT_PORTS["main"], "main").cursor()
    execute_and_fetch_all(cursor, f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';")
    execute_and_fetch_all(cursor, f"REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_2']}';")


def setup_main(main_cursor):
    execute_and_fetch_all(main_cursor, "USE DATABASE A;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'A'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")
    execute_and_fetch_all(main_cursor, "CREATE (:Node)-[:EDGE]->(:Node)")

    execute_and_fetch_all(main_cursor, "USE DATABASE B;")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")
    execute_and_fetch_all(main_cursor, "CREATE (:Node{on:'B'});")


def show_replicas_func(cursor):
    def func():
        return execute_and_fetch_all(cursor, "SHOW REPLICAS;")

    return func


def show_databases_func(cursor):
    def func():
        return execute_and_fetch_all(cursor, "SHOW DATABASES;")

    return func


def get_number_of_nodes_func(cursor, db_name):
    def func():
        execute_and_fetch_all(cursor, f"USE DATABASE {db_name};")
        return execute_and_fetch_all(cursor, "MATCH (n) RETURN count(*);")[0][0]

    return func


def get_number_of_edges_func(cursor, db_name):
    def func():
        execute_and_fetch_all(cursor, f"USE DATABASE {db_name};")
        return execute_and_fetch_all(cursor, "MATCH ()-[r]->() RETURN count(*);")[0][0]

    return func
