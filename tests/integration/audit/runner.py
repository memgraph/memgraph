#!/usr/bin/python3 -u

# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import argparse
import atexit
import csv
import json
import os
import subprocess
import sys
import tempfile
import time

DEFAULT_DB = "memgraph"
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
SIGNAL_SIGTERM = 15

QUERIES = [
    ("MATCH (n) DELETE n", {}),
    ("MATCH (n) DETACH DELETE n", {}),
    ("CREATE (n)", {}),
    ("CREATE (n {name: $name})", {"name": True}),
    ("CREATE (n {name: $name})", {"name": 5}),
    ("CREATE (n {name: $name})", {"name": 3.14}),
    ("CREATE (n {name: $name})", {"name": "nandare"}),
    ("CREATE (n {name: $name})", {"name": ["nandare", "hai hai hai"]}),
    ("CREATE (n {name: $name})", {"name": {"test": "ho ho ho"}}),
    ("CREATE (n {name: $name})", {"name": 5, "leftover": 42}),
    ("MATCH (n), (m) CREATE (n)-[:e {when: $when}]->(m)", {"when": 42}),
    ("MATCH (n) RETURN n", {}),
    ("MATCH (n), (m {type: $type}) RETURN count(n), count(m)", {"type": "dadada"}),
    (
        "MERGE (n) ON CREATE SET n.created = timestamp() "
        "ON MATCH SET n.lastSeen = timestamp() "
        "RETURN n.name, n.created, n.lastSeen",
        {},
    ),
    ("MATCH (n {value: $value}) SET n.value = 0 RETURN n", {"value": "nandare!"}),
    ("MATCH (n), (m) SET n.value = m.value", {}),
    ("MATCH (n {test: $test}) REMOVE n.value", {"test": 48}),
    ("MATCH (n), (m) REMOVE n.value, m.value", {}),
    ("CREATE INDEX ON :User (id)", {}),
]

CREATE_DB_QUERIES = [
    ("CREATE DATABASE clean", {}),
]


def wait_for_server(port, delay=0.1):
    cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
    while subprocess.call(cmd) != 0:
        time.sleep(0.01)
    time.sleep(delay)


def gen_mt_queries(queries, db):
    out = []
    for query, params in queries:
        out.append((db, query, params))
    return out


def execute_test(memgraph_binary, tester_binary):
    storage_directory = tempfile.TemporaryDirectory()
    memgraph_args = [
        memgraph_binary,
        "--storage-properties-on-edges",
        "--data-directory",
        storage_directory.name,
        "--audit-enabled",
        "--log-file=memgraph.log",
        "--log-level=TRACE",
    ]

    # Start the memgraph binary
    memgraph = subprocess.Popen(list(map(str, memgraph_args)))
    time.sleep(0.1)
    assert memgraph.poll() is None, "Memgraph process died prematurely!"
    wait_for_server(7687)

    # Register cleanup function
    @atexit.register
    def cleanup():
        pid = memgraph.pid
        try:
            os.kill(pid, SIGNAL_SIGTERM)
        except os.OSError:
            assert False

        time.sleep(1)

    def execute_queries(queries):
        for db, query, params in queries:
            print(query, params)
            args = [tester_binary, "--query", query, "--use-db", db, "--params-json", json.dumps(params)]
            subprocess.run(args).check_returncode()

    # Test default db
    mt_queries = gen_mt_queries(QUERIES, DEFAULT_DB)

    # Execute all queries
    print("\033[1;36m~~ Starting query execution ~~\033[0m")
    execute_queries(mt_queries)
    print("\033[1;36m~~ Finished query execution ~~\033[0m\n")

    # Test new db
    print("\033[1;36m~~ Creating clean database ~~\033[0m")
    mt_queries2 = gen_mt_queries(CREATE_DB_QUERIES, DEFAULT_DB)
    execute_queries(mt_queries2)
    print("\033[1;36m~~ Finished creating clean database ~~\033[0m\n")

    # Execute all queries on clean database
    mt_queries3 = gen_mt_queries(QUERIES, "clean")
    print("\033[1;36m~~ Starting query execution on clean database ~~\033[0m")
    execute_queries(mt_queries3)
    print("\033[1;36m~~ Finished query execution on clean database ~~\033[0m\n")

    pid = memgraph.pid
    try:
        os.kill(pid, SIGNAL_SIGTERM)
    except os.OSError:
        assert False
    time.sleep(1)

    # Verify the written log
    print("\033[1;36m~~ Starting log verification ~~\033[0m")
    with open(os.path.join(storage_directory.name, "audit", "audit.log")) as f:
        reader = csv.reader(
            f,
            delimiter=",",
            doublequote=False,
            escapechar="\\",
            lineterminator="\n",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            skipinitialspace=False,
            strict=True,
        )
        queries = []
        for line in reader:
            timestamp, address, username, database, query, params = line
            params = json.loads(params)
            if query.startswith("USE DATABASE"):
                continue  # Skip all databases switching queries
            queries.append((database, query, params))
            print(database, query, params)

        # Combine all queries executed
        all_queries = mt_queries
        all_queries += mt_queries2
        all_queries += mt_queries3
        assert queries == all_queries, "Logged queries don't match " "executed queries!"
    print("\033[1;36m~~ Finished log verification ~~\033[0m\n")


if __name__ == "__main__":
    memgraph_binary = os.path.join(PROJECT_DIR, "build", "memgraph")
    tester_binary = os.path.join(PROJECT_DIR, "build", "tests", "integration", "audit", "tester")

    parser = argparse.ArgumentParser()
    parser.add_argument("--memgraph", default=memgraph_binary)
    parser.add_argument("--tester", default=tester_binary)
    args = parser.parse_args()

    execute_test(args.memgraph, args.tester)

    sys.exit(0)
