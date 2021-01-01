#!/usr/bin/python3

import os
import sys
import atexit
import subprocess

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
# Append parent directory to the path.
sys.path.append(os.path.join(SCRIPT_DIR, ".."))
from memgraph import MemgraphInstanceRunner  # noqa E402

PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")
MEMGRAPH_BINARY = os.path.join(BUILD_DIR, "memgraph")
TEST_BINARY = os.path.join(
    BUILD_DIR,
    "tests",
    "e2e",
    "replication_benchmark",
    "memgraph__e2e__replication_benchmark")
MG_COUNT_NODES_QUERY = "MATCH (n) RETURN count(n);"
MG_COUNT_EDGES_QUERY = "MATCH (n)-[r]->(m) RETURN count(r);"
mg_instances = []


@atexit.register
def cleanup():
    for mg_instance in mg_instances:
        mg_instance.stop()

# TODO(gitbuda): Add config args and config queries for each instance (YAML).
#     Fields: name, type, args, config queries, validation queries, validation
#             data.


mg_replica_1 = MemgraphInstanceRunner(MEMGRAPH_BINARY)
mg_instances.append(mg_replica_1)
mg_replica_1.start(args=["--bolt-port", "7688"])
mg_replica_1.query("SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

mg_replica_2 = MemgraphInstanceRunner(MEMGRAPH_BINARY)
mg_instances.append(mg_replica_2)
mg_replica_2.start(args=["--bolt-port", "7689"])
mg_replica_2.query("SET REPLICATION ROLE TO REPLICA WITH PORT 10002")

mg_replica_3 = MemgraphInstanceRunner(MEMGRAPH_BINARY)
mg_instances.append(mg_replica_3)
mg_replica_3.start(args=["--bolt-port", "7690"])
mg_replica_3.query("SET REPLICATION ROLE TO REPLICA WITH PORT 10003")

mg_main = MemgraphInstanceRunner(MEMGRAPH_BINARY)
mg_instances.append(mg_main)
mg_main.start(args=["--main", "--bolt-port", "7687"])
mg_main.query(
    "REGISTER REPLICA replica_1 SYNC WITH TIMEOUT 0 TO '127.0.0.1:10001'")
mg_main.query("REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:10002'")
mg_main.query("REGISTER REPLICA replica_3 ASYNC TO '127.0.0.1:10003'")

subprocess.run(
    [TEST_BINARY],
    check=True,
    stderr=subprocess.STDOUT)

# TODO(gitbuda): Add validation queries for each running instance (YAML).


print(
    "Main: nodes %s, edges %s" %
    (mg_main.query(MG_COUNT_NODES_QUERY),
     mg_main.query(MG_COUNT_EDGES_QUERY)))
print(
    "Replica 1: nodes %s, edges %s" %
    (mg_replica_1.query(MG_COUNT_NODES_QUERY),
     mg_replica_1.query(MG_COUNT_EDGES_QUERY)))
print(
    "Replica 2: nodes %s, edges %s" %
    (mg_replica_2.query(MG_COUNT_NODES_QUERY),
     mg_replica_2.query(MG_COUNT_EDGES_QUERY)))
print(
    "Replica 3: nodes %s, edges %s" %
    (mg_replica_3.query(MG_COUNT_NODES_QUERY),
     mg_replica_3.query(MG_COUNT_EDGES_QUERY)))
