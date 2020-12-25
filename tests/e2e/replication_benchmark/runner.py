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
mg_instances = []


@atexit.register
def cleanup():
    for mg_instance in mg_instances:
        mg_instance.stop()


mg_replica_1 = MemgraphInstanceRunner(MEMGRAPH_BINARY)
mg_instances.append(mg_replica_1)
mg_replica_1.start(args=["--bolt-port", "7688"])
mg_replica_1.query("SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")

mg_replica_2 = MemgraphInstanceRunner(MEMGRAPH_BINARY)
mg_instances.append(mg_replica_2)
mg_replica_2.start(args=["--bolt-port", "7689"])
mg_replica_2.query("SET REPLICATION ROLE TO REPLICA WITH PORT 10002")

mg_main = MemgraphInstanceRunner(MEMGRAPH_BINARY)
mg_instances.append(mg_main)
mg_main.start(args=["--main", "--bolt-port", "7687"])
mg_main.query(
    "REGISTER REPLICA replica_1 SYNC WITH TIMEOUT 0 TO '127.0.0.1:10001'")
mg_main.query("REGISTER REPLICA replica_2 ASYNC TO '127.0.0.1:10002'")

subprocess.run([TEST_BINARY], check=True)

print("Main: %s" % mg_main.query("MATCH (n) RETURN count(n)"))
print("Replica 1: %s" % mg_replica_1.query("MATCH (n) RETURN count(n)"))
print("Replica 2: %s" % mg_replica_2.query("MATCH (n) RETURN count(n)"))
