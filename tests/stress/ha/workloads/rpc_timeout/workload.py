#!/usr/bin/env python3
"""
RPC timeout stress test.

Starts a heavy write query on MAIN, then blocks outgoing traffic from
instance_2's replication port (10002) using iptables. The query should
fail with an RPC timeout exception because the SYNC replica can no longer
respond.

Requires: iptables (runs with sudo if not root, without sudo if root).
Cluster port layout (from native deployment):
  - data_1 bolt=7687, mgmt=13011, repl=10001
  - data_2 bolt=7688, mgmt=13012, repl=10002
  - data_3 bolt=7689, mgmt=13013, repl=10003
"""
import multiprocessing
import os
import random
import shutil
import subprocess
import sys
import time
from datetime import datetime

from neo4j import GraphDatabase

# Replication port for data_2 (from deployment.sh)
REPL_PORT_DATA_2 = 10002

# MAIN instance bolt port
MAIN_BOLT_PORT = 7687

# Use sudo only if not running as root and sudo is available
_SUDO = ["sudo"] if os.geteuid() != 0 and shutil.which("sudo") else []


# Resolve iptables — may be in /usr/sbin which isn't always on PATH
_IPTABLES_PATH = shutil.which("iptables") or "/usr/sbin/iptables"


_IPTABLES = _IPTABLES_PATH

BLOCK_RULE = [
    *_SUDO,
    _IPTABLES,
    "-I",
    "OUTPUT",
    "-o",
    "lo",
    "-p",
    "tcp",
    "--sport",
    str(REPL_PORT_DATA_2),
    "-j",
    "DROP",
]
UNBLOCK_RULE = [
    *_SUDO,
    _IPTABLES,
    "-D",
    "OUTPUT",
    "-o",
    "lo",
    "-p",
    "tcp",
    "--sport",
    str(REPL_PORT_DATA_2),
    "-j",
    "DROP",
]

HEAVY_WRITE_QUERY = (
    "CREATE (n:Supernode) WITH n "
    "UNWIND range(1, 500000) AS x "
    "CREATE (n)-[:HAS {score: 1.05, str: 'ABCDEFGHIJKJFHDSKJDSKJFSDFJDSGFKJDSFKJDSHFKJDSHF'}]"
    "->(:Child {str: 'sdjghfskgshkf', id: x, flt: 1.0567, bool: true, dt: date(), dtz: localdatetime()});"
)

RPC_TIMEOUT_ERROR = "Main reached an RPC timeout"
RPC_GENERIC_SYNC_ERROR = "At least one SYNC replica has not confirmed"
RPC_GENERIC_STRICT_SYNC_ERROR = "At least one STRICT_SYNC replica has not confirmed"


def run_iptables(rule, description):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    result = subprocess.run(rule, capture_output=True, text=True)
    print(
        f"[iptables] [{timestamp}] {description}: cmd={' '.join(rule)}, rc={result.returncode}, "
        f"stdout='{result.stdout.strip()}', stderr='{result.stderr.strip()}'"
    )
    return result


def _run_heavy_query(error_queue, bolt_port):
    """Target function for subprocess. Connects directly via neo4j driver.

    We avoid ha_common here because multiprocessing.Process forks a child process
    that doesn't inherit ha_common's configuration (the resolver registered via
    STRESS_DEPLOYMENT auto-configure). Importing ha_common in the child would
    require re-configuring it, so we just use the neo4j driver directly.
    """
    driver = None
    try:
        driver = GraphDatabase.driver(f"bolt://127.0.0.1:{bolt_port}", auth=("", ""))
        with driver.session() as session:
            session.run(HEAVY_WRITE_QUERY).consume()
        error_queue.put(None)
    except Exception as e:
        error_queue.put(str(e))
    finally:
        if driver:
            driver.close()


def unblock():
    """Remove the iptables block rule (safe to call even if not applied)."""
    subprocess.run(UNBLOCK_RULE, capture_output=True, text=True)


def main():
    print("=" * 60)
    print("RPC Timeout Stress Test")
    print("=" * 60)

    try:
        # Start the heavy write query in a separate process (bypasses Python GIL,
        # so iptables subprocess can run truly in parallel with the Bolt query).
        error_queue = multiprocessing.Queue()
        query_proc = multiprocessing.Process(target=_run_heavy_query, args=(error_queue, MAIN_BOLT_PORT))
        query_proc.start()

        # Wait for the query to start executing and enter the replication phase
        sleep_time = random.randint(1, 20)
        print(f"Sleeping for {sleep_time}s")
        time.sleep(sleep_time)

        # Block outgoing communication from data_2's replication port
        result = run_iptables(BLOCK_RULE, "BLOCK")
        if result.returncode != 0:
            print(f"FATAL: Failed to apply iptables block rule: {result.stderr}")
            sys.exit(1)

        # Verify the rule is in place
        verify = subprocess.run(
            [*_SUDO, _IPTABLES, "-L", "OUTPUT", "-n", "--line-numbers"], capture_output=True, text=True
        )
        print(f"[iptables] Current OUTPUT rules:\n{verify.stdout}")

        # Wait for the query to finish (should fail with RPC timeout)
        # Budget: ~30s query execution + 30s RPC timeout + margin
        query_proc.join(timeout=180)
        if query_proc.is_alive():
            print("FATAL: Query process did not finish within the expected time")
            query_proc.terminate()
            query_proc.join(timeout=10)
            sys.exit(1)

        # Unblock communication
        run_iptables(UNBLOCK_RULE, "UNBLOCK")

        # Check the result
        if error_queue.empty():
            print("FATAL: Expected a result from the query process but got nothing")
            sys.exit(1)

        error_message = error_queue.get(timeout=5)
        if error_message is None:
            print("Replication managed to finish before network disruption")
            print("RPC Timeout Stress Test PASSED")
            sys.exit(0)

        # We currently check for both generic and specific timeout because the generic one will be thrown
        # if socket connect fails or sending data fails while timeout will occur if client waits for too
        # long for the reply from the replica
        if (
            RPC_TIMEOUT_ERROR not in error_message
            and RPC_GENERIC_SYNC_ERROR not in error_message
            and RPC_GENERIC_STRICT_SYNC_ERROR not in error_message
        ):
            print(f"FATAL: Expected '{RPC_TIMEOUT_ERROR}' in error message, got: {error_message}")
            sys.exit(1)

        print(f"\nReceived expected error: {error_message}")
        print("RPC Timeout Stress Test PASSED!")

    finally:
        # Always unblock, even on failure
        unblock()


if __name__ == "__main__":
    main()
