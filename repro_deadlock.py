#!/usr/bin/env python3
"""
Minimal reproduction script for parallel execution deadlock.

This script creates the exact conditions that trigger the deadlock:
- Parallel aggregation with exception in first element
- 2 workers to minimize complexity
"""

import os
import signal
import subprocess
import sys
import time

# Start memgraph
memgraph_cmd = [
    "./build/memgraph",
    "--bolt-port=7688",
    "--experimental-enabled=high-availability",
    "--log-level=TRACE",
]

# Query that causes deadlock
query = """
USING PARALLEL EXECUTION
UNWIND range(1, 10) AS i
CREATE (:A{p:i})
"""

query2 = """
USING PARALLEL EXECUTION
MATCH (n:A)
WHERE n.p = 1
SET n.p = 'invalid_string'
"""

query3 = """
USING PARALLEL EXECUTION
MATCH (n:A)
RETURN min(n.p)
"""


def main():
    print("Starting memgraph...")
    proc = subprocess.Popen(
        memgraph_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for memgraph to start
    time.sleep(3)

    try:
        from gqlalchemy import Memgraph

        print("Connecting to memgraph...")
        memgraph = Memgraph(host="localhost", port=7688)

        print("Creating test data...")
        memgraph.execute(query)

        print("Injecting type error...")
        memgraph.execute(query2)

        print("Running parallel aggregation that will trigger exception...")
        print("Query:", query3)
        print("This should deadlock within 10 seconds...")

        start = time.time()
        try:
            result = memgraph.execute_and_fetch(query3)
            for row in result:
                print("Result:", row)
        except Exception as e:
            print(f"Exception (expected): {e}")
            print(f"Query completed in {time.time() - start:.2f}s")
            return 0

        elapsed = time.time() - start
        if elapsed > 10:
            print(f"DEADLOCK DETECTED! Query took {elapsed:.2f}s (should fail quickly)")
            return 1
        else:
            print(f"Query completed in {elapsed:.2f}s")
            return 0

    except ImportError:
        print("gqlalchemy not installed, using raw bolt...")
        # Use raw bolt protocol or mgconsole
        return test_with_mgconsole(proc)
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        return 1
    finally:
        print("Killing memgraph...")
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except:
            proc.kill()


def test_with_mgconsole(proc):
    """Alternative test using mgconsole"""
    time.sleep(1)

    # Create a script file
    script = """
    USING PARALLEL EXECUTION
    UNWIND range(1, 10) AS i CREATE (:A{p:i});
    USING PARALLEL EXECUTION
    MATCH (n:A) WHERE n.p = 1 SET n.p = 'invalid_string';
    USING PARALLEL EXECUTION
    MATCH (n:A) RETURN min(n.p);
    """

    with open("/tmp/test_query.cypher", "w") as f:
        f.write(script)

    print("Running test via mgconsole...")
    start = time.time()
    result = subprocess.run(
        [
            "./build/mgconsole",
            "--host=127.0.0.1",
            "--port=7688",
            "--input=/tmp/test_query.cypher",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    elapsed = time.time() - start

    print("stdout:", result.stdout)
    print("stderr:", result.stderr)
    print(f"Return code: {result.returncode}, Time: {elapsed:.2f}s")

    if elapsed > 10:
        print("DEADLOCK DETECTED!")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
