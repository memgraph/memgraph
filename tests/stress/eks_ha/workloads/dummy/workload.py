#!/usr/bin/env python3
"""
Dummy workload for EKS testing.
Simply runs RETURN 1 query 100 times with 100 second sleep between each.
"""
import os
import sys
import time

# Add parent directory to path for common imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import create_driver, wait_for_service_ip

# Configuration
NUM_ITERATIONS = 100
SLEEP_SECONDS = 100


def main():
    # Discover MAIN instance connection via deployment.sh
    print("Discovering MAIN instance IP via deployment.sh...")
    host = wait_for_service_ip("data-0")
    port = "7687"
    uri = f"bolt://{host}:{port}"

    print(f"Connecting to Memgraph at {uri}")
    print(f"Running RETURN 1 query {NUM_ITERATIONS} times with {SLEEP_SECONDS}s sleep between each")
    print("-" * 60)

    driver = create_driver(uri)

    try:
        for i in range(NUM_ITERATIONS):
            print(f"Iteration {i + 1}/{NUM_ITERATIONS}: Running RETURN 1...")
            with driver.session() as session:
                result = session.run("RETURN 1 AS value")
                value = result.single()["value"]
                print(f"  Result: {value}")

            if i < NUM_ITERATIONS - 1:
                print(f"  Sleeping for {SLEEP_SECONDS} seconds...")
                time.sleep(SLEEP_SECONDS)
    finally:
        driver.close()

    print("-" * 60)
    print(f"Completed {NUM_ITERATIONS} iterations!")


if __name__ == "__main__":
    main()
