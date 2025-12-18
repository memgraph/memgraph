#!/usr/bin/env python3
"""
Dummy workload for EKS testing.
Simply runs RETURN 1 query 100 times with 100 second sleep between each.
"""
import os
import subprocess
import time

from neo4j import GraphDatabase

# Configuration
NUM_ITERATIONS = 100
SLEEP_SECONDS = 100

# Deployment script path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEPLOYMENT_SCRIPT = os.path.join(SCRIPT_DIR, "..", "..", "deployments", "eks_ha.sh")


def wait_for_service_ip(service_name: str, timeout: int = 300) -> str:
    """Wait for a LoadBalancer service to get an external IP using eks_ha.sh."""
    result = subprocess.run(
        [DEPLOYMENT_SCRIPT, "wait-ip", service_name, str(timeout)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Failed to wait for IP for {service_name}: {result.stderr}")
    return result.stdout.strip()


def main():
    # Discover MAIN instance connection via eks_ha.sh
    print("Discovering MAIN instance IP via eks_ha.sh...")
    host = wait_for_service_ip("data-0")
    port = "7687"
    uri = f"bolt://{host}:{port}"

    print(f"Connecting to Memgraph at {uri}")
    print(f"Running RETURN 1 query {NUM_ITERATIONS} times with {SLEEP_SECONDS}s sleep between each")
    print("-" * 60)

    driver = GraphDatabase.driver(uri, auth=("", ""))

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
