#!/usr/bin/env python3
"""
Sample workload script that intentionally fails.
Used to test error handling in the stress test framework.
"""
import sys

from neo4j import GraphDatabase

# Configure connection here or via environment variables
HOST = "127.0.0.1"
PORT = 7687


def main():
    uri = f"bolt://{HOST}:{PORT}"
    print(f"Connecting to Memgraph at {uri}")

    driver = GraphDatabase.driver(uri, auth=("", ""))

    try:
        with driver.session() as session:
            # Execute a valid query first
            print("Executing valid query...")
            session.run("CREATE (:FailTestNode {id: 1})")

            # Now execute an invalid query that will fail
            print("Executing invalid query (this should fail)...")
            session.run("THIS IS NOT VALID CYPHER SYNTAX!!!")

            # This line should never be reached
            print("This should not be printed")

    except Exception as e:
        print(f"Error occurred: {e}")
        driver.close()
        sys.exit(1)

    finally:
        driver.close()


if __name__ == "__main__":
    main()
