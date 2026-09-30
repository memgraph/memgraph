#!/usr/bin/env python3
"""
Sample workload script that executes queries using the Neo4j driver.
"""
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
            # Create some nodes
            print("Creating nodes...")
            session.run("CREATE (:TestNode {id: 1, name: 'Node A'})")
            session.run("CREATE (:TestNode {id: 2, name: 'Node B'})")
            session.run("CREATE (:TestNode {id: 3, name: 'Node C'})")

            # Create relationships
            print("Creating relationships...")
            session.run(
                """
                MATCH (a:TestNode {id: 1}), (b:TestNode {id: 2})
                CREATE (a)-[:CONNECTED_TO]->(b)
            """
            )
            session.run(
                """
                MATCH (b:TestNode {id: 2}), (c:TestNode {id: 3})
                CREATE (b)-[:CONNECTED_TO]->(c)
            """
            )

            # Query the data
            print("Querying data...")
            result = session.run("MATCH (n:TestNode) RETURN n.id, n.name")
            for record in result:
                print(f"  Node: id={record['n.id']}, name={record['n.name']}")

            result = session.run("MATCH ()-[r:CONNECTED_TO]->() RETURN count(r) as count")
            count = result.single()["count"]
            print(f"  Total relationships: {count}")

            # Cleanup
            print("Cleaning up...")
            session.run("MATCH (n:TestNode) DETACH DELETE n")

        print("Workload completed successfully!")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
