#!/usr/bin/env python3
"""
Analytical Memory Stress Test

Switches Memgraph to IN_MEMORY_ANALYTICAL mode, then repeatedly creates
50M nodes (with a property each) and 25M edges (25M pairs, with a
property each), then drops the graph. Logs memory after each
iteration to detect memory leaks in the analytical storage engine.
"""

import sys
import time

from neo4j import GraphDatabase
from standalone_monitor import StandaloneMonitor

HOST = "127.0.0.1"
PORT = 7687

ITERATIONS = 50
TOTAL_NODES = 50_000_000
TOTAL_EDGES = 25_000_000

MONITOR_FIELDS = ["vertex_count", "edge_count", "memory_res", "memory_tracked"]


def main():
    uri = f"bolt://{HOST}:{PORT}"
    print(f"Connecting to Memgraph at {uri}")

    driver = GraphDatabase.driver(uri, auth=("", ""))
    monitor = StandaloneMonitor(host=HOST, port=PORT, storage_info=MONITOR_FIELDS)

    try:
        with driver.session() as session:
            print("Switching to IN_MEMORY_ANALYTICAL storage mode...")
            session.run("STORAGE MODE IN_MEMORY_ANALYTICAL;").consume()
            print("Storage mode set to IN_MEMORY_ANALYTICAL")

            baseline = monitor.log_storage_info(label="BASELINE")
            memory_after_drop = []

            for i in range(1, ITERATIONS + 1):
                print(f"\n{'=' * 60}")
                print(f"Iteration {i}/{ITERATIONS}")
                print(f"{'=' * 60}")

                # -- Create nodes with a property --
                create_start = time.time()
                session.run(
                    f"UNWIND range(1, {TOTAL_NODES}) AS i CREATE (:Node {{id: i, name: 'node_' + toString(i)}})"
                ).consume()
                node_elapsed = time.time() - create_start
                print(f"  Created {TOTAL_NODES:,} nodes: {node_elapsed:.1f}s")

                # -- Create edges (25M pairs) with a property --
                edge_start = time.time()
                session.run(
                    f"MATCH (a:Node), (b:Node) "
                    f"WHERE a.id <= {TOTAL_EDGES} AND b.id = a.id + {TOTAL_EDGES} "
                    f"CREATE (a)-[:EDGE {{weight: a.id, label: 'edge_' + toString(a.id)}}]->(b)"
                ).consume()
                edge_elapsed = time.time() - edge_start
                print(f"  Created {TOTAL_EDGES:,} edges: {edge_elapsed:.1f}s")
                create_elapsed = node_elapsed + edge_elapsed

                monitor.log_storage_info(label=f"ITER {i} AFTER CREATE")

                # -- Drop graph --
                drop_start = time.time()
                session.run("DROP GRAPH;").consume()
                drop_elapsed = time.time() - drop_start
                print(f"  DROP GRAPH: {drop_elapsed:.1f}s")

                time.sleep(1)

                info = monitor.log_storage_info(label=f"ITER {i} AFTER DROP")
                memory_after_drop.append(info.get("memory_res", "?"))

                print(f"  Timings: create={create_elapsed:.1f}s drop={drop_elapsed:.1f}s")

            # Let memory settle after final iteration
            print("\nWaiting 5s for memory to settle...")
            time.sleep(5)
            monitor.log_storage_info(label="FINAL")

        # -- Summary --
        print(f"\n{'=' * 60}")
        print("SUMMARY")
        print(f"{'=' * 60}")
        print(f"Iterations completed: {ITERATIONS}")
        print(f"Nodes per iteration:  {TOTAL_NODES:,}")
        print(f"Edges per iteration:  {TOTAL_EDGES:,}")
        print(f"Baseline memory_res:  {baseline.get('memory_res', '?')}")
        print("Memory after DROP per iteration:")
        for idx, mem in enumerate(memory_after_drop, 1):
            print(f"  iter {idx:3d}: {mem}")

        print("\nAnalytical memory stress test completed!")

    except Exception as e:
        print(f"\nFATAL: {e}", file=sys.stderr)
        raise
    finally:
        driver.close()
        monitor.stop()


if __name__ == "__main__":
    main()
