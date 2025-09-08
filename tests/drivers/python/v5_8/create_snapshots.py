#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import sys
import time
from concurrent.futures import ThreadPoolExecutor

from neo4j import GraphDatabase

# Neo4j connection details
URI = "bolt://localhost:7687"


class Neo4jOperations:
    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri, auth=None, max_transaction_retry_time=0)
        print("Neo4j driver initialized")

    def close(self):
        self.driver.close()
        print("Neo4j driver closed")

    # Transaction function
    def explicit_tx(self, tx, query):
        result = tx.run(query)
        result.consume()

    # Function for implicit transactions (auto-commit)
    def implicit_tx(self, session, query):
        result = session.run(query)
        result.consume()


def write_task(neo4j_ops, query="CREATE ();"):
    with neo4j_ops.driver.session() as session:
        print("Starting write transaction")
        session.execute_write(neo4j_ops.explicit_tx, query)
        print(f"Write transaction completed")


def read_task(neo4j_ops, query="MATCH (n) RETURN n LIMIT 1"):
    with neo4j_ops.driver.session() as session:
        print("Starting read transaction")
        result = session.execute_read(neo4j_ops.explicit_tx, query)
        print(f"Read transaction completed")


def implicit_task(neo4j_ops, query):
    with neo4j_ops.driver.session() as session:
        print(f"Starting implicit transaction {query}")
        neo4j_ops.implicit_tx(session, query)
        print(f"Implicit transaction completed {query}")


def main():
    neo4j_ops = Neo4jOperations(URI)

    with ThreadPoolExecutor(max_workers=4) as executor:
        try:
            # Setup (need some data so snapshot takes time)
            with neo4j_ops.driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n").consume()
                print("Database cleared.")
                session.run("FREE MEMORY").consume()
                print("Memory cleared.")
                session.run("STORAGE MODE IN_MEMORY_ANALYTICAL").consume()
                print("Analytical mode.")

            # Setup (needs some data for snapshot)
            write_task(neo4j_ops, "UNWIND RANGE (1,5000000) as i create (:l)-[:e]->();")

            # TODO Check that a write tx allows reads and writes, but not read-only (create snapshot)
            #   Currently only cypher queries can timeout, uncomment this when CREATE SNAPSHOT can timeout
            # failed = 0
            # Use this write to also setup the database (needs some data for snapshot)
            # write_future = executor.submit(write_task, neo4j_ops, "UNWIND RANGE (1,5000000) as i create (:l)-[:e]->();")
            # # Wait for a moment before starting the read transaction
            # time.sleep(0.1)
            # read_future = executor.submit(read_task, neo4j_ops)
            # time.sleep(0.1)
            # impl_read_future = executor.submit(implicit_task, neo4j_ops, "MATCH(n) RETURN n LIMIT 1")
            # time.sleep(0.1)
            # impl_write_future = executor.submit(implicit_task, neo4j_ops, "CREATE ()")
            # time.sleep(0.1)
            # try:
            #     # Needs to timeout
            #     implicit_task(neo4j_ops, "CREATE SNAPSHOT")
            # except Exception as e:
            #     failed += 1

            # # Wait for all tasks to complete
            # write_future.result()
            # read_future.result()
            # impl_read_future.result()
            # impl_write_future.result()

            # assert failed == 1, "Snapshot transaction should not be allowed in write mode"

            # print("Write test OK!")

            failed = 0
            # Check that a read_only (snapshot) tx allows reads but not writes
            snapshot_future = executor.submit(implicit_task, neo4j_ops, "CREATE SNAPSHOT")
            # Wait for a moment before starting the read transaction
            time.sleep(0.1)
            read_future = executor.submit(read_task, neo4j_ops)
            time.sleep(0.1)
            impl_read_future = executor.submit(implicit_task, neo4j_ops, "MATCH(n) RETURN n LIMIT 1")
            time.sleep(0.1)
            try:
                # Needs to timeout
                implicit_task(neo4j_ops, "CREATE ()")
            except Exception as e:
                print(f"Write implicit transaction failed as expected: {e}")
                failed += 1
            try:
                # Needs to timeout
                write_task(neo4j_ops, "UNWIND RANGE (1,5000000) as i create (:l)-[:e]->();")
            except Exception as e:
                print(f"Write explicit transaction failed as expected: {e}")
                failed += 1

            # Wait for all tasks to complete
            snapshot_future.result()
            read_future.result()
            impl_read_future.result()

            assert failed == 2, "Write transaction should not be allowed in read-only mode"

            print("Read-only test OK!")

        except Exception as e:
            print(f"Error occurred: {e}")
            sys.exit(1)
        finally:
            with neo4j_ops.driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n").consume()
                print("Database cleared.")
                session.run("FREE MEMORY").consume()
                print("Memory cleared.")
                session.run("STORAGE MODE IN_MEMORY_TRANSACTIONAL").consume()
                print("Instance reset.")
            neo4j_ops.close()


if __name__ == "__main__":
    main()
