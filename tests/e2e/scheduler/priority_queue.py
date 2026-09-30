# Copyright 2022 Memgraph Ltd.
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

import pytest
from neo4j import GraphDatabase

URI = "bolt://127.0.0.1:7687"
NUM_WORKERS = 4
TIMEOUT_SEC = 1


def test_sidecar():
    with GraphDatabase.driver(URI, auth=("", "")) as driver:
        # Goal: to show that TTL is database specific
        # 0/ Setup DB
        # 1/ Setup many long running queries (more then num workers)
        # 2/ Try to connect
        # 3/ Try to execute high priority query

        # 0
        with driver.session() as session:
            session.run("UNWIND range(1,250) AS i CREATE(:A)-[:E]->(:B);").consume()

        # 1
        def long_running_task():
            with driver.session() as session:
                # Needs to aggregate, so the pull can't be split
                session.run("MATCH () MATCH () MATCH () RETURN count(*);").values()

        NUM_CONCURRENT_QUERIES = NUM_WORKERS * 2
        running_queries = []

        with ThreadPoolExecutor(max_workers=NUM_CONCURRENT_QUERIES) as executor:
            for _ in range(NUM_CONCURRENT_QUERIES):
                future = executor.submit(long_running_task)
                running_queries.append(future)

            # Give some time for queries to start
            time.sleep(1)

            # 2 - Verify we can still connect
            start_time = time.time()
            new_session = driver.session()
            connection_time = time.time() - start_time

            # Connection should be established within n second
            assert connection_time < TIMEOUT_SEC, f"Connection took too long: {connection_time} seconds"

            # 3 - Execute and measure high priority query
            start_time = time.time()
            transactions = new_session.run("SHOW TRANSACTIONS").values()
            execution_time = time.time() - start_time

            # High priority query should complete within n seconds
            assert execution_time < TIMEOUT_SEC, f"High priority query took too long: {execution_time} seconds"

            # Verify we have the expected number of running transactions
            assert (
                len(transactions) == NUM_WORKERS + 1
            ), f"Expected {NUM_WORKERS + 1} transactions, got {len(transactions)}"

            new_session.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
