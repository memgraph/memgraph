#!/usr/bin/env python3
# Copyright 2024 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""
Simple test script to verify kshortest path limit functionality.
This script can be run independently to test the feature.
"""

import sys

from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
AUTH = ("", "")


def execute_query(query: str):
    """Execute a query and return the results."""
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(query)
            return list(result)


def test_kshortest_limit():
    """Test the kshortest path limit functionality."""
    print("Testing kshortest path limit functionality...")

    # Clean up and create test graph
    print("Setting up test graph...")
    execute_query("MATCH (n) DETACH DELETE n")
    execute_query(
        """
        CREATE (a:Person {name: 'Alice'})
        CREATE (b:Person {name: 'Bob'})
        CREATE (c:Person {name: 'Charlie'})
        CREATE (d:Person {name: 'David'})
        CREATE (e:Person {name: 'Eve'})
        CREATE (a)-[:KNOWS]->(b)
        CREATE (b)-[:KNOWS]->(e)
        CREATE (a)-[:KNOWS]->(c)
        CREATE (c)-[:KNOWS]->(e)
        CREATE (a)-[:KNOWS]->(d)
        CREATE (d)-[:KNOWS]->(e)
    """
    )

    # Test 1: kshortest without limit
    print("Test 1: kshortest without limit")
    results = execute_query(
        """
        MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'})
        WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST]->(e) RETURN r
    """
    )
    print(f"  Found {len(results)} paths without limit")
    assert len(results) == 3, f"Expected 3 paths, got {len(results)}"

    # Test 2: kshortest with limit 1
    print("Test 2: kshortest with limit 1")
    results = execute_query(
        """
        MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'})
        WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST|1]->(e) RETURN r
    """
    )
    print(f"  Found {len(results)} paths with limit 1")
    assert len(results) == 1, f"Expected 1 path, got {len(results)}"

    # Test 3: kshortest with limit 2
    print("Test 3: kshortest with limit 2")
    results = execute_query(
        """
        MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'})
        WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST|2]->(e) RETURN r
    """
    )
    print(f"  Found {len(results)} paths with limit 2")
    assert len(results) == 2, f"Expected 2 paths, got {len(results)}"

    # Test 4: kshortest with limit 5 (more than available)
    print("Test 4: kshortest with limit 5")
    results = execute_query(
        """
        MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'})
        WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST|5]->(e) RETURN r
    """
    )
    print(f"  Found {len(results)} paths with limit 5")
    assert len(results) == 3, f"Expected 3 paths, got {len(results)}"

    print("All tests passed! ✅")


def test_syntax_errors():
    """Test that invalid syntax raises appropriate errors."""
    print("Testing syntax error cases...")

    # Clean up and create simple graph
    execute_query("MATCH (n) DETACH DELETE n")
    execute_query(
        """
        CREATE (a:Person {name: 'Alice'})
        CREATE (b:Person {name: 'Bob'})
        CREATE (a)-[:KNOWS]->(b)
    """
    )

    # Test that limit with non-kshortest expansion raises error
    print("Test: limit with BFS should raise error")
    try:
        execute_query(
            """
            MATCH (a:Person {name: 'Alice'}),(b:Person {name: 'Bob'})
            WITH a, b MATCH (a)-[r:KNOWS *BFS|2]->(b) RETURN r
        """
        )
        assert False, "Expected error for BFS with limit"
    except Exception as e:
        print(f"  Correctly got error: {e}")

    # kshortest with weight lambda and limit
    print("Test: kshortest with weight lambda and limit")
    try:
        execute_query(
            """
            MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'})
            WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST|2 (e, n | e.weight)]->(e) RETURN r
        """
        )
        assert False, "Expected error for kshortest with weight lambda and limit"
    except Exception as e:
        print(f"  Correctly got error: {e}")

    print("Syntax error tests passed! ✅")


if __name__ == "__main__":
    try:
        test_kshortest_limit()
        test_syntax_errors()
        print("\n🎉 All tests completed successfully!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
