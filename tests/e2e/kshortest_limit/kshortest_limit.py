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


def expect_error(query: str, label: str, expected: str):
    """Assert that a query fails with `expected` in the message.

    Matching the message matters: a bare `except Exception` would also pass if the server were
    unreachable, or if the query failed for a completely different reason than the one under test.
    The assertions are raised outside the `except` block so they cannot be swallowed.
    """
    try:
        execute_query(query)
    except Exception as exc:
        assert expected in str(exc), f"Expected {expected!r} in the error for {label}, got: {exc}"
        print(f"  Correctly got error for {label}: {exc}")
        return
    raise AssertionError(f"Expected an error for {label}")


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

    # Test 5: kshortest with a filter lambda
    print("Test 5: kshortest with a filter lambda")
    results = execute_query(
        """
        MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'})
        WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST (rel, n | n.name <> 'Bob')]->(e) RETURN r
    """
    )
    print(f"  Found {len(results)} paths with the Bob detour filtered out")
    assert len(results) == 2, f"Expected 2 paths, got {len(results)}"

    # Test 6: kshortest with a filter lambda and a limit
    print("Test 6: kshortest with a filter lambda and a limit")
    results = execute_query(
        """
        MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'})
        WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST|1 (rel, n | n.name <> 'Bob')]->(e) RETURN r
    """
    )
    print(f"  Found {len(results)} paths with the filter and limit 1")
    assert len(results) == 1, f"Expected 1 path, got {len(results)}"

    print("All tests passed! ✅")


def test_kshortest_inline_property_filter():
    """An inlined edge property map is now applied during the search, not only after it.

    The planner turns `{p: 1}` into two filters: an inline one over the expansion's inner edge, and
    a post-expansion `all(e IN r WHERE e.p = 1)`. The path limit is enforced inside the operator, so
    before the inline filter was honoured the limit could be spent on paths the post-expansion filter
    then threw away - yielding fewer than `k` matching paths, or none at all.

    The graph makes the shortest path the non-matching one, so the ordering is deterministic rather
    than dependent on storage iteration order.
    """
    print("Testing kshortest with an inlined edge property map...")
    execute_query("MATCH (n) DETACH DELETE n")
    execute_query(
        """
        CREATE (s:Route {id: 's'})
        CREATE (m:Route {id: 'm'})
        CREATE (t:Route {id: 't'})
        CREATE (s)-[:LINK {p: 0}]->(t)
        CREATE (s)-[:LINK {p: 1}]->(m)
        CREATE (m)-[:LINK {p: 1}]->(t)
    """
    )

    # The one-hop path is shorter but has p = 0, so the only matching path is the two-hop one.
    print("Test: inlined property map with a limit of 1")
    results = execute_query(
        """
        MATCH (s:Route {id: 's'}),(t:Route {id: 't'})
        WITH s, t MATCH (s)-[r:LINK *KSHORTEST|1 {p: 1}]->(t) RETURN r
    """
    )
    print(f"  Found {len(results)} matching paths with limit 1")
    assert len(results) == 1, f"Expected 1 path, got {len(results)}"
    assert len(results[0]["r"]) == 2, f"Expected the two-hop matching path, got {len(results[0]['r'])} edges"

    # Without a limit the answer was already correct, because the post-expansion filter got to see
    # every path the search produced. Guard that it stayed correct.
    print("Test: inlined property map without a limit")
    results = execute_query(
        """
        MATCH (s:Route {id: 's'}),(t:Route {id: 't'})
        WITH s, t MATCH (s)-[r:LINK *KSHORTEST {p: 1}]->(t) RETURN r
    """
    )
    print(f"  Found {len(results)} matching paths without a limit")
    assert len(results) == 1, f"Expected 1 path, got {len(results)}"
    assert len(results[0]["r"]) == 2, f"Expected the two-hop matching path, got {len(results[0]['r'])} edges"

    print("Inlined property map tests passed! ✅")


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
    expect_error(
        """
        MATCH (a:Person {name: 'Alice'}),(b:Person {name: 'Bob'})
        WITH a, b MATCH (a)-[r:KNOWS *BFS|2]->(b) RETURN r
    """,
        "BFS with limit",
        "Limit parameter is only supported with KSHORTEST path expansion.",
    )

    # kshortest accepts a two-argument filter lambda, but not the accumulated path
    print("Test: kshortest with an accumulated path in the filter lambda should raise error")
    expect_error(
        """
        MATCH (a:Person {name: 'Alice'}),(b:Person {name: 'Bob'})
        WITH a, b MATCH (a)-[r:KNOWS *KSHORTEST|2 (rel, n, p | size(p) > 0)]->(b) RETURN r
    """,
        "kshortest with an accumulated path in the filter lambda",
        "KSHORTEST expansion does not support the accumulated path in a filter lambda.",
    )

    # kshortest takes at most one lambda; a weight lambda is not accepted
    print("Test: kshortest with two lambdas should raise error")
    expect_error(
        """
        MATCH (a:Person {name: 'Alice'}),(b:Person {name: 'Bob'})
        WITH a, b MATCH (a)-[r:KNOWS *KSHORTEST|2 (rel, n | 1) (rel, n | true)]->(b) RETURN r
    """,
        "kshortest with two lambdas",
        "Only one filter lambda can be supplied.",
    )

    # A filter lambda evaluating to something other than a boolean or null must fail at runtime
    print("Test: kshortest with a non-boolean filter lambda should raise error")
    expect_error(
        """
        MATCH (a:Person {name: 'Alice'}),(b:Person {name: 'Bob'})
        WITH a, b MATCH (a)-[r:KNOWS *KSHORTEST|2 (rel, n | 42)]->(b) RETURN r
    """,
        "kshortest with a non-boolean filter lambda",
        "Expansion condition must evaluate to boolean or null",
    )

    print("Syntax error tests passed! ✅")


if __name__ == "__main__":
    try:
        test_kshortest_limit()
        test_kshortest_inline_property_filter()
        test_syntax_errors()
        print("\n🎉 All tests completed successfully!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
