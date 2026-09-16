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

import os
import sys

from neo4j import GraphDatabase

URI = f"bolt://localhost:{os.getenv('MEMGRAPH_BOLT_PORT', '7687')}"
AUTH = ("", "")


def execute_query(query: str):
    """Execute a query and return the results."""
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(query)
            return list(result)


def expect_error(query: str, label: str, expected: str):
    """Assert that a query fails with `expected` in the message.

    Matching the message matters: a bare `except Exception` also passes when the server is
    unreachable, or when the query fails for an unrelated reason.
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

    The planner turns `{p: 1}` into an inline filter on the inner edge plus a post-expansion
    `all(e IN r WHERE e.p = 1)`. The limit is enforced inside the operator, so before the inline
    filter was honoured it could be spent on paths the post-expansion filter then discarded.
    The shortest path is the non-matching one, so ordering does not depend on storage iteration.
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

    # Already correct without a limit, since the post-expansion filter saw every path. Guard that.
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


def test_kshortest_limit_is_per_input_row():
    """The `|k` limit caps each input row independently, not the whole result stream.

    The operator used to stop for good the first time one row produced `k` paths, dropping every
    later row. Three disjoint pairs with two paths each discriminate all three candidate
    semantics: per-row gives 3/6/6, a global budget 1/2/3, the old behaviour 1/2/6.
    """
    print("Testing that the kshortest limit is scoped to an input row...")
    execute_query("MATCH (n) DETACH DELETE n")
    for i in range(3):
        execute_query(
            f"""
            CREATE (s:Src {{id: {i}}})
            CREATE (m:Mid {{id: {i}}})
            CREATE (t:Dst {{id: {i}}})
            CREATE (s)-[:LINK]->(t)
            CREATE (s)-[:LINK]->(m)
            CREATE (m)-[:LINK]->(t)
        """
        )

    for limit, expected in ((1, 3), (2, 6), (3, 6)):
        results = execute_query(
            f"""
            MATCH (s:Src),(t:Dst) WHERE s.id = t.id
            WITH s, t MATCH (s)-[r:LINK *KSHORTEST|{limit}]->(t) RETURN r
        """
        )
        print(f"  limit {limit}: {len(results)} paths")
        assert len(results) == expected, f"Expected {expected} paths with limit {limit}, got {len(results)}"

    # Zero is a legal request for no paths, and yields nothing on every row.
    results = execute_query(
        """
        MATCH (s:Src),(t:Dst) WHERE s.id = t.id
        WITH s, t MATCH (s)-[r:LINK *KSHORTEST|0]->(t) RETURN r
    """
    )
    assert len(results) == 0, f"Expected 0 paths with limit 0, got {len(results)}"

    # A negative limit is not a request for anything, so it is an error rather than an empty result.
    expect_error(
        """
        MATCH (s:Src),(t:Dst) WHERE s.id = t.id
        WITH s, t MATCH (s)-[r:LINK *KSHORTEST|(0-1)]->(t) RETURN r
    """,
        "negative path limit",
        "Limit in KSHORTEST path expansion must not be negative.",
    )

    print("Per-input-row limit tests passed! ✅")


def test_kshortest_limit_can_depend_on_the_input_row():
    """A `|k` that reads a frame value is evaluated per row, against that row's frame.

    The limit used to be evaluated once at the top of every `Pull`, before the input row was
    pulled. A frame-dependent `|k` therefore read unbound nulls on the first pull, and thereafter
    capped each row by the *previous* row's value. Three symptoms, all covered below: a plain
    property limit failed outright; a limit that evaluated to 0 against the empty frame ended the
    whole result stream silently; and a row whose own limit was 0 still emitted one path while the
    row after it was dropped.
    """
    print("Testing that the kshortest limit may depend on the input row...")
    execute_query("MATCH (n) DETACH DELETE n")
    # Three disjoint pairs, two paths each, carrying their own limit.
    for i, k in enumerate((0, 1, 2)):
        execute_query(
            f"""
            CREATE (s:Src {{id: {i}, k: {k}}})
            CREATE (m:Mid {{id: {i}}})
            CREATE (t:Dst {{id: {i}}})
            CREATE (s)-[:LINK]->(t)
            CREATE (s)-[:LINK]->(m)
            CREATE (m)-[:LINK]->(t)
        """
        )

    # Each row is capped by its own `s.k`: 0 + 1 + 2 paths. Evaluating the limit before the input
    # pull instead raised "Limit in KSHORTEST path expansion must be an int".
    results = execute_query(
        """
        MATCH (s:Src),(t:Dst) WHERE s.id = t.id
        WITH s, t MATCH (s)-[r:LINK *KSHORTEST|s.k]->(t) RETURN s.id AS id, r
    """
    )
    per_row = {}
    for record in results:
        per_row[record["id"]] = per_row.get(record["id"], 0) + 1
    print(f"  per-row counts for |s.k: {per_row}")
    assert len(results) == 3, f"Expected 3 paths in total for |s.k, got {len(results)}"
    assert per_row == {1: 1, 2: 2}, f"Expected row 1 to yield 1 path and row 2 to yield 2, got {per_row}"

    # A limit that is 0 for one row must skip that row only, not end the stream. Reading the limit
    # from a stale frame gave 3 rows here: the k=0 row emitted a path and the last row vanished.
    results = execute_query(
        """
        UNWIND [2, 0, 2] AS k
        MATCH (s:Src {id: 0}),(t:Dst {id: 0})
        WITH k, s, t MATCH (s)-[r:LINK *KSHORTEST|k]->(t) RETURN k, r
    """
    )
    print(f"  |k over UNWIND [2, 0, 2]: {len(results)} paths")
    assert len(results) == 4, f"Expected 2 + 0 + 2 = 4 paths, got {len(results)}"
    assert all(record["k"] == 2 for record in results), "The k=0 row must contribute no paths"

    # A limit expression that evaluates to 0 only on an unbound frame must not silence the query.
    results = execute_query(
        """
        MATCH (s:Src),(t:Dst) WHERE s.id = t.id
        WITH s, t MATCH (s)-[r:LINK *KSHORTEST|(CASE WHEN s.k IS NULL THEN 0 ELSE 2 END)]->(t)
        RETURN r
    """
    )
    print(f"  CASE-guarded limit: {len(results)} paths")
    assert len(results) == 6, f"Expected 6 paths (2 per row), got {len(results)}"

    print("Row-dependent limit tests passed! ✅")


def test_kshortest_depth_bounds():
    """A zero lower bound is legal; a negative one and a non-positive upper bound are not.

    `0..n` is accepted by every other expansion taking a lower bound, and 0 is inert here since
    KSHORTEST already skips `source == target`. Only a negative bound wraps to a huge unsigned
    value and makes the top-up loop enumerate every simple path.
    """
    print("Testing kshortest depth bounds...")
    execute_query("MATCH (n) DETACH DELETE n")
    execute_query(
        """
        CREATE (s:Src {id: 0})
        CREATE (m:Mid {id: 0})
        CREATE (t:Dst {id: 0})
        CREATE (s)-[:LINK]->(t)
        CREATE (s)-[:LINK]->(m)
        CREATE (m)-[:LINK]->(t)
    """
    )

    for bounds in ("0..3", "0..", "1..3"):
        results = execute_query(
            f"""
            MATCH (s:Src),(t:Dst) WITH s, t MATCH (s)-[r:LINK *KSHORTEST {bounds}]->(t) RETURN r
        """
        )
        print(f"  bounds {bounds!r}: {len(results)} paths")
        assert len(results) == 2, f"Expected 2 paths for bounds {bounds}, got {len(results)}"

    expect_error(
        """
        MATCH (s:Src),(t:Dst) WITH s, t MATCH (s)-[r:LINK *KSHORTEST (0-1)..3]->(t) RETURN r
    """,
        "negative lower bound",
        "Minimum depth in KSHORTEST path expansion must not be negative.",
    )
    expect_error(
        """
        MATCH (s:Src),(t:Dst) WITH s, t MATCH (s)-[r:LINK *KSHORTEST 1..0]->(t) RETURN r
    """,
        "zero upper bound",
        "Maximum depth in KSHORTEST path expansion must be at least 1.",
    )
    # An inverted range is legally empty, unlike the invalid bounds above.
    results = execute_query(
        """
        MATCH (s:Src),(t:Dst) WITH s, t MATCH (s)-[r:LINK *KSHORTEST 4..3]->(t) RETURN r
    """
    )
    print(f"  inverted range: {len(results)} paths")
    assert len(results) == 0, f"Expected no paths for an inverted range, got {len(results)}"

    print("Depth bound tests passed! ✅")


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

    # A weight lambda cannot take the accumulated path: it never sees one.
    print("Test: kshortest with an accumulated path in the weight lambda should raise error")
    expect_error(
        """
        MATCH (a:Person {name: 'Alice'}),(b:Person {name: 'Bob'})
        WITH a, b MATCH (a)-[r:KNOWS *KSHORTEST|2 (rel, n, p | 1) total]->(b) RETURN r
    """,
        "kshortest with an accumulated path in the weight lambda",
        "accumulated path",
    )

    # Two lambdas are weight then filter, so a third has nowhere to go.
    print("Test: depth-first expansion with two lambdas should still raise error")
    expect_error(
        """
        MATCH (a:Person {name: 'Alice'}),(b:Person {name: 'Bob'})
        WITH a, b MATCH (a)-[r:KNOWS * (rel, n | 1) (rel, n | true)]->(b) RETURN r
    """,
        "depth-first expansion with two lambdas",
        "Only one filter lambda can be supplied.",
    )

    # The two below are runtime errors, so unlike the rest of this function they need a pair the
    # expansion actually walks: Alice-[:KNOWS]->Bob is the one edge this graph has.
    print("Test: kshortest with a null weight should raise error")
    expect_error(
        """
        MATCH (a:Person {name: 'Alice'}),(b:Person {name: 'Bob'})
        WITH a, b MATCH (a)-[r:KNOWS *KSHORTEST (rel, n | rel.missing) total]->(b) RETURN r
    """,
        "kshortest with a null weight",
        "must not evaluate to null",
    )

    # Dijkstra needs non-negative weights.
    print("Test: kshortest with a negative weight should raise error")
    expect_error(
        """
        MATCH (a:Person {name: 'Alice'}),(b:Person {name: 'Bob'})
        WITH a, b MATCH (a)-[r:KNOWS *KSHORTEST (rel, n | -1) total]->(b) RETURN r
    """,
        "kshortest with a negative weight",
        "non-negative",
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


def test_kshortest_weight_lambda():
    """The three spellings a weight lambda can take, and the order they impose."""
    print("Testing kshortest weight lambda...")

    execute_query("MATCH (n) DETACH DELETE n")
    # One cheap three-hop route against one expensive single hop, so hop count and weight disagree.
    execute_query(
        """
        CREATE (a:P {name: 'a'})
        CREATE (b:P {name: 'b'})
        CREATE (c:P {name: 'c'})
        CREATE (d:P {name: 'd'})
        CREATE (a)-[:E {w: 1}]->(b)
        CREATE (b)-[:E {w: 1}]->(c)
        CREATE (c)-[:E {w: 1}]->(d)
        CREATE (a)-[:E {w: 4}]->(c)
        CREATE (a)-[:E {w: 10}]->(d)
    """
    )

    # Spelling 1: weight lambda and a named total. Ascending by (weight, hops).
    print("Test: weight lambda with a named total weight")
    results = execute_query(
        """
        MATCH (a:P {name: 'a'}),(d:P {name: 'd'})
        WITH a, d MATCH (a)-[r:E *KSHORTEST (rel, n | rel.w) total]->(d)
        RETURN total, size(r) AS hops
    """
    )
    assert [(r["total"], r["hops"]) for r in results] == [
        (3, 3),
        (5, 2),
        (10, 1),
    ], f"Unexpected weighted order: {[(r['total'], r['hops']) for r in results]}"

    # Hop count alone would serve the same three paths the other way round.
    print("Test: the same pair without a weight lambda still orders by hops")
    results = execute_query(
        """
        MATCH (a:P {name: 'a'}),(d:P {name: 'd'})
        WITH a, d MATCH (a)-[r:E *KSHORTEST]->(d) RETURN size(r) AS hops
    """
    )
    assert [r["hops"] for r in results] == [1, 2, 3], f"Unexpected hop-count order: {[r['hops'] for r in results]}"

    # Spelling 2: a lone lambda with no total weight variable is still the filter, as it always was.
    print("Test: a lone lambda is still the filter lambda")
    results = execute_query(
        """
        MATCH (a:P {name: 'a'}),(d:P {name: 'd'})
        WITH a, d MATCH (a)-[r:E *KSHORTEST (rel, n | rel.w < 5)]->(d) RETURN size(r) AS hops
    """
    )
    assert [r["hops"] for r in results] == [2, 3], f"Unexpected filtered order: {[r['hops'] for r in results]}"

    # Spelling 3: weight then filter, with the total weight variable between them.
    print("Test: weight lambda, total weight and filter lambda")
    results = execute_query(
        """
        MATCH (a:P {name: 'a'}),(d:P {name: 'd'})
        WITH a, d MATCH (a)-[r:E *KSHORTEST (rel, n | rel.w) total (rel, n | n.name <> 'c')]->(d)
        RETURN total, size(r) AS hops
    """
    )
    assert [(r["total"], r["hops"]) for r in results] == [(10, 1)], f"Unexpected filtered weighted rows: {results}"

    # Spelling 4: two lambdas with the total weight variable left out.
    print("Test: weight and filter lambdas with an anonymous total weight")
    results = execute_query(
        """
        MATCH (a:P {name: 'a'}),(d:P {name: 'd'})
        WITH a, d MATCH (a)-[r:E *KSHORTEST (rel, n | rel.w) (rel, n | true)]->(d) RETURN size(r) AS hops
    """
    )
    assert [r["hops"] for r in results] == [
        3,
        2,
        1,
    ], f"Unexpected anonymous-total order: {[r['hops'] for r in results]}"

    # The path limit caps the cheapest paths, not the shortest ones.
    print("Test: weight lambda with a path limit")
    results = execute_query(
        """
        MATCH (a:P {name: 'a'}),(d:P {name: 'd'})
        WITH a, d MATCH (a)-[r:E *KSHORTEST|2 (rel, n | rel.w) total]->(d) RETURN total
    """
    )
    assert [r["total"] for r in results] == [3, 5], f"Unexpected limited totals: {[r['total'] for r in results]}"

    # The length bound still counts hops, so the cheapest route can fall outside it.
    print("Test: weight lambda with an upper bound")
    results = execute_query(
        """
        MATCH (a:P {name: 'a'}),(d:P {name: 'd'})
        WITH a, d MATCH (a)-[r:E *KSHORTEST ..2 (rel, n | rel.w) total]->(d) RETURN total
    """
    )
    assert [r["total"] for r in results] == [5, 10], f"Unexpected bounded totals: {[r['total'] for r in results]}"

    # The total weight variable is an ordinary bound symbol, so a later WHERE can filter on it.
    print("Test: filtering on the total weight")
    results = execute_query(
        """
        MATCH (a:P {name: 'a'}),(d:P {name: 'd'})
        WITH a, d MATCH (a)-[r:E *KSHORTEST (rel, n | rel.w) total]->(d)
        WHERE total < 6 RETURN total
    """
    )
    assert [r["total"] for r in results] == [3, 5], f"Unexpected filtered totals: {[r['total'] for r in results]}"

    print("Weight lambda tests passed! ✅")


if __name__ == "__main__":
    try:
        test_kshortest_limit()
        test_kshortest_inline_property_filter()
        test_kshortest_limit_is_per_input_row()
        test_kshortest_limit_can_depend_on_the_input_row()
        test_kshortest_depth_bounds()
        test_kshortest_weight_lambda()
        test_syntax_errors()
        print("\n🎉 All tests completed successfully!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
