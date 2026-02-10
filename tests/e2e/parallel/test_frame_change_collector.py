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

"""
Tests for Frame Change Collector in parallel execution.

The Frame Change Collector is used to cache IN LIST operations (like `x IN [1, a]`)
and REGEX operations for efficiency. This module tests that:
    1. IN LIST operations work correctly with parallel execution
    2. Cache invalidation works properly when dependent symbols change
    3. Results are consistent between serial and parallel execution
    4. Caching with variable-dependent lists works correctly

Test patterns include:
    - Static lists: x IN [1, 2, 3]
    - Variable-dependent lists: x IN [1, a] where 'a' is a node property
    - Range-based lists: x IN range(1, 100)
    - Mixed expressions in lists
"""

import sys
from typing import Any, Dict, List

import pytest
from common import clear_database, pq, setup_thread_count_db

# =============================================================================
# Helper Functions
# =============================================================================


def normalize_results(results: List[Dict[str, Any]]) -> List[tuple]:
    """Normalize results for comparison by sorting."""
    normalized = []
    for record in results:
        # Sort by keys and convert to tuple
        items = tuple(sorted(record.items()))
        normalized.append(items)
    return sorted(normalized)


def verify_parallel_matches_serial(memgraph, query: str, params: dict = None):
    """
    Execute query both with and without parallel execution.
    Verify results are identical.
    """
    # Serial execution
    serial_result = memgraph.fetch_all(query, params)

    # Parallel execution
    parallel_result = memgraph.fetch_all(pq(query), params)

    serial_normalized = normalize_results(serial_result)
    parallel_normalized = normalize_results(parallel_result)

    assert (
        parallel_normalized == serial_normalized
    ), f"Results differ!\nSerial: {serial_normalized}\nParallel: {parallel_normalized}"

    return serial_result


# =============================================================================
# Test Classes
# =============================================================================


class TestInListStaticLists:
    """
    Tests for IN LIST operator with static lists (no variable dependencies).
    These lists can be cached once and reused.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        """Set up test data."""
        self.memgraph = memgraph
        clear_database(memgraph)
        # Create nodes with various property values
        memgraph.execute_query(
            """
            UNWIND range(1, 100) AS i
            CREATE (:Node {id: i, value: i % 10, category: CASE WHEN i % 3 = 0 THEN 'A' ELSE 'B' END})
            """
        )

    def test_in_list_with_integer_literals(self):
        """Test IN LIST with a static list of integers."""
        query = "MATCH (n:Node) WHERE n.value IN [1, 3, 5, 7, 9] RETURN n.id AS id ORDER BY id"
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_with_string_literals(self):
        """Test IN LIST with a static list of strings."""
        query = "MATCH (n:Node) WHERE n.category IN ['A'] RETURN count(n) AS cnt"
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_with_range(self):
        """Test IN LIST with range() function (cacheable)."""
        query = "MATCH (n:Node) WHERE n.id IN range(1, 50) RETURN count(n) AS cnt"
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_with_empty_list(self):
        """Test IN LIST with empty list (should return no results)."""
        query = "MATCH (n:Node) WHERE n.value IN [] RETURN count(n) AS cnt"
        result = verify_parallel_matches_serial(self.memgraph, query)
        assert result[0]["cnt"] == 0

    def test_in_list_with_null_element(self):
        """Test IN LIST with null in the list."""
        query = "MATCH (n:Node) WHERE n.value IN [1, null, 3] RETURN count(n) AS cnt"
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_aggregation_with_static_list(self):
        """Test IN LIST combined with aggregation."""
        query = """
            MATCH (n:Node)
            WHERE n.value IN [0, 2, 4, 6, 8]
            RETURN n.value AS val, count(*) AS cnt
            ORDER BY val
        """
        verify_parallel_matches_serial(self.memgraph, query)


class TestInListVariableDependentLists:
    """
    Tests for IN LIST operator with variable-dependent lists.
    These lists depend on frame values and require cache invalidation
    when the dependent variable changes.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        """Set up test data with relationships."""
        self.memgraph = memgraph
        clear_database(memgraph)
        # Create a graph with nodes and relationships
        memgraph.execute_query(
            """
            UNWIND range(1, 50) AS i
            CREATE (a:Person {id: i, age: 20 + (i % 30), threshold: i % 5})
            """
        )
        # Create some relationships
        memgraph.execute_query(
            """
            MATCH (a:Person), (b:Person)
            WHERE a.id < b.id AND a.id % 10 = b.id % 10
            CREATE (a)-[:KNOWS {since: 2020 + (a.id % 5)}]->(b)
            """
        )

    def test_in_list_with_property_in_list(self):
        """
        Test IN LIST where the list contains a property reference.
        Pattern: x IN [1, n.prop]
        """
        query = """
            MATCH (n:Person)
            WHERE n.age IN [20, n.threshold + 20]
            RETURN n.id AS id
            ORDER BY id
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_with_computed_list_element(self):
        """
        Test IN LIST where list element is computed from node property.
        """
        query = """
            MATCH (n:Person)
            WHERE n.id IN [1, n.threshold * 10, 50]
            RETURN n.id AS id
            ORDER BY id
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_with_relationship_property(self):
        """Test IN LIST with relationship property in list."""
        query = """
            MATCH (a:Person)-[r:KNOWS]->(b:Person)
            WHERE r.since IN [2020, a.threshold + 2020]
            RETURN a.id AS aid, b.id AS bid
            ORDER BY aid, bid
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_with_multiple_variable_references(self):
        """Test IN LIST with multiple node properties in the list."""
        query = """
            MATCH (a:Person)-[:KNOWS]->(b:Person)
            WHERE a.age IN [b.age, b.threshold + 20, 25]
            RETURN a.id AS aid, b.id AS bid
            ORDER BY aid, bid
        """
        verify_parallel_matches_serial(self.memgraph, query)


class TestInListCacheInvalidation:
    """
    Tests for verifying that the frame change collector properly
    invalidates cached values when dependent symbols change.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        """Set up test data."""
        self.memgraph = memgraph
        clear_database(memgraph)
        memgraph.execute_query(
            """
            UNWIND range(1, 100) AS i
            CREATE (:Item {id: i, group: i % 10, subgroup: i % 5})
            """
        )

    def test_in_list_with_changing_frame_variable(self):
        """
        Test that cache is invalidated when dependent variable changes.
        Uses WITH to change the frame and verify caching works correctly.
        """
        query = """
            MATCH (n:Item)
            WITH n, n.group AS g
            WHERE n.subgroup IN [0, g]
            RETURN n.id AS id
            ORDER BY id
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_with_unwind_changing_variable(self):
        """
        Test IN LIST where the list depends on an UNWIND variable.
        The cache should be invalidated for each UNWIND iteration.
        """
        query = """
            UNWIND [1, 2, 3] AS multiplier
            MATCH (n:Item)
            WHERE n.group IN [0, multiplier]
            RETURN n.id AS id, multiplier
            ORDER BY id, multiplier
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_aggregation_with_variable_list(self):
        """
        Test IN LIST with variable-dependent list in aggregation context.
        """
        query = """
            MATCH (n:Item)
            WITH n.group AS g, collect(n) AS items
            UNWIND items AS item
            WITH item, g
            WHERE item.subgroup IN [0, g % 3]
            RETURN g, count(*) AS cnt
            ORDER BY g
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_variable_changes_through_with(self):
        """
        Test IN LIST where the list variable changes through multiple WITH clauses.
        The cache must be invalidated at each WITH boundary.
        """
        query = """
            MATCH (n:Item)
            WITH n, 1 AS x
            WITH n, x + 1 AS x
            WITH n, x + 1 AS x
            WHERE n.subgroup IN [0, x]
            RETURN n.id AS id, x
            ORDER BY id
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_nested_unwind_changing_variables(self):
        """
        Test IN LIST with nested UNWIND where list depends on outer variable.
        The cache must be invalidated for each combination of outer/inner values.
        """
        query = """
            UNWIND [0, 1, 2] AS outer_val
            UNWIND [0, 1] AS inner_val
            MATCH (n:Item)
            WITH n, outer_val, inner_val
            WHERE n.group IN [outer_val, outer_val + inner_val]
            RETURN n.id AS id, outer_val, inner_val
            ORDER BY id, outer_val, inner_val
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_multiple_variables_change_independently(self):
        """
        Test IN LIST where multiple variables in the list change independently.
        Each variable change should trigger cache invalidation.
        """
        query = """
            UNWIND [0, 2, 4] AS a
            UNWIND [1, 3] AS b
            MATCH (n:Item)
            WITH n, a, b
            WHERE n.subgroup IN [a, b, a + b]
            RETURN n.id AS id, a, b
            ORDER BY id, a, b
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_variable_from_previous_match(self):
        """
        Test IN LIST where the list variable comes from a previous MATCH.
        As different nodes are matched, the list changes.
        """
        # Add more nodes with different structures for this test
        self.memgraph.execute_query(
            """
            UNWIND range(1, 10) AS i
            CREATE (:Selector {id: i, select_value: i % 3})
            """
        )
        query = """
            MATCH (s:Selector)
            MATCH (n:Item)
            WHERE n.subgroup IN [0, s.select_value]
            RETURN s.id AS sid, count(n) AS cnt
            ORDER BY sid
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_expression_changes_each_iteration(self):
        """
        Test IN LIST where the list expression result changes each iteration.
        Uses computed values that differ per row.
        """
        query = """
            MATCH (n:Item)
            WITH n, n.id % 7 AS computed
            WHERE n.group IN [computed, computed + 1, computed - 1]
            RETURN n.id AS id
            ORDER BY id
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_variable_changes_with_reduce(self):
        """
        Test IN LIST where the list variable is computed using reduce.
        The computed value changes based on the current node.
        """
        query = """
            MATCH (n:Item)
            WITH n, reduce(acc = 0, x IN range(1, n.id % 5 + 1) | acc + x) AS sum_val
            WHERE n.group IN [0, sum_val % 10]
            RETURN n.id AS id, sum_val
            ORDER BY id
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_changes_through_orderby_limit(self):
        """
        Test that IN LIST works correctly when combined with ORDER BY and LIMIT
        which may affect execution order.
        """
        query = """
            MATCH (n:Item)
            WITH n ORDER BY n.id DESC LIMIT 50
            WITH n, n.id % 4 AS bucket
            WHERE n.subgroup IN [0, bucket]
            RETURN n.id AS id
            ORDER BY id
        """
        verify_parallel_matches_serial(self.memgraph, query)


class TestInListComplexPatterns:
    """
    Tests for complex IN LIST patterns that exercise various
    aspects of the frame change collector.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        """Set up test data with complex graph structure."""
        self.memgraph = memgraph
        clear_database(memgraph)
        # Create a more complex graph
        memgraph.execute_query(
            """
            UNWIND range(1, 50) AS i
            CREATE (:Product {
                id: i,
                price: i * 10,
                category: i % 5,
                tags: [i % 3, i % 4, i % 5]
            })
            """
        )
        memgraph.execute_query(
            """
            MATCH (p1:Product), (p2:Product)
            WHERE p1.id < p2.id AND p1.category = p2.category
            CREATE (p1)-[:SIMILAR {score: abs(p1.price - p2.price)}]->(p2)
            """
        )

    def test_nested_in_list_operations(self):
        """Test nested IN LIST with multiple conditions."""
        query = """
            MATCH (p:Product)
            WHERE p.category IN [0, 1, 2]
              AND p.id IN range(1, 25)
            RETURN p.id AS id, p.category AS cat
            ORDER BY id
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_with_collect(self):
        """Test IN LIST with collected values."""
        query = """
            MATCH (p:Product)
            WHERE p.category = 0
            WITH collect(p.id) AS ids
            MATCH (p2:Product)
            WHERE p2.id IN ids
            RETURN p2.id AS id
            ORDER BY id
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_with_list_property(self):
        """Test checking if value is in a list property."""
        query = """
            MATCH (p:Product)
            WHERE 0 IN p.tags
            RETURN p.id AS id
            ORDER BY id
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_in_list_with_relationship_aggregation(self):
        """Test IN LIST in relationship pattern with aggregation."""
        query = """
            MATCH (p1:Product)-[r:SIMILAR]->(p2:Product)
            WHERE p1.category IN [0, 1]
            RETURN p1.category AS cat, count(*) AS cnt, avg(r.score) AS avg_score
            ORDER BY cat
        """
        verify_parallel_matches_serial(self.memgraph, query)


class TestInListParallelCorrectness:
    """
    Tests specifically designed to verify correctness of IN LIST
    operations when executed in parallel across multiple workers.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph, num_workers):
        """Set up test data sized for parallel execution."""
        self.memgraph = memgraph
        self.num_workers = num_workers
        clear_database(memgraph)
        # Create enough data to distribute across workers
        memgraph.execute_query(
            f"""
            UNWIND range(1, {num_workers * 100}) AS i
            CREATE (:Data {{
                id: i,
                bucket: i % {num_workers},
                value: i % 20
            }})
            """
        )

    def test_parallel_in_list_distributed_data(self):
        """
        Test IN LIST with data distributed across parallel workers.
        Each worker should process its portion correctly.
        """
        query = """
            MATCH (d:Data)
            WHERE d.value IN [0, 5, 10, 15]
            RETURN d.bucket AS bucket, count(*) AS cnt
            ORDER BY bucket
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_parallel_in_list_with_aggregation(self):
        """
        Test IN LIST with aggregation in parallel execution.
        Verifies that partial results are correctly merged.
        """
        query = """
            MATCH (d:Data)
            WHERE d.value IN range(0, 10)
            RETURN sum(d.id) AS total, count(*) AS cnt
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_parallel_in_list_orderby(self):
        """
        Test IN LIST with ORDER BY in parallel execution.
        """
        query = """
            MATCH (d:Data)
            WHERE d.value IN [1, 3, 5, 7, 9]
            RETURN d.id AS id
            ORDER BY d.id DESC
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_parallel_in_list_with_variable_range(self):
        """
        Test IN LIST with variable-dependent range in parallel.
        """
        query = """
            MATCH (d:Data)
            WHERE d.id IN range(d.bucket * 10, d.bucket * 10 + 5)
            RETURN d.bucket AS bucket, count(*) AS cnt
            ORDER BY bucket
        """
        verify_parallel_matches_serial(self.memgraph, query)


class TestRegexMatchCaching:
    """
    Tests for REGEX operations which also use the frame change collector.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        """Set up test data with string properties."""
        self.memgraph = memgraph
        clear_database(memgraph)
        memgraph.execute_query(
            """
            UNWIND ['apple', 'application', 'banana', 'band', 'candy', 'can', 'dance', 'danger'] AS name
            WITH name, range(1, 10) AS ids
            UNWIND ids AS i
            CREATE (:Word {id: i, name: name, full: name + toString(i)})
            """
        )

    def test_regex_match_basic(self):
        """Test basic regex matching."""
        query = """
            MATCH (w:Word)
            WHERE w.name =~ 'app.*'
            RETURN DISTINCT w.name AS name
            ORDER BY name
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_regex_match_with_aggregation(self):
        """Test regex matching with aggregation."""
        query = """
            MATCH (w:Word)
            WHERE w.name =~ '^[abc].*'
            RETURN w.name AS name, count(*) AS cnt
            ORDER BY name
        """
        verify_parallel_matches_serial(self.memgraph, query)

    def test_regex_match_combined_with_in_list(self):
        """Test combining regex match and IN LIST."""
        query = """
            MATCH (w:Word)
            WHERE w.name =~ 'a.*' AND w.id IN [1, 2, 3]
            RETURN w.name AS name, w.id AS id
            ORDER BY name, id
        """
        verify_parallel_matches_serial(self.memgraph, query)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
