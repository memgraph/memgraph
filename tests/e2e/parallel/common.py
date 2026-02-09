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
Common utilities for parallel e2e tests.

This module provides utility functions and helpers for writing e2e tests
that can be executed in parallel.

Test Database Scenarios:
    - EMPTY: No data in database
    - SINGLE: One element (1 node pair with relationship)
    - THREAD_COUNT: Number of elements matching parallel worker count
    - LARGE: Large dataset for stress/performance testing
"""

import os
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import *

from neo4j import GraphDatabase, Result

# =============================================================================
# Constants for test scenarios
# =============================================================================

# Default element counts for different scenarios
DEFAULT_SINGLE_COUNT = 1
DEFAULT_THREAD_COUNT = 8  # Fixed value representing internal parallelism
DEFAULT_LARGE_COUNT = 10000


class DatabaseSize(Enum):
    """Database size scenarios for testing."""

    EMPTY = auto()
    SINGLE = auto()
    THREAD_COUNT = auto()
    LARGE = auto()


@dataclass
class DatabaseScenario:
    """Describes a database scenario for testing."""

    size: DatabaseSize
    element_count: int
    description: str


# Predefined scenarios
SCENARIOS = {
    DatabaseSize.EMPTY: DatabaseScenario(DatabaseSize.EMPTY, 0, "Empty database"),
    DatabaseSize.SINGLE: DatabaseScenario(DatabaseSize.SINGLE, 1, "Single element"),
    DatabaseSize.THREAD_COUNT: DatabaseScenario(
        DatabaseSize.THREAD_COUNT, DEFAULT_THREAD_COUNT, "Parallel testing elements"
    ),
    DatabaseSize.LARGE: DatabaseScenario(DatabaseSize.LARGE, DEFAULT_LARGE_COUNT, "Large dataset"),
}


# =============================================================================
# Parallel Execution Query Helpers
# =============================================================================

# The hint to force parallel execution
PARALLEL_HINT = "USING PARALLEL EXECUTION"


def parallel_query(query: str) -> str:
    """
    Wrap a query with USING PARALLEL EXECUTION hint.

    The hint is prepended to the query to force parallel execution.

    Args:
        query: The Cypher query to wrap

    Returns:
        Query with parallel execution hint prepended

    Example:
        >>> parallel_query("MATCH (n) RETURN n")
        'USING PARALLEL EXECUTION MATCH (n) RETURN n'
    """
    query = query.strip()
    # Don't add hint if already present
    if query.upper().startswith("USING PARALLEL"):
        return query
    return f"{PARALLEL_HINT} {query}"


def pq(query: str) -> str:
    """
    Short alias for parallel_query().

    Usage:
        memgraph.fetch_all(pq("MATCH (n) RETURN n"))
    """
    return parallel_query(query)


class ParallelQueryExecutor:
    """
    Wrapper for executing queries with parallel execution hint.

    This class wraps a memgraph instance and automatically adds
    the USING PARALLEL EXECUTION hint to all queries.

    Usage:
        pexec = ParallelQueryExecutor(memgraph)
        result = pexec.fetch_all("MATCH (n) RETURN n")
        # Executes: USING PARALLEL EXECUTION MATCH (n) RETURN n
    """

    def __init__(self, memgraph):
        """
        Initialize with a memgraph instance.

        Args:
            memgraph: MemgraphInstance from fixture
        """
        self._memgraph = memgraph

    def execute_query(self, query: str, params: Optional[Dict] = None) -> None:
        """Execute a query with parallel hint, no return value."""
        self._memgraph.execute_query(parallel_query(query), params)

    def fetch_all(self, query: str, params: Optional[Dict] = None) -> list:
        """Execute a query with parallel hint and return all results."""
        return self._memgraph.fetch_all(parallel_query(query), params)

    def fetch_one(self, query: str, params: Optional[Dict] = None):
        """Execute a query with parallel hint and return first result."""
        return self._memgraph.fetch_one(parallel_query(query), params)

    def get_driver(self):
        """Get the underlying neo4j driver."""
        return self._memgraph.get_driver()

    @property
    def memgraph(self):
        """Access the underlying memgraph instance."""
        return self._memgraph


def get_file_path(file: str) -> str:
    """Get the absolute path to a file in the test directory."""
    return os.path.join(Path(__file__).parent.absolute(), file)


def execute_and_fetch_all(driver, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
    """
    Execute a query and return all results as a list of dictionaries.

    Args:
        driver: Neo4j driver instance
        query: Cypher query to execute
        params: Optional query parameters

    Returns:
        List of result records as dictionaries
    """
    with driver.session() as session:
        result = session.run(query, params or {})
        return [record.data() for record in result]


def execute_query(driver, query: str, params: Optional[Dict] = None) -> None:
    """
    Execute a query without returning results.

    Args:
        driver: Neo4j driver instance
        query: Cypher query to execute
        params: Optional query parameters
    """
    with driver.session() as session:
        session.run(query, params or {})


def get_query_summary(driver, query: str, params: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Execute a query and return the result summary metadata.

    Args:
        driver: Neo4j driver instance
        query: Cypher query to execute
        params: Optional query parameters

    Returns:
        Summary metadata dictionary
    """
    with driver.session() as session:
        result = session.run(query, params or {})
        return result.consume().metadata


def create_test_graph(driver, graph_type: str = "simple") -> None:
    """
    Create a standard test graph for testing.

    Args:
        driver: Neo4j driver instance
        graph_type: Type of graph to create ('simple', 'chain', 'tree')
    """
    # Clear existing data
    execute_query(driver, "MATCH (n) DETACH DELETE n")

    if graph_type == "simple":
        execute_query(
            driver,
            """
            CREATE (a:Person {name: 'Alice', age: 30})
            CREATE (b:Person {name: 'Bob', age: 25})
            CREATE (c:Person {name: 'Charlie', age: 35})
            CREATE (a)-[:KNOWS {since: 2020}]->(b)
            CREATE (b)-[:KNOWS {since: 2021}]->(c)
            CREATE (a)-[:FRIENDS]->(c)
            """,
        )
    elif graph_type == "chain":
        execute_query(
            driver,
            """
            CREATE (a:Node {id: 1})-[:NEXT]->(b:Node {id: 2})-[:NEXT]->(c:Node {id: 3})
                   -[:NEXT]->(d:Node {id: 4})-[:NEXT]->(e:Node {id: 5})
            """,
        )
    elif graph_type == "tree":
        execute_query(
            driver,
            """
            CREATE (root:Node {name: 'root'})
            CREATE (l1:Node {name: 'left1'})
            CREATE (r1:Node {name: 'right1'})
            CREATE (l2:Node {name: 'left2'})
            CREATE (r2:Node {name: 'right2'})
            CREATE (root)-[:CHILD]->(l1)
            CREATE (root)-[:CHILD]->(r1)
            CREATE (l1)-[:CHILD]->(l2)
            CREATE (r1)-[:CHILD]->(r2)
            """,
        )


# =============================================================================
# Database Setup Functions
# =============================================================================


def clear_database(memgraph) -> None:
    """Clear all data from the database."""
    memgraph.execute_query("MATCH (n) DETACH DELETE n")


def setup_empty_db(memgraph) -> int:
    """
    Set up an empty database.

    Args:
        memgraph: Memgraph instance from fixture

    Returns:
        Number of elements created (0)
    """
    clear_database(memgraph)
    return 0


def setup_single_element_db(memgraph) -> int:
    """
    Set up database with a single element (node pair with relationship).

    Creates: (:A{p:1})-[:E{p:1}]->(:B{p:1})

    Args:
        memgraph: Memgraph instance from fixture

    Returns:
        Number of elements created (1)
    """
    clear_database(memgraph)
    memgraph.execute_query("CREATE (:A{p:1})-[:E{p:1}]->(:B{p:1})")
    return 1


def setup_thread_count_db(memgraph, thread_count: Optional[int] = None) -> int:
    """
    Set up database with elements matching the thread/worker count.

    Creates N elements where N is the number of parallel workers.
    Each element is: (:A{p:i})-[:E{p:i}]->(:B{p:i})

    Args:
        memgraph: Memgraph instance from fixture
        thread_count: Number of elements to create (defaults to DEFAULT_THREAD_COUNT)

    Returns:
        Number of elements created
    """
    count = thread_count or DEFAULT_THREAD_COUNT
    clear_database(memgraph)
    if count > 0:
        memgraph.execute_query(f"UNWIND range(1, {count}) AS i CREATE (:A{{p:i}})-[:E{{p:i}}]->(:B{{p:i}})")
    return count


def setup_large_db(memgraph, element_count: Optional[int] = None) -> int:
    """
    Set up database with a large number of elements.

    Creates N elements for stress/performance testing.
    Each element is: (:A{p:i})-[:E{p:i}]->(:B{p:i})

    Args:
        memgraph: Memgraph instance from fixture
        element_count: Number of elements to create (defaults to DEFAULT_LARGE_COUNT)

    Returns:
        Number of elements created
    """
    count = element_count or DEFAULT_LARGE_COUNT
    clear_database(memgraph)
    if count > 0:
        memgraph.execute_query(f"UNWIND range(1, {count}) AS i CREATE (:A{{p:i}})-[:E{{p:i}}]->(:B{{p:i}})")
    return count


def setup_database(memgraph, size: DatabaseSize, custom_count: Optional[int] = None) -> int:
    """
    Set up database according to the specified size scenario.

    Args:
        memgraph: Memgraph instance from fixture
        size: DatabaseSize enum value
        custom_count: Optional custom element count (for THREAD_COUNT and LARGE)

    Returns:
        Number of elements created
    """
    if size == DatabaseSize.EMPTY:
        return setup_empty_db(memgraph)
    elif size == DatabaseSize.SINGLE:
        return setup_single_element_db(memgraph)
    elif size == DatabaseSize.THREAD_COUNT:
        return setup_thread_count_db(memgraph, custom_count)
    elif size == DatabaseSize.LARGE:
        return setup_large_db(memgraph, custom_count)
    else:
        raise ValueError(f"Unknown database size: {size}")


def get_element_counts(memgraph) -> Dict[str, int]:
    """
    Get counts of nodes and relationships in the database.

    Args:
        memgraph: Memgraph instance from fixture

    Returns:
        Dictionary with 'nodes', 'relationships', 'a_nodes', 'b_nodes', 'edges' counts
    """
    result = memgraph.fetch_one(
        """
        MATCH (n)
        OPTIONAL MATCH ()-[r]->()
        WITH count(DISTINCT n) AS nodes, count(DISTINCT r) AS rels
        OPTIONAL MATCH (a:A) WITH nodes, rels, count(a) AS a_count
        OPTIONAL MATCH (b:B) WITH nodes, rels, a_count, count(b) AS b_count
        OPTIONAL MATCH ()-[e:E]->() WITH nodes, rels, a_count, b_count, count(e) AS e_count
        RETURN nodes, rels, a_count AS a_nodes, b_count AS b_nodes, e_count AS edges
        """
    )
    return dict(result) if result else {"nodes": 0, "relationships": 0, "a_nodes": 0, "b_nodes": 0, "edges": 0}


# =============================================================================
# Test Runner Helpers
# =============================================================================


def run_on_all_scenarios(
    memgraph,
    query: str,
    params: Optional[Dict] = None,
    thread_count: Optional[int] = None,
    large_count: Optional[int] = None,
) -> Dict[DatabaseSize, Any]:
    """
    Run a query on all database size scenarios.

    Args:
        memgraph: Memgraph instance from fixture
        query: Cypher query to execute
        params: Optional query parameters
        thread_count: Custom count for THREAD_COUNT scenario
        large_count: Custom count for LARGE scenario

    Returns:
        Dictionary mapping DatabaseSize to query results
    """
    results = {}

    for size in DatabaseSize:
        if size == DatabaseSize.EMPTY:
            setup_empty_db(memgraph)
        elif size == DatabaseSize.SINGLE:
            setup_single_element_db(memgraph)
        elif size == DatabaseSize.THREAD_COUNT:
            setup_thread_count_db(memgraph, thread_count)
        elif size == DatabaseSize.LARGE:
            setup_large_db(memgraph, large_count)

        results[size] = memgraph.fetch_all(query, params)

    return results


def scenario_test(
    sizes: Optional[List[DatabaseSize]] = None,
    thread_count: Optional[int] = None,
    large_count: Optional[int] = None,
):
    """
    Decorator to run a test function on multiple database scenarios.

    Usage:
        @scenario_test(sizes=[DatabaseSize.EMPTY, DatabaseSize.SINGLE])
        def test_my_query(memgraph, scenario: DatabaseScenario):
            # Test code here
            pass

    Args:
        sizes: List of DatabaseSize values to test (defaults to all)
        thread_count: Custom count for THREAD_COUNT scenario
        large_count: Custom count for LARGE scenario
    """
    import pytest

    test_sizes = sizes or list(DatabaseSize)

    def decorator(func: Callable):
        @pytest.mark.parametrize(
            "db_size",
            test_sizes,
            ids=[s.name.lower() for s in test_sizes],
        )
        def wrapper(memgraph, db_size: DatabaseSize, *args, **kwargs):
            # Set up database for this scenario
            if db_size == DatabaseSize.EMPTY:
                count = setup_empty_db(memgraph)
            elif db_size == DatabaseSize.SINGLE:
                count = setup_single_element_db(memgraph)
            elif db_size == DatabaseSize.THREAD_COUNT:
                count = setup_thread_count_db(memgraph, thread_count)
            elif db_size == DatabaseSize.LARGE:
                count = setup_large_db(memgraph, large_count)
            else:
                count = 0

            scenario = DatabaseScenario(db_size, count, SCENARIOS[db_size].description)
            return func(memgraph, scenario, *args, **kwargs)

        return wrapper

    return decorator


def all_scenarios_generator(
    memgraph,
    thread_count: Optional[int] = None,
    large_count: Optional[int] = None,
) -> Generator[DatabaseScenario, None, None]:
    """
    Generator that yields database scenarios after setting up the database.

    Usage:
        for scenario in all_scenarios_generator(memgraph):
            result = memgraph.fetch_all("MATCH (n) RETURN count(n)")
            # assertions...

    Args:
        memgraph: Memgraph instance from fixture
        thread_count: Custom count for THREAD_COUNT scenario
        large_count: Custom count for LARGE scenario

    Yields:
        DatabaseScenario for each size
    """
    for size in DatabaseSize:
        count = setup_database(
            memgraph,
            size,
            custom_count=thread_count
            if size == DatabaseSize.THREAD_COUNT
            else large_count
            if size == DatabaseSize.LARGE
            else None,
        )
        yield DatabaseScenario(size, count, SCENARIOS[size].description)


# =============================================================================
# Query Builder
# =============================================================================


class QueryBuilder:
    """Simple query builder for common patterns."""

    @staticmethod
    def create_node(label: str, properties: Optional[Dict] = None) -> str:
        """Generate a CREATE node query."""
        props = ""
        if properties:
            props_str = ", ".join(f"{k}: ${k}" for k in properties.keys())
            props = f" {{{props_str}}}"
        return f"CREATE (n:{label}{props}) RETURN n"

    @staticmethod
    def create_elements(count: int) -> str:
        """Generate a query to create N elements with the standard pattern."""
        return f"UNWIND range(1, {count}) AS i CREATE (:A{{p:i}})-[:E{{p:i}}]->(:B{{p:i}})"

    @staticmethod
    def match_all(label: Optional[str] = None) -> str:
        """Generate a MATCH query for all nodes."""
        label_str = f":{label}" if label else ""
        return f"MATCH (n{label_str}) RETURN n"

    @staticmethod
    def count_nodes(label: Optional[str] = None) -> str:
        """Generate a query to count nodes."""
        label_str = f":{label}" if label else ""
        return f"MATCH (n{label_str}) RETURN count(n) AS count"

    @staticmethod
    def count_relationships(rel_type: Optional[str] = None) -> str:
        """Generate a query to count relationships."""
        rel_str = f":{rel_type}" if rel_type else ""
        return f"MATCH ()-[r{rel_str}]->() RETURN count(r) AS count"

    @staticmethod
    def delete_all() -> str:
        """Generate a query to delete all nodes and relationships."""
        return "MATCH (n) DETACH DELETE n"
