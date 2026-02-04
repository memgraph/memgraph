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
Tests for exception handling in parallel query execution.

This module tests that exceptions are properly handled regardless of which
parallel branch they originate from:
    - Main branch (first element triggers exception)
    - Last branch (last element triggers exception)
    - Multiple branches (multiple elements trigger exception)

The test strategy uses MATCH(n) RETURN min(n.p) where mixing string and int
values for property 'p' causes a type error during aggregation.

All queries use USING PARALLEL EXECUTION hint via the pq() wrapper.
"""

import sys
from enum import Enum, auto
from typing import List, Optional

import pytest
from common import *
from common import pq
from neo4j.exceptions import *

# =============================================================================
# Exception Origin Types
# =============================================================================


class ExceptionOrigin(Enum):
    """Defines where the exception-triggering element is located."""

    FIRST = auto()  # First element (main branch)
    LAST = auto()  # Last element (last parallel branch)
    MIDDLE = auto()  # Middle element
    EVERY_OTHER = auto()  # Every other element (multiple branches)
    ALL = auto()  # All elements have invalid type


# =============================================================================
# Setup Helpers
# =============================================================================


def inject_type_error(memgraph, positions: List[int]) -> None:
    """
    Inject type errors by changing p property to string at given positions.

    Args:
        memgraph: Memgraph instance
        positions: List of p values to convert to strings
    """
    for pos in positions:
        memgraph.execute_query(f"MATCH (n:A {{p: {pos}}}) SET n.p = 'invalid_string_{pos}'")


def setup_with_type_error(
    memgraph,
    element_count: int,
    origin: ExceptionOrigin,
) -> List[int]:
    """
    Set up database and inject type errors at specified positions.

    Args:
        memgraph: Memgraph instance
        element_count: Total number of elements to create
        origin: Where to inject the type error

    Returns:
        List of positions where type errors were injected
    """
    if element_count == 0:
        return []

    # Create the base data
    clear_database(memgraph)
    memgraph.execute_query(f"UNWIND range(1, {element_count}) AS i CREATE (:A{{p:i}})-[:E{{p:i}}]->(:B{{p:i}})")

    # Determine which positions to corrupt
    error_positions = []

    if origin == ExceptionOrigin.FIRST:
        error_positions = [1]
    elif origin == ExceptionOrigin.LAST:
        error_positions = [element_count]
    elif origin == ExceptionOrigin.MIDDLE:
        error_positions = [element_count // 2] if element_count > 1 else [1]
    elif origin == ExceptionOrigin.EVERY_OTHER:
        error_positions = list(range(1, element_count + 1, 2))  # 1, 3, 5, ...
    elif origin == ExceptionOrigin.ALL:
        error_positions = list(range(1, element_count + 1))

    # Inject the type errors
    inject_type_error(memgraph, error_positions)

    return error_positions


def run_aggregation_query(memgraph) -> None:
    """
    Run an aggregation query with parallel execution that will fail on mixed types.

    This uses min() which requires comparable types.
    """
    memgraph.fetch_all(pq("MATCH (n:A) RETURN min(n.p) AS result"))


# =============================================================================
# Test Classes
# =============================================================================


class TestExceptionOnEmptyDatabase:
    """Test exception handling on empty database."""

    def test_no_exception_on_empty(self, memgraph):
        """Empty database should not raise exception - no data to aggregate."""
        setup_empty_db(memgraph)

        # Should not raise - min of nothing is null
        result = memgraph.fetch_all(pq("MATCH (n:A) RETURN min(n.p) AS result"))
        assert len(result) == 1
        assert result[0]["result"] is None


class TestExceptionOnSingleElement:
    """Test exception handling on single element database."""

    def test_no_exception_single_valid(self, memgraph):
        """Single valid element should not raise exception."""
        setup_single_element_db(memgraph)

        result = memgraph.fetch_all(pq("MATCH (n:A) RETURN min(n.p) AS result"))
        assert result[0]["result"] == 1

    def test_exception_single_invalid(self, memgraph):
        """Single element with string should work (no type conflict)."""
        setup_single_element_db(memgraph)
        inject_type_error(memgraph, [1])

        # Single string element - min of one string is that string
        result = memgraph.fetch_all(pq("MATCH (n:A) RETURN min(n.p) AS result"))
        assert result[0]["result"] == "invalid_string_1"


class TestExceptionParameterized:
    """Consolidated parameterized tests covering all combinations of size and origin."""

    @pytest.mark.parametrize("element_count", [2, 4, 8, 16, 100, 1000])
    @pytest.mark.parametrize(
        "origin",
        [ExceptionOrigin.FIRST, ExceptionOrigin.LAST, ExceptionOrigin.MIDDLE, ExceptionOrigin.EVERY_OTHER],
    )
    def test_exception_matrix(self, memgraph, element_count, origin):
        """Test exception handling across different sizes and origins."""
        setup_with_type_error(memgraph, element_count, origin)

        with pytest.raises((DatabaseError, ClientError, TransientError)):
            run_aggregation_query(memgraph)

    def test_no_exception_all_valid(self, memgraph, num_workers):
        """Baseline check: all valid integers should aggregate without exception."""
        setup_thread_count_db(memgraph, num_workers)
        result = memgraph.fetch_all(pq("MATCH (n:A) RETURN min(n.p) AS result"))
        assert result[0]["result"] == 1


class TestExceptionMessageConsistency:
    """Test that exception messages are consistent regardless of origin."""

    def test_exception_message_same_for_different_origins(self, memgraph):
        """Exception message should be consistent regardless of which branch triggers it."""
        element_count = 10
        exception_messages = []

        for origin in [ExceptionOrigin.FIRST, ExceptionOrigin.MIDDLE, ExceptionOrigin.LAST]:
            setup_with_type_error(memgraph, element_count, origin)

            try:
                run_aggregation_query(memgraph)
                pytest.fail(f"Expected exception for origin {origin}")
            except (DatabaseError, ClientError) as e:
                exception_messages.append(str(e))

        # All exception messages should indicate type error
        for msg in exception_messages:
            assert "unable to get min" in msg.lower(), f"Exception message doesn't match: {msg}"


class TestNoExceptionCases:
    """Test cases where no exception should occur."""

    def test_all_integers(self, memgraph):
        """All integers should aggregate without exception."""
        setup_thread_count_db(memgraph, 10)

        result = memgraph.fetch_all(pq("MATCH (n:A) RETURN min(n.p) AS min_val, max(n.p) AS max_val"))
        assert result[0]["min_val"] == 1
        assert result[0]["max_val"] == 10

    def test_all_strings(self, memgraph):
        """All strings should aggregate without exception."""
        setup_thread_count_db(memgraph, 10)
        setup_with_type_error(memgraph, 10, ExceptionOrigin.ALL)

        result = memgraph.fetch_all(pq("MATCH (n:A) RETURN min(n.p) AS min_val, max(n.p) AS max_val"))
        assert isinstance(result[0]["min_val"], str)
        assert isinstance(result[0]["max_val"], str)

    def test_sum_with_nulls_no_exception(self, memgraph):
        """SUM should handle nulls gracefully."""
        clear_database(memgraph)
        memgraph.execute_query(
            """
            CREATE (:A {p: 1})
            CREATE (:A {p: 2})
            CREATE (:A)
            CREATE (:A {p: 4})
            """
        )

        result = memgraph.fetch_all(pq("MATCH (n:A) RETURN sum(n.p) AS total"))
        assert result[0]["total"] == 7  # 1 + 2 + 4, null is ignored


class TestDifferentAggregationFunctions:
    """Test that different aggregation functions handle type errors correctly."""

    @pytest.fixture(autouse=True)
    def setup_mixed_types(self, memgraph):
        """Set up database with mixed types for each test."""
        setup_with_type_error(memgraph, 10, ExceptionOrigin.FIRST)

    def test_min_exception(self, memgraph):
        """MIN should raise on mixed types."""
        with pytest.raises((DatabaseError, ClientError, TransientError)):
            memgraph.fetch_all(pq("MATCH (n:A) RETURN min(n.p)"))

    def test_max_exception(self, memgraph):
        """MAX should raise on mixed types."""
        with pytest.raises((DatabaseError, ClientError, TransientError)):
            memgraph.fetch_all(pq("MATCH (n:A) RETURN max(n.p)"))

    def test_sum_exception(self, memgraph):
        """SUM should raise on string types."""
        with pytest.raises((DatabaseError, ClientError, TransientError)):
            memgraph.fetch_all(pq("MATCH (n:A) RETURN sum(n.p)"))

    def test_avg_exception(self, memgraph):
        """AVG should raise on string types."""
        with pytest.raises((DatabaseError, ClientError, TransientError)):
            memgraph.fetch_all(pq("MATCH (n:A) RETURN avg(n.p)"))

    def test_count_no_exception(self, memgraph):
        """COUNT should work regardless of types."""
        result = memgraph.fetch_all(pq("MATCH (n:A) RETURN count(n.p) AS cnt"))
        assert result[0]["cnt"] == 10

    def test_collect_no_exception(self, memgraph):
        """COLLECT should work regardless of types."""
        result = memgraph.fetch_all(pq("MATCH (n:A) RETURN collect(n.p) AS vals"))
        assert len(result[0]["vals"]) == 10


class TestArithmeticExceptions:
    """Test arithmetic exceptions (like division by zero) in parallel execution."""

    def test_division_by_zero_parallel(self, memgraph):
        """Division by zero should raise exception in parallel execution."""
        setup_thread_count_db(memgraph, 10)

        with pytest.raises((DatabaseError, ClientError, TransientError)) as excinfo:
            # Force parallel execution and trigger division by zero for some elements
            memgraph.fetch_all(pq("MATCH (n:A) RETURN n.p / (n.p - 5) AS result"))

        assert "invalid types" in str(excinfo.value).lower()


class TestOrderByExceptions:
    """Test exceptions during ORDER BY in parallel execution."""

    @pytest.mark.parametrize(
        "origin",
        [ExceptionOrigin.FIRST, ExceptionOrigin.LAST, ExceptionOrigin.MIDDLE, ExceptionOrigin.EVERY_OTHER],
    )
    def test_order_by_mixed_types_exception(self, memgraph, origin):
        """Ordering mixed types should raise exception if not handled gracefully."""
        setup_with_type_error(memgraph, 10, origin)

        with pytest.raises((DatabaseError, ClientError, TransientError)):
            memgraph.fetch_all(pq("MATCH (n:A) RETURN n.p AS p ORDER BY p"))

    def test_order_by_all_strings_no_exception(self, memgraph):
        """Ordering all strings should work without exception."""
        setup_with_type_error(memgraph, 10, ExceptionOrigin.ALL)

        result = memgraph.fetch_all(pq("MATCH (n:A) RETURN n.p AS p ORDER BY p"))
        assert len(result) == 10
        # Lexicographical order: "invalid_string_1", "invalid_string_10", "invalid_string_2", ...
        p_values = [r["p"] for r in result]
        assert p_values == sorted(p_values)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
