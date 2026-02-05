import sys
from enum import Enum, auto

import pytest
from common import *


class TargetBranch(Enum):
    FIRST = auto()  # First element/thread
    LAST = auto()  # Last element/thread
    EVERY_OTHER = auto()  # Every other element (dispersed)
    ALL = auto()  # All elements


class SyntaxOrder(Enum):
    PARALLEL_FIRST = auto()  # USING PARALLEL ..., HOPS LIMIT ...
    LIMIT_FIRST = auto()  # USING HOPS LIMIT ..., PARALLEL ...


def get_filter_and_expected_count(target: TargetBranch, element_count: int, thread_count: int) -> tuple[str, int]:
    """
    Returns the WHERE clause part and the expected return count.
    Assumes data is (:A {p: i}) where i in 1..count.
    """
    if element_count == 0:
        return "1=1", 0

    if target == TargetBranch.FIRST:
        # User formula: n.p < n elements / n threads * 0.7 (min 1)
        limit_val = (element_count / thread_count) * 0.7
        limit_int = max(1, int(limit_val))
        return f"n.p <= {limit_int}", limit_int

    elif target == TargetBranch.LAST:
        # User formula: n.p > n elements - n elements / n threads * 0.7 (max n-1)
        threshold_val = element_count - (element_count / thread_count) * 0.7
        threshold_int = min(element_count - 1, int(threshold_val))
        expected = element_count - threshold_int
        return f"n.p > {threshold_int}", expected

    elif target == TargetBranch.EVERY_OTHER:
        # Target odd numbers: 1, 3, 5...
        expected = (element_count + 1) // 2
        return "n.p % 2 = 1", expected

    elif target == TargetBranch.ALL:
        return "n.p > 0", element_count

    return "1=1", element_count


def build_query(syntax: SyntaxOrder, limit: int, filter_clause: str) -> str:
    base_match = f"MATCH (n:A)-[:E]->(m:B) WHERE {filter_clause} RETURN n"

    if syntax == SyntaxOrder.PARALLEL_FIRST:
        return f"USING PARALLEL EXECUTION, HOPS LIMIT {limit} {base_match}"
    elif syntax == SyntaxOrder.LIMIT_FIRST:
        return f"USING HOPS LIMIT {limit}, PARALLEL EXECUTION {base_match}"
    return base_match


@pytest.mark.parametrize(
    "db_size",
    [DatabaseSize.EMPTY, DatabaseSize.SINGLE, DatabaseSize.THREAD_COUNT, DatabaseSize.LARGE],
    ids=lambda s: s.name.lower(),
)
@pytest.mark.parametrize("target", [TargetBranch.FIRST, TargetBranch.LAST, TargetBranch.EVERY_OTHER, TargetBranch.ALL])
@pytest.mark.parametrize("syntax", [SyntaxOrder.PARALLEL_FIRST, SyntaxOrder.LIMIT_FIRST])
@pytest.mark.parametrize("limit_scenario", ["SUFFICIENT", "INSUFFICIENT"])
def test_hops_limit_correctness(memgraph, db_size, target, syntax, num_workers, limit_scenario):
    """
    Verify that parallel execution with Hops Limit behaves correctly:
    - SUFFICIENT: Limit is high, expects full results.
    - INSUFFICIENT: Limit is low, expects partial results (limit reached).
    """
    # Manual setup replacing @scenario_test
    if db_size == DatabaseSize.EMPTY:
        count = setup_empty_db(memgraph)
    elif db_size == DatabaseSize.SINGLE:
        count = setup_single_element_db(memgraph)
    elif db_size == DatabaseSize.THREAD_COUNT:
        count = setup_thread_count_db(memgraph)
    elif db_size == DatabaseSize.LARGE:
        count = setup_large_db(memgraph)

    scenario = DatabaseScenario(db_size, count, SCENARIOS[db_size].description)

    filter_clause, expected_count = get_filter_and_expected_count(target, scenario.element_count, num_workers)

    # Calculate limit based on scenario
    if limit_scenario == "SUFFICIENT":
        # High limit: ensure we get all expected results
        limit = max(scenario.element_count * 2, 1000)
    else:
        # Insufficient limit: set it lower than expected count to force cutoff
        # Only makes sense if expected_count > 0.
        if expected_count <= 1:
            pytest.skip("Not enough elements to test insufficient limit")
        limit = max(1, expected_count // 2)

    query = build_query(syntax, limit, filter_clause)

    try:
        result = memgraph.fetch_all(query)
        count_res = len(result)

        if limit_scenario == "SUFFICIENT":
            assert count_res == expected_count, (
                f"Full results expected. Scenario: {scenario.description}, "
                f"Target: {target}, Syntax: {syntax}, Got: {count_res}, Expected: {expected_count}"
            )
        else:
            # INSUFFICIENT: Should not exceed expected_count (obviously)
            # AND should essentially be bounded by limit.
            assert count_res == limit, (
                f"Partial results expected. Scenario: {scenario.description}, "
                f"Target: {target}, Syntax: {syntax}, Limit: {limit}, Got: {count_res}"
            )
            # Also verify we got SOME results if limit > 0
            if limit > 0:
                assert count_res > 0, "Expected some partial results, got 0"

    except Exception as e:
        raise e


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
