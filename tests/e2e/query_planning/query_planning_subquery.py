import sys

import pytest
from common import memgraph


def test_variable_start_planner_shared_ast_corruption(memgraph):
    """
    Regression test: Variable start planner generates multiple plan variations
    that share AST expression nodes. When one plan is rewritten (index lookup
    optimization removes filter expressions), it must not corrupt the shared AST
    that subsequent plan generations will use.

    Trigger conditions:
    1. OPTIONAL MATCH with edge pattern - causes multiple plan variations
    2. CALL subquery with WITH * - brings symbols into scope visited during plan generation
    3. WHERE with AND containing id() - triggers RemoveExpressions during index rewriting

    Previously, RemoveExpressions mutated AndOperator in-place, corrupting the
    shared AST. Plan 2's generation would then crash when visiting the corrupted
    expression via ReturnBodyContext.

    The left side of AND can be any expression (even literal `true`).
    """
    # Query must complete without crash - empty result expected (no data)
    # Minimal reproduction: undirected edge pattern + WITH * + AND with id()
    result = list(
        memgraph.execute_and_fetch(
            "OPTIONAL MATCH ()-[]-() CALL { MATCH (n) WITH * WHERE true AND id(n) = 0 RETURN n } RETURN n"
        )
    )
    assert result == []


def test_variable_start_planner_nested_and_ast_corruption(memgraph):
    """
    Regression test: Similar to test_variable_start_planner_shared_ast_corruption
    but with a nested AND expression that triggers storage->Create<AndOperator>()
    during expression cloning.

    The nested AND `(true AND id(n) = 0)` requires the cloning logic to create
    a new AndOperator via storage, exercising the deep clone path.
    """
    result = list(
        memgraph.execute_and_fetch(
            "OPTIONAL MATCH ()-[]-() CALL { MATCH (n) WITH * WHERE true AND (true AND id(n) = 0) RETURN n } RETURN n"
        )
    )
    assert result == []


def test_variable_start_planner_nested_and_left_ast_corruption(memgraph):
    """
    Regression test: Variant with nested AND on the left side containing id().

    The nested AND `(id(n) = 0 AND true)` on the left requires cloning when
    the id() lookup is extracted, exercising the path where the nested
    expression containing the filter target is on the left operand.
    """
    result = list(
        memgraph.execute_and_fetch(
            "OPTIONAL MATCH ()-[]-() CALL { MATCH (n) WITH * WHERE (id(n) = 0 AND true) AND true RETURN n } RETURN n"
        )
    )
    assert result == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
