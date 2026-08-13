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


def _plan(memgraph, query):
    return "\n".join(row["QUERY PLAN"] for row in memgraph.execute_and_fetch("EXPLAIN " + query))


def test_scoped_subquery_import_index_result_equivalence(memgraph):
    """
    The seek key inside `CALL (ids) { ... }` is derived from a value that changes on every outer row,
    so assert the indexed plan returns what the unindexed one does across rows carrying different
    lists. Catches a stale or wrongly-scoped seek key, the only way this could give wrong results.
    """
    for i in range(12):
        memgraph.execute(f"CREATE (:L {{p: 'v{i}'}});")

    query = """
        UNWIND [{i: 0, ids: ['v0', 'v1']},
                {i: 1, ids: ['v3']},
                {i: 2, ids: []},
                {i: 3, ids: ['v5', 'v7', 'v11']},
                {i: 4, ids: ['v2', 'v2']},
                {i: 5, ids: ['nope']}] AS row
        WITH row.i AS i, row.ids AS ids
        CALL (ids) {
            MATCH (n:L) WHERE n.p IN ids
            RETURN collect(n.p) AS found
        }
        RETURN i, found ORDER BY i
    """

    def run():
        return [(row["i"], sorted(row["found"])) for row in memgraph.execute_and_fetch(query)]

    expected = [
        (0, ["v0", "v1"]),
        (1, ["v3"]),
        (2, []),
        (3, ["v11", "v5", "v7"]),
        (4, ["v2"]),
        (5, []),
    ]

    unindexed = run()
    assert "ScanAllByLabelProperties" not in _plan(memgraph, query)
    assert unindexed == expected

    memgraph.execute("CREATE INDEX ON :L(p);")
    indexed_plan = _plan(memgraph, query)
    assert "ScanAllByLabelProperties" in indexed_plan, f"index not used inside scoped CALL:\n{indexed_plan}"

    assert run() == unindexed


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
