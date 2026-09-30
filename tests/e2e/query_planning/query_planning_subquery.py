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


def test_scoped_subquery_import_with_optional_and_edge_index(memgraph):
    """
    An OPTIONAL MATCH filtering an edge on the imported variable, with an edge index present. Covers
    both import forms, and is the only coverage of CALL (*) / all_variables_scoped_.

    Has to be e2e: PlanValidator rejects indexed edge scans under an Optional whose branch and input
    share a symbol, and the query_plan.cpp harness runs the rewriters but never calls IsValidPlan.
    """
    memgraph.execute("CREATE EDGE INDEX ON :T;")
    try:
        memgraph.execute("CREATE (a:L {p: 1}), (b:L {p: 2});")
        memgraph.execute("MATCH (a:L {p: 1}), (b:L {p: 2}) CREATE (a)-[:T {p: 1}]->(b);")

        query = """
            WITH 1 AS v
            CALL (v) { MATCH (a) OPTIONAL MATCH ()-[e:T]->() WHERE e.p = v RETURN a, e }
            RETURN count(*) AS c
        """
        assert [row["c"] for row in memgraph.execute_and_fetch(query)] == [2]
        # CALL (*) reaches the same code path via all_variables_scoped_.
        star = query.replace("CALL (v)", "CALL (*)")
        assert [row["c"] for row in memgraph.execute_and_fetch(star)] == [2]
    finally:
        # the memgraph fixture's drop_indexes() does not cover edge indexes
        memgraph.execute("DROP EDGE INDEX ON :T;")


def test_scoped_subquery_import_does_not_convert_unrelated_cartesian(memgraph):
    """
    The Cartesian -> IndexedJoin conversion must fire only when a removed filter spans both branches.
    Converting otherwise re-executes the sub-branch once per main row, and forfeits the HashJoin
    because JoinRewriter runs later and only rewrites Cartesian.
    """
    memgraph.execute("CREATE INDEX ON :L(p);")
    memgraph.execute("UNWIND range(1, 5) AS i CREATE (:A {id: i});")
    memgraph.execute("UNWIND range(1, 5) AS i CREATE (:B {id: i});")
    memgraph.execute("UNWIND range(1, 3) AS i CREATE (:L {p: 1, q: i});")

    # c.p = v touches one side only, so the Cartesian must survive. No UNWIND in between, so this
    # actually reaches the conversion decision.
    query = """
        WITH 1 AS v
        CALL (v) { MATCH (a:A), (c:L) WHERE c.p = v RETURN a, c }
        RETURN count(*) AS c
    """
    plan = _plan(memgraph, query)
    assert "ScanAllByLabelProperties" in plan, f"the fix itself stopped working:\n{plan}"
    assert "IndexedJoin" not in plan, f"unrelated Cartesian was converted:\n{plan}"
    assert "Cartesian" in plan, f"Cartesian did not survive:\n{plan}"
    assert [row["c"] for row in memgraph.execute_and_fetch(query)] == [15]

    # The gate must not under-fire: a genuine cross-branch filter still has to convert.
    joining = """
        WITH 1 AS v
        CALL (v) { MATCH (a:A), (c:L) WHERE c.p = a.id RETURN a, c }
        RETURN count(*) AS c
    """
    joining_plan = _plan(memgraph, joining)
    assert "ScanAllByLabelProperties" in joining_plan, f"cross-branch seek was lost:\n{joining_plan}"
    assert "Cartesian" not in joining_plan, f"cross-branch filter left a Cartesian:\n{joining_plan}"
    assert [row["c"] for row in memgraph.execute_and_fetch(joining)] == [3]


def test_scoped_subquery_import_index_result_equivalence(memgraph):
    """
    The seek key inside `CALL (ids) { ... }` changes on every outer row, so the indexed plan must
    return what the unindexed one does across rows carrying different lists. Catches a stale or
    wrongly-scoped seek key.
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
