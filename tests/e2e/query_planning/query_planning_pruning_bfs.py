# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import sys

import pytest
from common import memgraph

QUERY_PLAN = "QUERY PLAN"


def get_plan(memgraph, query):
    return [x[QUERY_PLAN] for x in memgraph.execute_and_fetch(f"EXPLAIN {query}")]


def operator_names(plan):
    return [line.strip().removeprefix("* ").split(" ")[0] for line in plan]


@pytest.fixture(autouse=True)
def setup_graph(memgraph):
    memgraph.execute(
        "CREATE (a:N {id: 'a'})-[:TO]->(b:N {id: 'b'})-[:TO]->(c:N {id: 'c'}), "
        "(a)-[:TO]->(d:N {id: 'd'})-[:TO]->(e:N {id: 'e'}), "
        "(d)-[:TO]->(c);"
    )
    yield
    memgraph.drop_database()


# === Plan shape tests: rewrite should fire ===


def test_pruning_bfs_when_edges_unused(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*1..5]->(b) RETURN DISTINCT b")
    ops = operator_names(plan)
    assert "PruningBFSExpand" in ops, f"Expected PruningBFSExpand in plan, got: {plan}"


def test_no_rewrite_with_plain_aggregation(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*]->(b) RETURN count(b)")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_pruning_bfs_with_count_distinct(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*]->(b) RETURN count(DISTINCT b)")
    ops = operator_names(plan)
    assert "PruningBFSExpand" in ops, f"Expected PruningBFSExpand in plan, got: {plan}"


def test_pruning_bfs_with_collect_distinct(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*]->(b) RETURN collect(DISTINCT b)")
    ops = operator_names(plan)
    assert "PruningBFSExpand" in ops, f"Expected PruningBFSExpand in plan, got: {plan}"


def test_no_rewrite_with_mixed_aggregation(memgraph):
    """One DISTINCT agg + one plain agg on target: must NOT rewrite."""
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*]->(b) RETURN count(DISTINCT b), count(b)")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_when_distinct_above_plain_aggregate(memgraph):
    """DISTINCT above a non-distinct Aggregate must not leak deduplicates_ downward."""
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*]->(b) WITH count(b) AS cnt, a RETURN DISTINCT a, cnt")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_pruning_bfs_with_filter_lambda(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*1..5 (e, n | n:N)]->(b) RETURN DISTINCT b")
    ops = operator_names(plan)
    assert "PruningBFSExpand" in ops, f"Expected PruningBFSExpand in plan, got: {plan}"


def test_pruning_bfs_undirected(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*1..3]-(b) RETURN DISTINCT b")
    ops = operator_names(plan)
    assert "PruningBFSExpand" in ops, f"Expected PruningBFSExpand in plan, got: {plan}"


# === Plan shape tests: rewrite should NOT fire ===


def test_no_rewrite_without_distinct(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*]->(b) RETURN b")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_with_return_star(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*]->(b) RETURN *")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_when_edges_used(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[r*1..5]->(b) RETURN r, b")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_when_named_path_used(memgraph):
    plan = get_plan(memgraph, "MATCH p=(a:N {id: 'a'})-[*1..5]->(b) RETURN p")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_for_explicit_bfs(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*BFS 1..5]->(b) RETURN b")
    ops = operator_names(plan)
    assert "BFSExpand" in ops, f"Expected BFSExpand in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_when_existing_node(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'}), (b:N {id: 'c'}) WITH a, b MATCH (a)-[*]->(b) RETURN DISTINCT b")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_with_accumulated_path_lambda(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N)-[* (e, n, p | size(nodes(p)) < 5)]->(b) RETURN DISTINCT b")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_with_multi_expand(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N)-[*]->(b)-[*]->(c) RETURN DISTINCT c")
    ops = operator_names(plan)
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_leaks_across_union(memgraph):
    plan = get_plan(memgraph, "MATCH (a)-[*]->(b) RETURN DISTINCT b UNION ALL MATCH (a)-[*]->(b) RETURN b")
    plan_text = "\n".join(plan)
    assert plan_text.count("PruningBFSExpand") == 1, f"Right branch should not be rewritten, got: {plan}"
    assert "ExpandVariable" in plan_text, f"Expected ExpandVariable in right branch, got: {plan}"


# === Correctness tests: pruning BFS results == DFS + DISTINCT ===


def test_correctness_directed(memgraph):
    results = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*]->(b) RETURN DISTINCT b"))
    expected_ids = {"b", "c", "d", "e"}
    actual_ids = {row["b"]._properties["id"] for row in results}
    assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"


def test_correctness_with_cycle(memgraph):
    memgraph.drop_database()
    memgraph.execute("CREATE (a:N {id: 'a'})-[:TO]->(b:N {id: 'b'})-[:TO]->(c:N {id: 'c'})-[:TO]->(a);")
    results = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*]->(b) RETURN DISTINCT b"))
    expected_ids = {"a", "b", "c"}
    actual_ids = {row["b"]._properties["id"] for row in results}
    assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"


def test_correctness_multiple_paths_same_target(memgraph):
    results = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*]->(b) RETURN DISTINCT b"))
    ids = [row["b"]._properties["id"] for row in results]
    assert ids.count("c") == 1, f"'c' should appear exactly once, got: {ids}"


def test_correctness_multiple_sources(memgraph):
    """Each source vertex must independently discover its reachable set."""
    memgraph.drop_database()
    memgraph.execute("CREATE (a:N {id: 'a'})-[:TO]->(c:N {id: 'c'}), (b:N {id: 'b'})-[:TO]->(c);")
    results = list(
        memgraph.execute_and_fetch(
            "MATCH (src:N)-[*]->(dst) WHERE src.id IN ['a', 'b'] RETURN DISTINCT src.id AS s, dst.id AS d"
        )
    )
    pairs = {(row["s"], row["d"]) for row in results}
    assert ("a", "c") in pairs, f"Expected ('a', 'c') in results, got: {pairs}"
    assert ("b", "c") in pairs, f"Expected ('b', 'c') in results, got: {pairs}"


def test_correctness_count_distinct(memgraph):
    result = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*]->(b) RETURN count(DISTINCT b) AS cnt"))
    assert result[0]["cnt"] == 4, f"Expected 4, got {result[0]['cnt']}"


def test_correctness_collect_distinct(memgraph):
    result = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*]->(b) RETURN collect(DISTINCT b.id) AS ids"))
    assert sorted(result[0]["ids"]) == ["b", "c", "d", "e"], f"Got {result[0]['ids']}"


def test_correctness_bounded_depth(memgraph):
    """B1: bounded [*1..2] must match DFS results."""
    pruning = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*1..2]->(b) RETURN DISTINCT b.id AS id"))
    dfs = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[r*1..2]->(b) RETURN DISTINCT b.id AS id"))
    assert {r["id"] for r in pruning} == {r["id"] for r in dfs}


def test_correctness_zero_lower_bound(memgraph):
    """B6: [*0..2] must include the start vertex."""
    results = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*0..2]->(b) RETURN DISTINCT b.id AS id"))
    ids = {r["id"] for r in results}
    assert "a" in ids, f"Start vertex missing, got: {ids}"


def test_correctness_zero_zero_bound(memgraph):
    """B6: [*0..0] must return only the start vertex."""
    results = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*0..0]->(b) RETURN DISTINCT b.id AS id"))
    ids = {r["id"] for r in results}
    assert ids == {"a"}, f"Expected only start vertex, got: {ids}"


def test_negative_bound_throws(memgraph):
    """N2: negative parameterised bound must raise, not silently succeed."""
    with pytest.raises(Exception):
        list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*$lo..5]->(b) RETURN DISTINCT b", {"lo": -1}))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
