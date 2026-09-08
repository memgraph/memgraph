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


def fetch_pruning(memgraph, query):
    """Run `query`, first asserting it really is planned as a pruning BFS."""
    ops = operator_names(get_plan(memgraph, query))
    assert "PruningBFSExpand" in ops, f"Expected a pruning BFS plan, got: {ops}"
    return list(memgraph.execute_and_fetch(query))


def fetch_depth_first(memgraph, query):
    """Run `query`, first asserting the rewrite left it depth-first. Binding an
    edge variable is not enough to block the rewrite; the query must read it."""
    ops = operator_names(get_plan(memgraph, query))
    assert "ExpandVariable" in ops, f"Expected a depth-first plan, got: {ops}"
    assert "PruningBFSExpand" not in ops, f"Expected a depth-first plan, got: {ops}"
    return list(memgraph.execute_and_fetch(query))


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
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*..3]-(b) RETURN DISTINCT b")
    ops = operator_names(plan)
    assert "PruningBFSExpand" in ops, f"Expected PruningBFSExpand in plan, got: {plan}"


# === A lower bound above one is walked depth-first ===


def test_lower_bound_above_every_path_finds_nothing(memgraph):
    """No path out of 'a' runs to three edges, so the bound excludes them all."""
    results = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*3..3]->(x) RETURN DISTINCT x.id AS id"))
    assert results == [], f"Expected no rows, got {[r['id'] for r in results]}"


# === A bound the plan cannot read is left to the walk ===


def test_malformed_bound_stays_quiet_where_nothing_expands(memgraph):
    for bound in ["'x'", "null", "1.5"]:
        query = f"MATCH (a:Absent)-[*{bound}..2]->(x) RETURN DISTINCT x.id AS id"
        assert list(memgraph.execute_and_fetch(query)) == [], f"Expected no rows for a bound of {bound}"


def test_malformed_bound_is_reported_where_something_expands(memgraph):
    for bound in ["'x'", "null"]:
        query = f"MATCH (a:N {{id: 'a'}})-[*{bound}..2]->(x) RETURN DISTINCT x.id AS id"
        with pytest.raises(Exception):
            list(memgraph.execute_and_fetch(query))


# === Re-expanding: the walk survives being reset ===


def test_expansion_reset_once_per_outer_row(memgraph):
    """An Apply resets its subtree for every row it feeds, so the expansion is
    reset between sources rather than built afresh for each."""
    plan = get_plan(
        memgraph,
        "MATCH (a:N) CALL { WITH a MATCH (a)-[*1..2]->(x:N) RETURN DISTINCT x } RETURN a.id, x.id",
    )
    # The expansion sits on the Apply's branch, which operator_names does not
    # reach past, so the raw lines are what carry it.
    assert any("PruningBFSExpand" in line for line in plan), f"Expected a pruning plan, got: {plan}"

    results = list(
        memgraph.execute_and_fetch(
            "MATCH (a:N) CALL { WITH a MATCH (a)-[*1..2]->(x:N) RETURN DISTINCT x } "
            "RETURN a.id + '->' + x.id AS pair"
        )
    )
    found = {r["pair"] for r in results}
    expected = {"a->b", "a->c", "a->d", "a->e", "b->c", "d->c", "d->e"}
    assert found == expected, f"Got {sorted(found)}"


# === Undirected expansion: the source is reachable only over a real cycle ===
#
# Every other vertex is reached by a shortest path, which is simple and so uses
# each edge once. Only the source needs a closed walk, and that walk must not
# reuse an edge, which is what separates these cases.


def test_correctness_undirected_single_edge_excludes_source(memgraph):
    """Returning to 'a' would reuse the only edge, which DFS forbids."""
    memgraph.drop_database()
    memgraph.execute("CREATE (a:N {id: 'a'})-[:TO]->(b:N {id: 'b'});")
    results = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*..2]-(x) RETURN DISTINCT x.id AS id"))
    assert {r["id"] for r in results} == {"b"}, f"Expected {{'b'}}, got {[r['id'] for r in results]}"


def test_correctness_undirected_path_excludes_source(memgraph):
    memgraph.drop_database()
    memgraph.execute("CREATE (a:N {id: 'a'})-[:TO]->(b:N {id: 'b'})-[:TO]->(c:N {id: 'c'});")
    results = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*..3]-(x) RETURN DISTINCT x.id AS id"))
    assert {r["id"] for r in results} == {"b", "c"}, f"Got {[r['id'] for r in results]}"


def test_correctness_undirected_cycle_reaches_source(memgraph):
    """A triangle gives a closed walk over three distinct edges."""
    memgraph.drop_database()
    memgraph.execute("CREATE (a:N {id: 'a'})-[:TO]->(b:N {id: 'b'})-[:TO]->(c:N {id: 'c'}), (c)-[:TO]->(a);")
    results = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*..3]-(x) RETURN DISTINCT x.id AS id"))
    assert {r["id"] for r in results} == {"a", "b", "c"}, f"Got {[r['id'] for r in results]}"


def test_correctness_undirected_cycle_beyond_upper_bound(memgraph):
    """The same triangle, but three hops is further than the query allows."""
    memgraph.drop_database()
    memgraph.execute("CREATE (a:N {id: 'a'})-[:TO]->(b:N {id: 'b'})-[:TO]->(c:N {id: 'c'}), (c)-[:TO]->(a);")
    results = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*..2]-(x) RETURN DISTINCT x.id AS id"))
    assert {r["id"] for r in results} == {"b", "c"}, f"Got {[r['id'] for r in results]}"


def test_correctness_undirected_matches_depth_first(memgraph):
    memgraph.drop_database()
    memgraph.execute(
        "CREATE (a:N {id: 'a'})-[:TO]->(b:N {id: 'b'})-[:TO]->(c:N {id: 'c'}), "
        "(a)-[:TO]->(d:N {id: 'd'}), (d)-[:TO]->(c);"
    )
    pruning = fetch_pruning(memgraph, "MATCH (a:N {id: 'a'})-[*..3]-(x) RETURN DISTINCT x.id AS id")
    dfs = fetch_depth_first(memgraph, "MATCH p=(a:N {id: 'a'})-[*1..3]-(x) RETURN DISTINCT x.id AS id")
    assert {r["id"] for r in pruning} == {r["id"] for r in dfs}, f"pruning={pruning} dfs={dfs}"


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
    """A bounded expansion must agree with DFS."""
    pruning = fetch_pruning(memgraph, "MATCH (a:N {id: 'a'})-[*1..2]->(b) RETURN DISTINCT b.id AS id")
    dfs = fetch_depth_first(memgraph, "MATCH p=(a:N {id: 'a'})-[*1..2]->(b) RETURN DISTINCT b.id AS id")
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


# === Soundness: the rewrite must not fire when it cannot be proven safe ===


@pytest.fixture
def diamond_graph(memgraph):
    """'x' is reachable from 'a' in one hop and, via 'y', in two."""
    memgraph.drop_database()
    memgraph.execute("CREATE (a:N {id: 'a'})-[:TO]->(x:N {id: 'x'}), (a)-[:TO]->(y:N {id: 'y'})-[:TO]->(x);")
    return memgraph


def test_one_cached_plan_serves_every_bound(diamond_graph):
    """One plan serves every bound, so the same query text run again with a
    different bound must get the walk that bound calls for. The bounds are
    revisited out of order, where a walk held over would show up."""
    query = "MATCH (a:N {id: 'a'})-[*$lo..3]->(b) RETURN DISTINCT b.id AS id"
    expected = {1: {"x", "y"}, 2: {"x"}, 3: set()}

    for lo in [1, 2, 3, 3, 2, 1, 2, 1, 3]:
        found = {r["id"] for r in diamond_graph.execute_and_fetch(query, parameters={"lo": lo})}
        assert found == expected[lo], f"For a lower bound of {lo}, expected {expected[lo]}, got {found}"


def test_correctness_lower_bound_above_one(diamond_graph):
    """A vertex first reached below the lower bound must still be emitted at a
    qualifying depth. Pruning BFS marks it visited on discovery, so it cannot."""
    results = list(diamond_graph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*2..2]->(b) RETURN DISTINCT b.id AS id"))
    assert {r["id"] for r in results} == {"x"}, f"Expected {{'x'}} via a->y->x, got {[r['id'] for r in results]}"


def test_the_plan_names_the_walk_that_runs(memgraph):
    """The bound decides the walk twice over: once to name it in a plan, once to
    run it. Were the two to disagree, a plan would report a walk other than the
    one taken, so hold them together across the bounds either may see."""
    for bounds in ["*", "*1", "*..2", "*0..2", "*1..2", "*1..5", "*2..2", "*2..3", "*3..5"]:
        query = f"MATCH (a:N {{id: 'a'}})-[{bounds}]->(b:N) RETURN DISTINCT b.id AS id"

        planned = [op for op in operator_names(get_plan(memgraph, query)) if "Expand" in op]
        profiled = [
            row["OPERATOR"].strip().removeprefix("* ").split(" ")[0]
            for row in memgraph.execute_and_fetch(f"PROFILE {query}")
            if "Expand" in row["OPERATOR"]
        ]
        assert planned == profiled, f"For {bounds}, the plan says {planned} and the profile says {profiled}"


def test_pruning_bfs_with_lower_bound_of_zero(memgraph):
    """A walk of no edges reaches only its own source, which a pruning BFS emits
    as readily, so a bound below one permits pruning just as a bound of one does."""
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*0..2]->(b) RETURN DISTINCT b")
    assert "PruningBFSExpand" in operator_names(plan), f"Expected a pruning plan, got: {plan}"

    results = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*0..2]->(x) RETURN DISTINCT x.id AS id"))
    assert {r["id"] for r in results} == {"a", "b", "c", "d", "e"}, f"Got {sorted(r['id'] for r in results)}"


def test_no_rewrite_with_lower_bound_above_one(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*2..3]->(b) RETURN DISTINCT b")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_pruning_bfs_with_written_lower_bound_of_one(memgraph):
    """Stripping turns a written-out bound into a parameter, which the cursor
    resolves rather than the plan."""
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*1..3]->(b) RETURN DISTINCT b")
    ops = operator_names(plan)
    assert "PruningBFSExpand" in ops, f"Expected PruningBFSExpand in plan, got: {plan}"


def test_bounds_sharing_a_stripped_query_do_not_share_a_plan(diamond_graph):
    """[*1..2] and [*2..2] strip to the same text, so caching a plan settled from
    one bound would serve it to the other, which it is wrong for."""
    shallow = list(diamond_graph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*1..2]->(b) RETURN DISTINCT b.id AS id"))
    assert {r["id"] for r in shallow} == {"x", "y"}, f"Got {[r['id'] for r in shallow]}"
    deep = list(diamond_graph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*2..2]->(b) RETURN DISTINCT b.id AS id"))
    assert {r["id"] for r in deep} == {"x"}, f"Got {[r['id'] for r in deep]}"


def test_bounds_sharing_a_stripped_query_do_not_share_a_plan_in_reverse(diamond_graph):
    """The order the bounds are first seen in must not decide the outcome."""
    deep = list(diamond_graph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*2..2]->(b) RETURN DISTINCT b.id AS id"))
    assert {r["id"] for r in deep} == {"x"}, f"Got {[r['id'] for r in deep]}"
    shallow = list(diamond_graph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*1..2]->(b) RETURN DISTINCT b.id AS id"))
    assert {r["id"] for r in shallow} == {"x", "y"}, f"Got {[r['id'] for r in shallow]}"


def test_supplied_parameter_bounds_do_not_share_a_plan(diamond_graph):
    query = "MATCH (a:N {id: 'a'})-[*$lo..2]->(b) RETURN DISTINCT b.id AS id"
    shallow = list(diamond_graph.execute_and_fetch(query, {"lo": 1}))
    assert {r["id"] for r in shallow} == {"x", "y"}, f"Got {[r['id'] for r in shallow]}"
    deep = list(diamond_graph.execute_and_fetch(query, {"lo": 2}))
    assert {r["id"] for r in deep} == {"x"}, f"Got {[r['id'] for r in deep]}"


def test_no_rewrite_with_limit_below_distinct(memgraph):
    """LIMIT counts rows, and pruning BFS emits fewer of them than DFS."""
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*..3]->(b) WITH b LIMIT 5 RETURN DISTINCT b")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_with_skip_below_distinct(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N {id: 'a'})-[*..3]->(b) WITH b SKIP 2 RETURN DISTINCT b")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_when_merge_create_branch_reads_edges(memgraph):
    """ON CREATE SET reads the edge symbol from an operator the rewriter does not
    analyse, and the matching branch does not mention it."""
    plan = get_plan(memgraph, "MATCH (a:N)-[e*..3]->(b) MERGE (z:M) ON CREATE SET z.n = size(e) RETURN DISTINCT b")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_when_merge_match_branch_reads_edges(memgraph):
    plan = get_plan(memgraph, "MATCH (a:N)-[e*..3]->(b) MERGE (z:M) ON MATCH SET z.n = size(e) RETURN DISTINCT b")
    ops = operator_names(plan)
    assert "ExpandVariable" in ops, f"Expected ExpandVariable in plan, got: {plan}"
    assert "PruningBFSExpand" not in ops, f"PruningBFSExpand should not appear, got: {plan}"


def test_no_rewrite_when_later_lambda_reads_earlier_edges(memgraph):
    """Separate MATCH clauses are not linked by an EdgeUniquenessFilter, so the
    second expand's lambda is the only thing that mentions the first's edges."""
    query = "MATCH (a:N)-[e*..3]->(b) MATCH (b)-[*..3 (r, n | size(e) >= 0)]->(c) RETURN DISTINCT c"
    plan = get_plan(memgraph, query)
    plan_text = "\n".join(plan)
    assert "ExpandVariable (a)-[e]" in plan_text, f"Expand binding 'e' must not be rewritten, got: {plan}"


def test_correctness_later_lambda_reads_earlier_edges(diamond_graph):
    """size(e) is Null if the first expand never binds e, which silently filters
    the second expand down to nothing."""
    query = "MATCH (a:N)-[e*..3]->(b) MATCH (b)-[*..3 (r, n | size(e) >= 0)]->(c) RETURN DISTINCT c.id AS id"
    results = list(diamond_graph.execute_and_fetch(query))
    assert {r["id"] for r in results} == {"x"}, f"Expected {{'x'}}, got {[r['id'] for r in results]}"


def test_non_vertex_input_names_the_symbol(memgraph):
    """The DFS cursor reports which symbol held the non-vertex; so must this one."""
    with pytest.raises(Exception) as excinfo:
        list(memgraph.execute_and_fetch("UNWIND [1] AS w MATCH (w)-[*..3]->(b) RETURN DISTINCT b"))
    assert "'w'" in str(excinfo.value), f"Error should name the symbol, got: {excinfo.value}"


def test_correctness_directed_cycle_reaches_source(memgraph):
    """A directed closed walk uses distinct edges, so the source does qualify.
    This is why leaving the source unmarked is right for directed expansion."""
    memgraph.drop_database()
    memgraph.execute("CREATE (a:N {id: 'a'})-[:TO]->(b:N {id: 'b'})-[:TO]->(a);")
    results = list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*..3]->(x) RETURN DISTINCT x.id AS id"))
    assert {r["id"] for r in results} == {"a", "b"}, f"Got {[r['id'] for r in results]}"


def test_correctness_filter_lambda_matches_dfs(memgraph):
    """The lambda is a memoryless node/edge predicate, so pruning BFS explores
    exactly the subgraph of passing edges, where reachability matches DFS."""
    memgraph.drop_database()
    memgraph.execute(
        "CREATE (a:N {id: 'a'})-[:TO]->(b:N {id: 'b'})-[:TO]->(c:N {id: 'c'}), "
        "(a)-[:TO]->(d:N {id: 'd'})-[:TO]->(c);"
    )
    lam = "(e, n | n.id <> 'b')"
    pruning = fetch_pruning(memgraph, f"MATCH (a:N {{id: 'a'}})-[*..3 {lam}]->(x) RETURN DISTINCT x.id AS id")
    dfs = fetch_depth_first(memgraph, f"MATCH p=(a:N {{id: 'a'}})-[*1..3 {lam}]->(x) RETURN DISTINCT x.id AS id")
    assert {r["id"] for r in pruning} == {r["id"] for r in dfs} == {"c", "d"}, f"pruning={pruning} dfs={dfs}"


def test_negative_bound_throws(memgraph):
    """N2: negative parameterised bound must raise, not silently succeed."""
    with pytest.raises(Exception):
        list(memgraph.execute_and_fetch("MATCH (a:N {id: 'a'})-[*$lo..5]->(b) RETURN DISTINCT b", {"lo": -1}))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
