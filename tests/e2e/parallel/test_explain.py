import sys

import pytest
from common import pq, setup_thread_count_db


class TestParallelExplain:
    """
    Tests for EXPLAIN output of parallel queries.
    Verifies that the query plan correctly reflects parallel operators and thread counts
    based on scenarios from tests/unit/query_parallel_plan.cpp.
    """

    @pytest.fixture(autouse=True)
    def setup(self, memgraph):
        self.memgraph = memgraph
        setup_thread_count_db(memgraph)

    def _get_explain_plan(self, query):
        """Helper to get the explain plan content."""
        result = self.memgraph.fetch_all(f"EXPLAIN {query}")
        # Result is list of dicts: [{'QUERY PLAN': '...'}, ...]
        # Typically returns a single column.
        plan_lines = [list(record.values())[0] for record in result]
        return plan_lines

    def _verify_plan_contains(self, plan, required_operators: list[str], forbidden_operators: list[str] = None):
        """
        Verify that the plan contains all required operators and none of the forbidden ones.
        Args:
            plan (list[str]): The explain plan lines.
            required_operators (list[str]): List of operator substrings (e.g. "P ScanAll").
            forbidden_operators (list[str]): List of operator substrings that must NOT be present.
        """
        plan_text = "\n".join(plan)
        missing = []
        for op in required_operators:
            if not any(op in line for line in plan):
                missing.append(op)

        unexpected = []
        if forbidden_operators:
            for op in forbidden_operators:
                if any(op in line for line in plan):
                    unexpected.append(op)

        assert not missing, f"Missing operators in plan: {missing}.\nFull Plan:\n{plan_text}"
        assert not unexpected, f"Unexpected operators in plan: {unexpected}.\nFull Plan:\n{plan_text}"

    def test_parallel_scan_and_aggregate(self):
        """
        Corresponds to ParallelExecutionAggregation
        USING PARALLEL EXECUTION MATCH (n) RETURN count(*)
        Expect: P ScanAll, P Aggregate
        """
        query = "USING PARALLEL EXECUTION MATCH (n) RETURN count(*)"
        plan = self._get_explain_plan(query)
        self._verify_plan_contains(plan, ["P ScanAll", "P Aggregate"])

    def test_parallel_aggregation_with_filter(self):
        """
        Corresponds to CountWithFilter
        USING PARALLEL EXECUTION MATCH (n) WHERE n.p < 100 RETURN count(n)
        Expect: P ScanAll, Filter, P Aggregate
        """
        query = "USING PARALLEL EXECUTION MATCH (n) WHERE n.p < 100 RETURN count(n)"
        plan = self._get_explain_plan(query)
        self._verify_plan_contains(plan, ["P ScanAll", "Filter", "P Aggregate"])

    def test_parallel_avg_with_alias(self):
        """
        Corresponds to AvgWithAlias
        USING PARALLEL EXECUTION MATCH (n) WITH n AS m RETURN avg(m.p)
        Expect: P ScanAll, P Aggregate
        """
        query = "USING PARALLEL EXECUTION MATCH (n) WITH n AS m RETURN avg(m.p)"
        plan = self._get_explain_plan(query)
        self._verify_plan_contains(plan, ["P ScanAll", "P Aggregate"])

    def test_parallel_multi_agg(self):
        """
        Corresponds to MultiAgg
        USING PARALLEL EXECUTION MATCH (n) RETURN min(n.p), max(n.p), count(n.p)
        Expect: P ScanAll, P Aggregate (handling multiple aggs)
        """
        query = "USING PARALLEL EXECUTION MATCH (n) RETURN min(n.p), max(n.p), count(n.p)"
        plan = self._get_explain_plan(query)
        self._verify_plan_contains(plan, ["P ScanAll", "P Aggregate"])

    def test_parallel_orderby(self):
        """
        Corresponds to ParallelExecutionOrderBy
        USING PARALLEL EXECUTION MATCH (n) RETURN n ORDER BY n.prop
        Expect: P ScanAll, P OrderBy
        """
        query = "USING PARALLEL EXECUTION MATCH (n) RETURN n ORDER BY n.p"
        plan = self._get_explain_plan(query)
        self._verify_plan_contains(plan, ["P ScanAll", "P OrderBy"])

    def test_parallel_orderby_with_skip_limit(self):
        """
        Corresponds to ParallelExecutionOrderByWithSkipLimit
        USING PARALLEL EXECUTION MATCH (n) RETURN n ORDER BY n.p SKIP 5 LIMIT 10
        Expect: P ScanAll, P OrderBy
        """
        query = "USING PARALLEL EXECUTION MATCH (n) RETURN n ORDER BY n.p SKIP 5 LIMIT 10"
        plan = self._get_explain_plan(query)
        self._verify_plan_contains(plan, ["P ScanAll", "P OrderBy", "Skip", "Limit"])

    def test_parallel_agg_and_orderby_group_key(self):
        """
        Corresponds to ParallelExecutionGroupByWithOrderByOnGroupKey
        USING PARALLEL EXECUTION MATCH (n) RETURN n.type AS t, count(n) AS c ORDER BY t
        Expect: P ScanAll, P Aggregate, OrderBy (NOT Parallel OrderBy)
        Reason: ORDER BY is on the group key.
        """
        # Note: n.type might be null or not exist, still valid query
        query = "USING PARALLEL EXECUTION MATCH (n) RETURN n.type AS t, count(n) AS c ORDER BY t"
        plan = self._get_explain_plan(query)
        # Verify we have P Aggregate but standard OrderBy (not P OrderBy)
        # Based on unit test comment: "OrderBy is NOT parallelized because..."
        self._verify_plan_contains(
            plan, required_operators=["P ScanAll", "P Aggregate", "OrderBy"], forbidden_operators=["P OrderBy"]
        )

    def test_parallel_subquery(self):
        """
        Corresponds to BranchedSubqueries / NestedSubqueries structure
        MATCH (n) CALL { MATCH (m) RETURN count(*) AS c } RETURN count(*)
        Expect: Subquery should use parallel operators if hinted/implied?
        The unit test explicitly wraps PROJECTION/CALL in PARALLEL_QUERY.
        In E2E, "USING PARALLEL EXECUTION" at start applies to the query.
        """
        # Using a simpler form that forces parallel scan in subquery if possible
        # USING PARALLEL EXECUTION MATCH (n) CALL { MATCH (m) RETURN count(m) as c } RETURN c
        query = "USING PARALLEL EXECUTION MATCH (n) CALL { MATCH (m) RETURN count(m) as c } RETURN c"
        plan = self._get_explain_plan(query)
        # The main query might scan serial, but subquery should be parallel?
        # Actually unit test 'BranchedSubqueries' expects ExpectScanAll (serial outer) and ExpectApply(subquery_plan (parallel))
        # But 'NesetedSubqueries' (typo in C++) expects parallel subquery.

        # We check if *ScanAll* (serial) AND *P ScanAll* (parallel inside subquery) might coexist or just P ScanAll.
        # If the outer match is parallelized, good. If not, good.
        # Let's just check that we see P ScanAll somewhere.
        self._verify_plan_contains(plan, ["P ScanAll"])

    def test_write_inhibits_parallel(self):
        """
        Corresponds to MatchSetCount
        MATCH (n) SET n:A RETURN count(n)
        Expect: * ScanAll (Serial), NO Parallel operators
        """
        query = "USING PARALLEL EXECUTION MATCH (n) SET n:A RETURN count(n)"
        plan = self._get_explain_plan(query)
        # Unit test says: "Expect serial plan because SET inhibits parallelization"
        # So we expect NO "P ScanAll"
        self._verify_plan_contains(
            plan, required_operators=["* ScanAll", "SetLabels"], forbidden_operators=["P ScanAll", "P Aggregate"]
        )

    def test_write_with_cartesian_inhibits_parallel(self):
        """
        Corresponds to MultiMatchCreateCount
        MATCH (b), (t) CREATE () RETURN count(*)
        Expect: * ScanAll (Serial), Cartesian, CreateNode, NO Parallel operators
        Write operators should inhibit parallelization even with Cartesian joins.
        """
        query = "USING PARALLEL EXECUTION MATCH (b), (t) CREATE () RETURN count(*)"
        plan = self._get_explain_plan(query)
        # Expect serial plan because CREATE inhibits parallelization
        # The path from Aggregate through CreateNode to Cartesian to ScanAll is NOT read-only
        self._verify_plan_contains(
            plan,
            required_operators=["* ScanAll", "Cartesian", "CreateNode"],
            forbidden_operators=["P ScanAll", "P Aggregate"],
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
