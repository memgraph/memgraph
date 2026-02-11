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
Tests for Fine-Grained Access Control (FGAC) in parallel execution.

These tests verify that labels and edges are correctly filtered based on user permissions
when queries are executed using the parallel execution engine.
"""

import sys
from enum import Enum

import pytest
from common import pq, setup_thread_count_db
from conftest import MemgraphWrapper
from neo4j import GraphDatabase


class RestrictionScenario(Enum):
    NONE = "NONE"
    FIRST = "FIRST"
    LAST = "LAST"
    MIDDLE = "MIDDLE"
    EVERY_OTHER = "EVERY_OTHER"
    ALL = "ALL"


def setup_fgac_user(memgraph: MemgraphWrapper):
    """Setup a user with limited permissions for FGAC testing."""
    # admin connection is default for memgraph fixture
    memgraph.execute_query("CREATE USER parallel_user IDENTIFIED BY 'password'")
    # Grant basic read on one label, but not another
    memgraph.execute_query("GRANT ALL PRIVILEGES TO parallel_user")
    memgraph.execute_query("GRANT READ ON NODES CONTAINING LABELS :A TO parallel_user")
    # We will purposefully NOT grant READ on :B to test filtering


@pytest.fixture
def fgac_user_driver(memgraph_auth_instance):
    """Driver for the restricted FGAC user."""
    uri = memgraph_auth_instance.uri
    driver = GraphDatabase.driver(uri, auth=("parallel_user", "password"))
    yield driver
    driver.close()


def inject_restricted_labels(memgraph: MemgraphWrapper, count: int, scenario: RestrictionScenario):
    """
    Inject nodes with a restricted label (:B) into a set of nodes with allowed label (:A).
    The parallel_user only has access to :A.
    """
    memgraph.clear_database()
    # First create all as :A
    memgraph.execute_query(f"UNWIND range(1, {count}) AS i CREATE (:A{{p:i}})")

    if scenario == RestrictionScenario.NONE:
        indices = []
    elif scenario == RestrictionScenario.FIRST:
        indices = [1]
    elif scenario == RestrictionScenario.LAST:
        indices = [count]
    elif scenario == RestrictionScenario.MIDDLE:
        indices = [count // 2]
    elif scenario == RestrictionScenario.EVERY_OTHER:
        indices = list(range(1, count + 1, 2))
    elif scenario == RestrictionScenario.ALL:
        indices = list(range(1, count + 1))
    else:
        indices = []

    if indices:
        memgraph.execute_query(
            "UNWIND $indices AS i MATCH (n:A{p:i}) REMOVE n:A SET n:B",
            {"indices": indices},
        )

    return len(indices)


class TestFGACParallel:
    """Test FGAC filtering in parallel queries."""

    @pytest.fixture
    def restricted_memgraph(self, memgraph_auth):
        """Fixture providing a memgraph wrapper as the restricted user."""
        # memgraph fixture is already admin
        try:
            memgraph_auth.execute_query("DROP USER parallel_user")
        except:
            pass
        setup_fgac_user(memgraph_auth)
        return memgraph_auth.with_auth("parallel_user", "password")

    @pytest.mark.parametrize(
        "scenario",
        [
            RestrictionScenario.NONE,
            RestrictionScenario.FIRST,
            RestrictionScenario.LAST,
            RestrictionScenario.MIDDLE,
            RestrictionScenario.EVERY_OTHER,
            RestrictionScenario.ALL,
        ],
    )
    def test_parallel_label_filtering(self, memgraph_auth, restricted_memgraph, num_workers, scenario):
        """
        Verify that parallel execution correctly filters out nodes with restricted labels.
        The restricted user should only see nodes with label :A.
        """
        # Admin injects labels
        restricted_count = inject_restricted_labels(memgraph_auth, num_workers, scenario)
        expected_visible = num_workers - restricted_count

        query = pq("MATCH (n) RETURN count(n) AS count")

        # Run as restricted user
        result = restricted_memgraph.fetch_one(query)
        assert result["count"] == expected_visible

    def test_parallel_filtered_scan(self, memgraph_auth, restricted_memgraph, num_workers):
        """
        Verify that restricted nodes are not even scanned/processed in a way that
        they leak through aggregations.
        """
        # Admin setup
        memgraph_auth.clear_database()
        memgraph_auth.execute_query(f"UNWIND range(1, {num_workers}) AS i CREATE (:A{{val: 1}}), (:B{{val: 100}})")

        # Restricted user query
        query = pq("MATCH (n) RETURN sum(n.val) AS total_val")

        result = restricted_memgraph.fetch_one(query)
        # Should only sum values from :A nodes (1 * num_workers)
        assert result["total_val"] == num_workers


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
