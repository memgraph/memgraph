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
from gqlalchemy import Memgraph


def get_explain(memgraph, query):
    return [row["QUERY PLAN"] for row in memgraph.execute_and_fetch(f"EXPLAIN {query}")]


def get_index_info(memgraph):
    return list(memgraph.execute_and_fetch("SHOW INDEX INFO"))


def setup_data(memgraph: Memgraph):
    memgraph.execute("CREATE GLOBAL INDEX ON :(prop)")
    memgraph.execute("CREATE (:A {prop: 1})")
    memgraph.execute("CREATE (:A {prop: 2})")
    memgraph.execute("CREATE (:B {prop: 3})")
    memgraph.execute("CREATE (:B {prop: 4})")
    memgraph.execute("CREATE (:C {prop: 5})")
    memgraph.execute("CREATE (:C {other: 99})")


def teardown_data(memgraph: Memgraph):
    memgraph.execute("MATCH (n) DETACH DELETE n")
    memgraph.execute("DROP GLOBAL INDEX ON :(prop)")


def test_create_and_show_index(memgraph):
    memgraph.execute("CREATE GLOBAL INDEX ON :(prop)")
    try:
        info = get_index_info(memgraph)
        vp_indices = [row for row in info if row["index type"] == "vertex-property"]
        assert len(vp_indices) == 1
        assert vp_indices[0]["property"] == "prop"
    finally:
        memgraph.execute("DROP GLOBAL INDEX ON :(prop)")


def test_drop_index(memgraph):
    memgraph.execute("CREATE GLOBAL INDEX ON :(prop)")
    memgraph.execute("DROP GLOBAL INDEX ON :(prop)")
    info = get_index_info(memgraph)
    vp_indices = [row for row in info if row["index type"] == "vertex-property"]
    assert len(vp_indices) == 0


def test_scan_by_vertex_property_plan(memgraph):
    setup_data(memgraph)
    try:
        query = "MATCH (n) WHERE n.prop IS NOT NULL RETURN n.prop AS prop"
        plan = get_explain(memgraph, query)
        assert any("ScanAllByVertexProperty" in step for step in plan)

        results = {x["prop"] for x in memgraph.execute_and_fetch(query)}
        assert results == {1, 2, 3, 4, 5}
    finally:
        teardown_data(memgraph)


def test_scan_by_vertex_property_value_plan(memgraph):
    setup_data(memgraph)
    try:
        query = "MATCH (n) WHERE n.prop = 3 RETURN n.prop AS prop"
        plan = get_explain(memgraph, query)
        assert any("ScanAllByVertexPropertyValue" in step for step in plan)

        results = {x["prop"] for x in memgraph.execute_and_fetch(query)}
        assert results == {3}
    finally:
        teardown_data(memgraph)


def test_scan_by_vertex_property_range_plan(memgraph):
    setup_data(memgraph)
    try:
        query = "MATCH (n) WHERE n.prop > 1 AND n.prop < 5 RETURN n.prop AS prop"
        plan = get_explain(memgraph, query)
        assert any("ScanAllByVertexPropertyRange" in step for step in plan)

        results = {x["prop"] for x in memgraph.execute_and_fetch(query)}
        assert results == {2, 3, 4}
    finally:
        teardown_data(memgraph)


def test_results_span_multiple_labels(memgraph):
    setup_data(memgraph)
    try:
        query = "MATCH (n) WHERE n.prop > 0 RETURN labels(n)[0] AS label, n.prop AS prop ORDER BY prop"
        results = list(memgraph.execute_and_fetch(query))
        assert len(results) == 5
        assert [(r["label"], r["prop"]) for r in results] == [
            ("A", 1),
            ("A", 2),
            ("B", 3),
            ("B", 4),
            ("C", 5),
        ]
    finally:
        teardown_data(memgraph)


def test_create_index_after_data(memgraph):
    """Index created after data should still find existing vertices."""
    memgraph.execute("CREATE (:X {val: 10})")
    memgraph.execute("CREATE (:Y {val: 20})")
    memgraph.execute("CREATE GLOBAL INDEX ON :(val)")
    try:
        query = "MATCH (n) WHERE n.val = 10 RETURN n.val AS val"
        results = {x["val"] for x in memgraph.execute_and_fetch(query)}
        assert results == {10}
    finally:
        memgraph.execute("MATCH (n) DETACH DELETE n")
        memgraph.execute("DROP GLOBAL INDEX ON :(val)")


def test_duplicate_create_is_idempotent(memgraph):
    memgraph.execute("CREATE GLOBAL INDEX ON :(prop)")
    try:
        # Second create should succeed (no-op with notification)
        memgraph.execute("CREATE GLOBAL INDEX ON :(prop)")
        info = get_index_info(memgraph)
        vp_indices = [row for row in info if row["index type"] == "vertex-property"]
        assert len(vp_indices) == 1
    finally:
        memgraph.execute("DROP GLOBAL INDEX ON :(prop)")


def test_drop_nonexistent_is_idempotent(memgraph):
    # Drop of nonexistent index should succeed (no-op with notification)
    memgraph.execute("DROP GLOBAL INDEX ON :(nonexistent)")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
