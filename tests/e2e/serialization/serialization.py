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

import sys

import pytest
from common import serialization


def test_serialization_on_creating_edges(serialization):
    serialization.setup("CREATE (:L1), (:L2)")

    (passes, fails) = serialization.run(
        [{"query": "MATCH (m:L1), (n:L2) CREATE (m)-[:$edge_label]->(n)", "args": {"edge_label": "alfa"}, "delay": 3}],
        [{"query": "MATCH (m:L1), (n:L2) CREATE (m)-[:$edge_label]->(n)", "args": {"edge_label": "bravo"}}],
    )
    assert passes == 2
    assert fails == 0


def test_edge_creation_shared_from_vertex(serialization):
    serialization.setup("CREATE (:V1), (:V2), (:V3)")

    (passes, fails) = serialization.run(
        [{"query": "MATCH (v1:V1), (v2:V2) CREATE (v1)-[:R1]->(v2)", "delay": 3}],
        [{"query": "MATCH (v1:V1), (v3:V3) CREATE (v1)-[:R2]->(v3)"}],
    )
    assert passes == 2
    assert fails == 0


def test_edge_creation_shared_to_vertex(serialization):
    serialization.setup("CREATE (:V1), (:V2), (:V3)")

    (passes, fails) = serialization.run(
        [{"query": "MATCH (v1:V1), (v2:V2) CREATE (v1)-[:R1]->(v2)", "delay": 3}],
        [{"query": "MATCH (v3:V3), (v2:V2) CREATE (v3)-[:R2]->(v2)"}],
    )
    assert passes == 2
    assert fails == 0


def test_edge_creation_no_conflict(serialization):
    serialization.setup("CREATE (:V1), (:V2), (:V3), (:V4)")

    (passes, fails) = serialization.run(
        [{"query": "MATCH (v1:V1), (v2:V2) CREATE (v1)-[:R1]->(v2)", "delay": 3}],
        [{"query": "MATCH (v3:V3), (v4:V4) CREATE (v3)-[:R2]->(v4)"}],
    )
    assert passes == 2
    assert fails == 0


def test_supernode_concurrent_edges(serialization):
    serialization.setup("CREATE (:V1), (:V2), (:V3), (:V4), (:V5), (:V6)")

    (passes, fails) = serialization.run(
        [{"query": "MATCH (v1:V1), (v2:V2) CREATE (v1)-[:R1]->(v2)", "delay": 3}],
        [
            {"query": "MATCH (v1:V1), (v3:V3) CREATE (v1)-[:R2]->(v3)"},
            {"query": "MATCH (v1:V1), (v4:V4) CREATE (v1)-[:R3]->(v4)"},
            {"query": "MATCH (v5:V5), (v1:V1) CREATE (v5)-[:R4]->(v1)"},
            {"query": "MATCH (v6:V6), (v1:V1) CREATE (v6)-[:R5]->(v1)"},
        ],
    )
    assert passes == 5
    assert fails == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
