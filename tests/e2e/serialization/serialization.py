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

    serialization.run(
        [{"query": "MATCH (m:L1), (n:L2) CREATE (m)-[:$edge_label]->(n)", "args": {"edge_label": "alfa"}, "delay": 3}],
        [{"query": "MATCH (m:L1), (n:L2) CREATE (m)-[:$edge_label]->(n)", "args": {"edge_label": "bravo"}}],
    )


def test_serialization_on_updating_props(serialization):
    serialization.setup("CREATE (:L1)")

    serialization.run(
        [{"query": "MATCH (m:L1) SET m.prop = $prop", "args": {"prop": "first"}, "delay": 3}],
        [{"query": "MATCH (m:L1) SET m.prop = $prop", "args": {"prop": "second"}}],
    )


def test_serialization_chain_on_updating_props(serialization):
    serialization.setup("CREATE (:A), (:B), (:C)")

    serialization.run(
        [{"query": "MATCH (c:C) SET c.prop = $prop", "args": {"prop": "C1"}, "delay": 3}],
        [
            {
                "query": "MATCH (b:B), (c:C) SET b.prop = $prop_b SET c.prop = $prop_c",
                "args": {"prop_b": "B1", "prop_c": "C2"},
            },
            {
                "query": "MATCH (a:A), (b:B) SET a.prop = $prop_a SET b.prop = $prop_b",
                "args": {"prop_a": "A1", "prop_b": "B2"},
            },
        ],
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
