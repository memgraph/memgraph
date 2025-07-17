# Copyright 2023 Memgraph Ltd.
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
import time
from threading import Barrier, Thread

import pytest
from common import serialization


def test_serialization_on_creating_edges(serialization):
    serialization.setup("CREATE (:L1), (:L2)")

    serialization.run(
        [
            {"query": "MATCH (m:L1), (n:L2) CREATE (m)-[:$edge_label]->(n)", "args": {"edge_label": "alfa"}, "wait": 3},
            {"query": "MATCH (m:L1), (n:L2) CREATE (m)-[:$edge_label]->(n)", "args": {"edge_label": "bravo"}},
        ]
    )


def test_serialization_on_updating_props(serialization):
    serialization.setup("CREATE (:L1)")

    serialization.run(
        [
            {"query": "MATCH (m:L1) SET m.prop_value = $prop_value", "args": {"prop_value": "first"}, "wait": 3},
            {"query": "MATCH (m:L1) SET m.prop_value = $prop_value", "args": {"prop_value": "second"}},
        ]
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
