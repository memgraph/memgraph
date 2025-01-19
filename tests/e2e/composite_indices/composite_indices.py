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

import pytest
from common import memgraph
from gqlalchemy import GQLAlchemyError

QUERY_PLAN = "QUERY PLAN"


def test_basic_composite_label_property_index(memgraph):
    memgraph.execute("CREATE INDEX ON :Node(prop1, prop2);")
    memgraph.execute("CREATE (n:Node {prop1: 1, prop2: 2})")

    expected_explain = [
        f" * Produce {{n}}",
        f" * ScanAllByLabelPropertyCompositeValue (n :Node {{prop1, prop2}})",
        f" * Once",
    ]

    actual_explain = list(
        memgraph.execute_and_fetch("EXPLAIN MATCH (n:Node) WHERE n.prop1 = 1 AND n.prop2 = 2 RETURN n")
    )
    actual_explain = [x[QUERY_PLAN] for x in actual_explain]

    assert expected_explain == actual_explain


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
