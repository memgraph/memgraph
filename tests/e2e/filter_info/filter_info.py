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


def test_label_index_hint(memgraph):
    memgraph.execute("CREATE (n:Label1:Label2 {prop: 1});")
    memgraph.execute("CREATE INDEX ON :Label1;")

    # TODO: Fix this test since it should only filter on :Label2 and prop
    expected_explain = [
        " * Produce {n}",
        " * Filter (n :Label1:Label2), {n.prop}",
        " * ScanAllByLabel (n :Label1)",
        " * Once",
    ]

    actual_explain = [
        row["QUERY PLAN"]
        for row in memgraph.execute_and_fetch("EXPLAIN MATCH (n:Label1:Label2) WHERE n.prop = 1 return n;")
    ]

    assert expected_explain == actual_explain


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
