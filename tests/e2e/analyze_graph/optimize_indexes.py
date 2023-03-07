# Copyright 2022 Memgraph Ltd.
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

import common
import pytest


def test_analyze_graph_better_index_applied_after_analysis(connection):
    cursor = connection.cursor()

    # id1 is the more scattered index
    common.execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id1);")

    # id2 is a more dense index -> this indes should not be applied
    common.execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id2);")

    expected_explain = [
        (f" * Produce {{n}}",),
        (f" * Filter",),
        (f" * ScanAllByLabelPropertyValue (n :Label {{id2}})",),
        (f" * Once",),
    ]

    results = common.execute_and_fetch_all(cursor, "EXPLAIN MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;")

    assert results == expected_explain

    common.execute_and_fetch_all(cursor, "ANALYZE GRAPH;")

    expected_explain_after_analysis = [
        (f" * Produce {{n}}",),
        (f" * Filter",),
        (f" * ScanAllByLabelPropertyValue (n :Label {{id1}})",),
        (f" * Once",),
    ]
    results = common.execute_and_fetch_all(cursor, "EXPLAIN MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;")

    assert results == expected_explain_after_analysis

    # results should not differ after switching the indexes in the filter
    results = common.execute_and_fetch_all(cursor, f"EXPLAIN MATCH (n:Label) WHERE n.id1 = 3 AND n.id2 = 3 RETURN n;")

    assert results == expected_explain_after_analysis


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
