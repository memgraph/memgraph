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

import pytest
from common import connect, execute_and_fetch_all


@pytest.mark.parametrize(
    "analyze_query",
    [
        "ANALYZE GRAPH",
        "ANALYZE GRAPH ON LABELS *",
        "ANALYZE GRAPH ON LABELS :Label",
        "ANALYZE GRAPH ON LABELS :Label, :NONEXISTING",
    ],
)
def test_analyze_full_graph(analyze_query, connect):
    """Tests analyzing full graph and choosing better index based on the smaller average group size.
    It also tests querying based on labels and that nothing bad will happend by providing non-existing label.
    """
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 100) | CREATE (n:Label {id1: i, id2: i % 5}));")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Label(id2);")
    analyze_graph_results = execute_and_fetch_all(cursor, analyze_query)
    if analyze_graph_results[0][1] == "id1":
        first_index = 0
    else:
        first_index = 1

    assert analyze_graph_results[first_index][0] == "Label"
    assert analyze_graph_results[first_index][1] == "id1"
    assert analyze_graph_results[first_index][2] == 100
    assert analyze_graph_results[first_index][3] == 1
    assert analyze_graph_results[first_index][4] == 0
    assert analyze_graph_results[1 - first_index][0] == "Label"
    assert analyze_graph_results[1 - first_index][1] == "id2"
    assert analyze_graph_results[1 - first_index][2] == 100
    assert analyze_graph_results[1 - first_index][3] == 20
    assert analyze_graph_results[1 - first_index][4] == 0

    expected_explain_after_analysis = [
        (f" * Produce {{n}}",),
        (f" * Filter",),
        (f" * ScanAllByLabelPropertyValue (n :Label {{id1}})",),
        (f" * Once",),
    ]
    assert (
        execute_and_fetch_all(cursor, "EXPLAIN MATCH (n:Label) WHERE n.id2 = 3 AND n.id1 = 3 RETURN n;")
        == expected_explain_after_analysis
    )
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id1);")
    execute_and_fetch_all(cursor, "DROP INDEX ON :Label(id2);")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
