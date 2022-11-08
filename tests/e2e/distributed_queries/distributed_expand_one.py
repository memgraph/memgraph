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

from common import connection, execute_and_fetch_all, has_n_result_row, wait_for_shard_manager_to_initialize


def test_sequenced_expand_one(connection):
    wait_for_shard_manager_to_initialize()
    cursor = connection.cursor()

    for i in range(1, 4):
        assert has_n_result_row(cursor, f"CREATE (:label {{property:{i}}})", 0), f"Failed creating node"
    assert has_n_result_row(cursor, "MATCH (n {property:1}), (m {property:2}) CREATE (n)-[:TO]->(m)", 0)
    assert has_n_result_row(cursor, "MATCH (n {property:2}), (m {property:3}) CREATE (n)-[:TO]->(m)", 0)

    results = execute_and_fetch_all(cursor, "MATCH (n)-[:TO]->(m)-[:TO]->(l) RETURN n,m,l")
    assert len(results) == 1
    n, m, l = results[0]
    assert n.properties["property"] == 1
    assert m.properties["property"] == 2
    assert l.properties["property"] == 3


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
