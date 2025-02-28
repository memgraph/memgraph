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
import mgclient

def test_vector_search_can_be_performed_on_ints_and_doubles(memgraph):
    memgraph.execute(
        "CREATE VECTOR INDEX t1 ON :Node1(point) WITH CONFIG {'dimension': 2, 'capacity': 1000, 'metric': 'l2sq','resize_coefficient': 10};"
    )
    memgraph.execute("CREATE (:Node1 {id: 1, point: [1, 1]})")
    memgraph.execute("CREATE (:Node1 {id: 2, point: [2.2, 2.2]})")

    results = list(
        memgraph.execute_and_fetch(
            """
            CALL vector_search.search('t1', 1, [1.1, 1.1]) YIELD node, similarity 
            RETURN node.point as closest_point
            """
        )
    )

    assert len(results) == 1
    assert results[0]["closest_point"] == [1, 1]

    results = list(
        memgraph.execute_and_fetch(
            """
            CALL vector_search.search('t1', 1, [2, 2]) YIELD node, similarity 
            RETURN node.point as closest_point
            """
        )
    )

    assert len(results) == 1
    assert results[0]["closest_point"] == [2.2, 2.2]

    with pytest.raises(mgclient.DatabaseError):
        results = list(
            memgraph.execute_and_fetch(
                """
                CALL vector_search.search('t1', 1, ['invalid', 'invalid']) YIELD node, similarity 
                RETURN node.point as closest_point
                """
            )
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
