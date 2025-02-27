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



def test_vector_search_indexes_needed_labels(memgraph):
    memgraph.execute("CREATE (:Vector {point: [1, 1]});")
    memgraph.execute("CREATE (:NoVector {point: [1, 1]});")

    memgraph.execute("CREATE VECTOR INDEX t99 ON :Vector(point) WITH CONFIG {'dimension': 2, 'capacity': 1000, 'metric': 'haversine', 'resize_coefficient': 2};")

    results = list(memgraph.execute_and_fetch("CALL vector_search.show_index_info() YIELD * RETURN *;"))
    
    assert len(results) == 1
    assert results[0]["size"] == 1
    
    memgraph.execute("CREATE (:NoVector {point: [1, 1]});")

    results = list(memgraph.execute_and_fetch("CALL vector_search.show_index_info() YIELD * RETURN *;"))
    
    assert len(results) == 1
    assert results[0]["size"] == 1
    
    memgraph.execute("CREATE (:Vector {point: [1, 1]});")

    results = list(memgraph.execute_and_fetch("CALL vector_search.show_index_info() YIELD * RETURN *;"))
    
    assert len(results) == 1
    assert results[0]["size"] == 2


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
