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

QUERY_PLAN = "QUERY PLAN"


def test_valid_plan_generation(memgraph):
    expected_explain = [
        f" * Produce {{0}}",
        f" * Apply",
        f" |\\ ",
        f" | * Produce {{i_2}}",
        f" | * EdgeUniquenessFilter {{anon11, anon10 : anon13}}",
        f" | * Expand (anon12)<-[anon13]-(hyc_1)",
        f" | * EdgeUniquenessFilter {{anon10 : anon11}}",
        f" | * Expand (i)-[anon11]-(anon12)",
        f" | * Expand (d_4)-[anon10]->(i)",
        f" | * Produce {{d_4, hyc_1}}",
        f" | * Once",
        f" * Apply",
        f" |\\ ",
        f" | * Produce {{i_1}}",
        f" | * EdgeUniquenessFilter {{anon6, anon5 : anon8}}",
        f" | * Expand (anon7)<-[anon8]-(hyc_1)",
        f" | * EdgeUniquenessFilter {{anon5 : anon6}}",
        f" | * Expand (i)-[anon6]-(anon7)",
        f" | * Expand (d_1)-[anon5]->(i)",
        f" | * ScanAll (d_1)",
        f" | * Once",
        f" * Expand (d_5)-[l_7]->(h_2)",
        f" * ScanAll (d_5)",
        f" * Produce {{d_1, d_2, d_4, h_2, hyc_1, l_4, l_5, l_6}}",
        f" * Expand (d_2)-[l_6]->(h_2)",
        f" * ScanAll (d_2)",
        f" * Produce {{d_1, d_4, hyc_1, l_4, l_5}}",
        f" * Expand (d_4)-[l_5]->(hyc_1)",
        f" * ScanAll (d_4)",
        f" * Produce {{d_1, hyc_1, l_4}}",
        f" * Expand (d_1)-[l_4]->(hyc_1)",
        f" * ScanAll (d_1)",
        f" * Once",
    ]

    results = list(
        memgraph.execute_and_fetch(
            "EXPLAIN MATCH (d_1)-[l_4]->(hyc_1) WITH * MATCH (d_4)-[l_5]->(hyc_1) WITH * MATCH (d_2)-[l_6]->(h_2) WITH * MATCH (d_5)-[l_7]->(h_2) CALL { MATCH (d_1)-[]->(i)-[]-()<-[]-(hyc_1) RETURN 1 as i_1 } CALL { WITH d_4, hyc_1 MATCH (d_4)-[]->(i)-[]-()<-[]-(hyc_1) RETURN 1 as i_2 } RETURN 1;"
        )
    )
    actual_explain = [x[QUERY_PLAN] for x in results]

    assert expected_explain == actual_explain


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
