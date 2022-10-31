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

import typing
import mgclient
import sys
import pytest
import time
from common import *


def test_optional_match(connection):
    wait_for_shard_manager_to_initialize()
    cursor = connection.cursor()

    assert has_n_result_row(cursor, "CREATE (n :label {property:0})", 0)

    results = execute_and_fetch_all(
        cursor, "MATCH (n:label) OPTIONAL MATCH (n:label)-[:TO]->(parent:label) RETURN parent"
    )
    assert len(results) == 1

    assert has_n_result_row(cursor, "CREATE (n :label {property:2})", 0)
    assert has_n_result_row(cursor, "MATCH (n), (m) CREATE (n)-[:TO]->(m)", 0)
    assert has_n_result_row(cursor, "MATCH (n:label) OPTIONAL MATCH (n)-[r:TO]->(m:label) RETURN r", 4)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
