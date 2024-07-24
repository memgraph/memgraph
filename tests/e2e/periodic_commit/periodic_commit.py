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


def test_create_trigger_on_create_periodic_commit(memgraph):
    QUERY_TRIGGER_CREATE = f"""
        CREATE TRIGGER CreateOnCreateTrigger
        ON () CREATE
        BEFORE COMMIT
        EXECUTE
        UNWIND createdVertices AS createdVertex
        CREATE (n:TriggerCreated)
    """

    # Setup queries
    memgraph.execute(QUERY_TRIGGER_CREATE)
    memgraph.execute("UNWIND range(1, 10) as x CALL { CREATE (n:PeriodicCommitCreated) } IN TRANSACTIONS OF 1 ROWS;")

    actual = list(memgraph.execute_and_fetch("MATCH (n:PeriodicCommitCreated) RETURN count(n) as cnt"))[0]["cnt"]
    assert actual == 10

    actual = list(memgraph.execute_and_fetch("MATCH (n:TriggerCreated) RETURN count(n) as cnt"))[0]["cnt"]
    assert actual == 10


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
