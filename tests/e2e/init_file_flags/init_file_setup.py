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
from gqlalchemy import Memgraph


def test_given_init_file_when_memgraph_started_then_node_is_created():
    mg = Memgraph("localhost", 7687)

    result = next(mg.execute_and_fetch("MATCH (n) RETURN count(n) AS cnt"))["cnt"]

    assert result == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
