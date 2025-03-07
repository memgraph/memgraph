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


def test_to_set_unduplicates_values(memgraph):
    result = next(memgraph.execute_and_fetch("RETURN toSet([1, 2, 3, 1, 2, 3, 4]) as l"))["l"]
    assert set(result) == {1, 2, 3, 4}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
