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


def test_user_creation(memgraph):
    memgraph.execute("CREATE USER mrma;")
    with pytest.raises(Exception):
        memgraph.execute("CREATE USER mrma;")
    memgraph.execute("CREATE USER IF NOT EXISTS mrma;")


def test_role_creation(memgraph):
    memgraph.execute("CREATE ROLE mrma;")
    with pytest.raises(Exception):
        memgraph.execute("CREATE ROLE mrma;")
    memgraph.execute("CREATE ROLE IF NOT EXISTS mrma;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
