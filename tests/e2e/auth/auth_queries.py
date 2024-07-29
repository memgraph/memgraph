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
from common import enterprise_only, memgraph, provide_user
from gqlalchemy import Memgraph


def test_user_creation(memgraph):
    memgraph.execute("CREATE USER mrma;")
    with pytest.raises(Exception):
        memgraph.execute("CREATE USER mrma;")
    memgraph.execute("CREATE USER IF NOT EXISTS mrma;")


def test_role_creation(enterprise_only, memgraph):
    memgraph.execute("CREATE ROLE mrma;")
    with pytest.raises(Exception):
        memgraph.execute("CREATE ROLE mrma;")
    memgraph.execute("CREATE ROLE IF NOT EXISTS mrma;")


def test_show_current_user_if_no_users(memgraph):
    results = list(memgraph.execute_and_fetch("SHOW CURRENT USER;"))
    assert len(results) == 1 and "user" in results[0] and results[0]["user"] == None


def test_show_current_user(provide_user):
    USERNAME = "anthony"
    memgraph_with_user = Memgraph(username=USERNAME, password="password")
    results = list(memgraph_with_user.execute_and_fetch("SHOW CURRENT USER;"))
    assert len(results) == 1 and "user" in results[0] and results[0]["user"] == USERNAME


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
