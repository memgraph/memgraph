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
from common import memgraph, provide_user
from gqlalchemy import Memgraph


def test_empty_show_active_users_info(memgraph):
    results = list(memgraph.execute_and_fetch("SHOW ACTIVE USERS INFO"))
    assert len(results) == 1
    assert len(results[0]["username"]) == 0
    assert len(results[0]["session uuid"]) > 0
    assert len(results[0]["login timestamp"]) > 0


def test_active_show_users_info_with_2_users(provide_user):
    USERNAME = "anthony"
    memgraph_with_user = Memgraph(username=USERNAME, password="password")
    results = list(memgraph_with_user.execute_and_fetch("SHOW ACTIVE USERS INFO;"))
    found = False
    for r in results:
        if r["username"] == USERNAME:
            found = True

    assert found


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
