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

import os
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

    # Cross-check against SHOW DATABASE instead of hardcoding "memgraph", so the assertion still
    # holds if the default database name is ever reconfigured.
    current_database = list(memgraph.execute_and_fetch("SHOW DATABASE"))
    assert len(current_database) == 1
    expected_db_name = current_database[0]["Current"]

    assert results[0]["database"] == expected_db_name
    assert results[0]["database marked for deletion"] is False


@pytest.mark.skipif(
    not (os.environ.get("MEMGRAPH_ENTERPRISE_LICENSE") and os.environ.get("MEMGRAPH_ORGANIZATION_NAME")),
    reason="Needs an enterprise license (MEMGRAPH_ENTERPRISE_LICENSE + MEMGRAPH_ORGANIZATION_NAME) to exercise "
    "CREATE DATABASE / USE DATABASE",
)
def test_show_active_users_info_database_follows_use_database(memgraph):
    TENANT_DB = "tenant_show_active_users_info"

    memgraph.execute(f"CREATE DATABASE {TENANT_DB};")
    try:
        memgraph.execute(f"USE DATABASE {TENANT_DB};")

        results = list(memgraph.execute_and_fetch("SHOW ACTIVE USERS INFO"))
        assert len(results) == 1
        assert results[0]["database"] == TENANT_DB
        assert results[0]["database marked for deletion"] is False
    finally:
        memgraph.execute("USE DATABASE memgraph;")
        memgraph.execute(f"DROP DATABASE {TENANT_DB};")


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
