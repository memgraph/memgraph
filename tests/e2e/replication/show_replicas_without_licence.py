# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

# MAIN keeps replicating every database without a valid licence, so SHOW REPLICAS reports every database.
# MAIN is licensed through settings only, so a mismatched organization name makes it unlicensed.

import os
import sys
from unittest.mock import patch

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "show_replicas_without_licence"
LICENSE_KEY = os.environ.get("MEMGRAPH_ENTERPRISE_LICENSE", "")
ORGANIZATION_NAME = os.environ.get("MEMGRAPH_ORGANIZATION_NAME", "")

pytestmark = pytest.mark.skipif(
    not (LICENSE_KEY and ORGANIZATION_NAME),
    reason="needs an enterprise license (MEMGRAPH_ENTERPRISE_LICENSE + MEMGRAPH_ORGANIZATION_NAME)",
)


@pytest.fixture(autouse=True)
def cleanup_after_test():
    interactive_mg_runner.kill_all(keep_directories=False)
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def test_show_replicas_lists_tenants_without_licence(request):
    description = {
        "replica": {
            "args": ["--bolt-port=7688"],
            "log_file": f"{get_logs_path(file, request.node.name)}/replica.log",
            "data_directory": f"{get_data_path(file, request.node.name)}/replica",
        },
        "main": {
            "args": ["--bolt-port=7687"],
            "log_file": f"{get_logs_path(file, request.node.name)}/main.log",
            "data_directory": f"{get_data_path(file, request.node.name)}/main",
        },
    }
    interactive_mg_runner.start(description, "replica")
    with patch.dict(os.environ):
        del os.environ["MEMGRAPH_ENTERPRISE_LICENSE"]
        del os.environ["MEMGRAPH_ORGANIZATION_NAME"]
        interactive_mg_runner.start(description, "main")
    main_cursor = connect(host="localhost", port=7687).cursor()
    for query in [
        f"SET DATABASE SETTING 'organization.name' TO '{ORGANIZATION_NAME}';",
        f"SET DATABASE SETTING 'enterprise.license' TO '{LICENSE_KEY}';",
        "CREATE DATABASE tenant;",
    ]:
        execute_and_fetch_all(main_cursor, query)
    execute_and_fetch_all(
        connect(host="localhost", port=7688).cursor(), "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;"
    )
    execute_and_fetch_all(main_cursor, "REGISTER REPLICA replica SYNC TO '127.0.0.1:10001';")

    execute_and_fetch_all(main_cursor, f"SET DATABASE SETTING 'organization.name' TO '{ORGANIZATION_NAME}-invalid';")
    assert not dict(execute_and_fetch_all(main_cursor, "SHOW LICENSE INFO;"))["is_valid"]

    data_info = execute_and_fetch_all(main_cursor, "SHOW REPLICAS;")[0][4]
    assert set(data_info) == {"memgraph", "tenant"}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
