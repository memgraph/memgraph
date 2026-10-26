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

# MAIN restarted without a valid licence still replicates every database once the licence is valid again.
# MAIN is licensed through settings only, so a mismatched organization name makes it unlicensed; the replica keeps
# the licence from the environment.

import os
import sys
from functools import partial
from unittest.mock import patch

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

file = "unlicensed_restart"
LICENSE_KEY = os.environ.get("MEMGRAPH_ENTERPRISE_LICENSE", "")
ORGANIZATION_NAME = os.environ.get("MEMGRAPH_ORGANIZATION_NAME", "")

pytestmark = pytest.mark.skipif(
    not (LICENSE_KEY and ORGANIZATION_NAME),
    reason="needs an enterprise license (MEMGRAPH_ENTERPRISE_LICENSE + MEMGRAPH_ORGANIZATION_NAME)",
)


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def instances(test_name):
    return {
        "replica": {
            "args": ["--bolt-port=7688", "--log-level=TRACE", "--data-recovery-on-startup"],
            "log_file": f"{get_logs_path(file, test_name)}/replica.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica",
        },
        "main": {
            "args": [
                "--bolt-port=7687",
                "--log-level=TRACE",
                "--data-recovery-on-startup",
                "--replication-restore-state-on-startup",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/main.log",
            "data_directory": f"{get_data_path(file, test_name)}/main",
        },
    }


def start_unlicensed(description, name):
    with patch.dict(os.environ):
        del os.environ["MEMGRAPH_ENTERPRISE_LICENSE"]
        del os.environ["MEMGRAPH_ORGANIZATION_NAME"]
        interactive_mg_runner.start(description, name)


def cursor(port):
    return connect(host="localhost", port=port).cursor()


def set_organization_name(main_cursor, organization_name):
    execute_and_fetch_all(main_cursor, f"SET DATABASE SETTING 'organization.name' TO '{organization_name}';")


def tenant_nodes(port):
    db_cursor = cursor(port)
    execute_and_fetch_all(db_cursor, "USE DATABASE tenant;")
    return execute_and_fetch_all(db_cursor, "MATCH (n) RETURN count(n);")[0][0]


def test_tenant_replicated_after_unlicensed_restart(request):
    description = instances(request.node.name)
    interactive_mg_runner.start(description, "replica")
    start_unlicensed(description, "main")
    main_cursor = cursor(7687)
    set_organization_name(main_cursor, ORGANIZATION_NAME)
    execute_and_fetch_all(main_cursor, f"SET DATABASE SETTING 'enterprise.license' TO '{LICENSE_KEY}';")
    execute_and_fetch_all(cursor(7688), "SET REPLICATION ROLE TO REPLICA WITH PORT 10001;")
    for query in [
        "REGISTER REPLICA replica SYNC TO '127.0.0.1:10001';",
        "CREATE DATABASE tenant;",
        "USE DATABASE tenant;",
        "UNWIND range(1, 10) AS i CREATE ();",
    ]:
        execute_and_fetch_all(main_cursor, query)
    mg_sleep_and_assert(10, partial(tenant_nodes, 7688))

    set_organization_name(main_cursor, ORGANIZATION_NAME.swapcase())
    interactive_mg_runner.stop(description, "main")
    start_unlicensed(description, "main")
    main_cursor = cursor(7687)
    set_organization_name(main_cursor, ORGANIZATION_NAME)

    execute_and_fetch_all(main_cursor, "USE DATABASE tenant;")
    execute_and_fetch_all(main_cursor, "CREATE ();")
    mg_sleep_and_assert(11, partial(tenant_nodes, 7688))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
