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

# TTL is an enterprise feature: it deletes nothing while the licence is invalid, and resumes once it is valid again.
# The instance is licensed through settings only, so a mismatched organization name makes it unlicensed.

import os
import sys
import time
from functools import partial
from unittest.mock import patch

import interactive_mg_runner
import pytest
from common import execute_and_fetch_all
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

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


def start_licensed_by_settings(test_name):
    description = {
        "main": {
            "args": ["--bolt-port=7687", "--log-level=TRACE"],
            "log_file": f"time_to_live/licence/{test_name}/main.log",
            "data_directory": f"time_to_live/licence/{test_name}/main",
        }
    }
    with patch.dict(os.environ):
        del os.environ["MEMGRAPH_ENTERPRISE_LICENSE"]
        del os.environ["MEMGRAPH_ORGANIZATION_NAME"]
        interactive_mg_runner.start(description, "main")
    cursor = interactive_mg_runner.MEMGRAPH_INSTANCES["main"].get_connection().cursor()
    set_organization_name(cursor, ORGANIZATION_NAME)
    execute_and_fetch_all(cursor, f"SET DATABASE SETTING 'enterprise.license' TO '{LICENSE_KEY}';")
    return cursor


def set_organization_name(cursor, organization_name):
    execute_and_fetch_all(cursor, f"SET DATABASE SETTING 'organization.name' TO '{organization_name}';")


def ttl_nodes(cursor):
    return execute_and_fetch_all(cursor, "MATCH (n:TTL) RETURN count(n);")[0][0]


def test_ttl_deletes_nothing_without_licence(request):
    cursor = start_licensed_by_settings(request.node.name)
    execute_and_fetch_all(cursor, 'ENABLE TTL EVERY "1s";')

    set_organization_name(cursor, ORGANIZATION_NAME.swapcase())
    execute_and_fetch_all(cursor, "UNWIND range(1, 10) AS i CREATE (:TTL {ttl: 0});")
    time.sleep(3)
    assert ttl_nodes(cursor) == 10

    set_organization_name(cursor, ORGANIZATION_NAME)
    mg_sleep_and_assert(0, partial(ttl_nodes, cursor))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
