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

# Multi-tenancy is an enterprise feature: without a valid licence, databases other than the default stay readable but
# refuse writes. The instance is licensed through settings only, so a mismatched organization name makes it unlicensed.

import os
import sys
from unittest.mock import patch

import interactive_mg_runner
import mgclient
import pytest

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


def execute(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def set_organization_name(cursor, organization_name):
    execute(cursor, f"SET DATABASE SETTING 'organization.name' TO '{organization_name}';")


def test_tenant_read_only_without_licence(request):
    description = {
        "main": {
            "args": ["--bolt-port=7687", "--log-level=TRACE"],
            "log_file": f"show_license_info/tenants_without_licence/{request.node.name}/main.log",
            "data_directory": f"show_license_info/tenants_without_licence/{request.node.name}/main",
        }
    }
    with patch.dict(os.environ):
        del os.environ["MEMGRAPH_ENTERPRISE_LICENSE"]
        del os.environ["MEMGRAPH_ORGANIZATION_NAME"]
        interactive_mg_runner.start(description, "main")
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()
    set_organization_name(cursor, ORGANIZATION_NAME)
    execute(cursor, f"SET DATABASE SETTING 'enterprise.license' TO '{LICENSE_KEY}';")
    execute(cursor, "CREATE DATABASE tenant;")
    execute(cursor, "USE DATABASE tenant;")
    execute(cursor, "UNWIND range(1, 10) AS i CREATE ();")
    execute(cursor, "USE DATABASE memgraph;")

    set_organization_name(cursor, ORGANIZATION_NAME.swapcase())

    assert "tenant" in [row[0] for row in execute(cursor, "SHOW DATABASES;")]
    execute(cursor, "USE DATABASE tenant;")
    assert execute(cursor, "MATCH (n) RETURN count(n);")[0][0] == 10
    for write in ["CREATE ();", "CREATE INDEX ON :Label;"]:
        with pytest.raises(mgclient.DatabaseError, match="read-only without a valid license"):
            execute(cursor, write)
    execute(cursor, "USE DATABASE memgraph;")
    execute(cursor, "CREATE ();")
    with pytest.raises(mgclient.DatabaseError):
        execute(cursor, "CREATE DATABASE another;")

    set_organization_name(cursor, ORGANIZATION_NAME)

    execute(cursor, "USE DATABASE tenant;")
    execute(cursor, "CREATE ();")
    assert execute(cursor, "MATCH (n) RETURN count(n);")[0][0] == 11


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
