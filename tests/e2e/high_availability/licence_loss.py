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

# Each HA instance has its own licence. Promoting an instance whose licence is invalid must not cost the others their
# users, roles, or databases.

import os
import sys
import time
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
file = "licence_loss"

LICENSE_KEY = os.environ.get("MEMGRAPH_ENTERPRISE_LICENSE", "")
ORGANIZATION_NAME = os.environ.get("MEMGRAPH_ORGANIZATION_NAME", "")
USER = "tester"

pytestmark = pytest.mark.skipif(
    not (LICENSE_KEY and ORGANIZATION_NAME),
    reason="needs an enterprise license (MEMGRAPH_ENTERPRISE_LICENSE + MEMGRAPH_ORGANIZATION_NAME)",
)


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def instances(test_name: str):
    description = {}
    for i in (1, 2, 3):
        description[f"instance_{i}"] = {
            "args": [f"--bolt-port={7686 + i}", "--log-level=TRACE", f"--management-port={10010 + i}"],
            "log_file": f"{get_logs_path(file, test_name)}/instance_{i}.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_{i}",
        }
        description[f"coordinator_{i}"] = {
            "args": [
                f"--bolt-port={7689 + i}",
                "--log-level=TRACE",
                f"--coordinator-id={i}",
                f"--coordinator-port={10110 + i}",
                f"--management-port={10120 + i}",
                "--coordinator-hostname=localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_{i}.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_{i}",
        }
    return description


def cursor(port, authenticated=True):
    credentials = (USER, USER) if authenticated else ("", "")
    return connect(host="localhost", port=port, username=credentials[0], password=credentials[1]).cursor()


def node_count(port):
    def func():
        db_cursor = cursor(port)
        execute_and_fetch_all(db_cursor, "USE DATABASE tenant;")
        return execute_and_fetch_all(db_cursor, "MATCH (n) RETURN count(n);")[0][0]

    return func


def test_failover_to_unlicensed_replica_keeps_other_instances_state(test_name):
    description = instances(test_name)

    # instance_2 is licensed by settings only, so its licence can be broken while the others keep theirs.
    with patch.dict(os.environ):
        del os.environ["MEMGRAPH_ENTERPRISE_LICENSE"]
        del os.environ["MEMGRAPH_ORGANIZATION_NAME"]
        interactive_mg_runner.start(description, "instance_2")
    instance_2 = cursor(7688, authenticated=False)
    execute_and_fetch_all(instance_2, f"SET DATABASE SETTING 'organization.name' TO '{ORGANIZATION_NAME}';")
    execute_and_fetch_all(instance_2, f"SET DATABASE SETTING 'enterprise.license' TO '{LICENSE_KEY}';")
    for name in ("instance_1", "instance_3", "coordinator_1", "coordinator_2", "coordinator_3"):
        interactive_mg_runner.start(description, name)

    coordinator = cursor(7692, authenticated=False)
    for i in (1, 2, 3):
        execute_and_fetch_all(
            coordinator,
            f"ADD COORDINATOR {i} WITH CONFIG {{'bolt_server': 'localhost:{7689 + i}', "
            f"'coordinator_server': 'localhost:{10110 + i}', 'management_server': 'localhost:{10120 + i}'}};",
        )
    for i in (1, 2, 3):
        execute_and_fetch_all(
            coordinator,
            f"REGISTER INSTANCE instance_{i} WITH CONFIG {{'bolt_server': 'localhost:{7686 + i}', "
            f"'management_server': 'localhost:{10010 + i}', 'replication_server': 'localhost:{10000 + i}'}};",
        )
    execute_and_fetch_all(coordinator, "SET INSTANCE instance_1 TO MAIN;")

    main = cursor(7687, authenticated=False)
    for query in [
        "CREATE DATABASE tenant;",
        "USE DATABASE tenant;",
        "UNWIND range(1, 10) AS i CREATE (:Node);",
        "CREATE ROLE analyst;",
        f"CREATE USER {USER} IDENTIFIED BY '{USER}';",
        f"GRANT ALL PRIVILEGES TO {USER};",
        f"GRANT DATABASE * TO {USER};",
        f"GRANT READ ON NODES CONTAINING LABELS * TO {USER};",
    ]:
        execute_and_fetch_all(main, query)
    for port in (7688, 7689):
        mg_sleep_and_assert(10, node_count(port))

    execute_and_fetch_all(
        cursor(7688), f"SET DATABASE SETTING 'organization.name' TO '{ORGANIZATION_NAME.swapcase()}';"
    )
    execute_and_fetch_all(coordinator, "DEMOTE INSTANCE instance_1;")
    execute_and_fetch_all(coordinator, "SET INSTANCE instance_2 TO MAIN;")

    time.sleep(5)

    for port in (7687, 7689):
        assert [row[0] for row in execute_and_fetch_all(cursor(port), "SHOW USERS;")] == [USER]
        assert [row[0] for row in execute_and_fetch_all(cursor(port), "SHOW ROLES;")] == ["analyst"]
        assert node_count(port)() == 10


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
