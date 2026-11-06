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

# What a replicated cluster keeps, and what it enforces, when MAIN goes from licensed to unlicensed and back.
# MAIN is licensed through settings only, so a mismatched organization name makes it unlicensed; replicas keep the
# licence from the environment.

import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

import interactive_mg_runner
import mgclient
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path
from mg_utils import mg_sleep_and_assert

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

BOLT_PORTS = {"main": 7687, "replica_1": 7688}
REPLICATION_PORTS = {"replica_1": 10001}
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


def instances(test_name: str, main_args=()):
    return {
        "replica_1": {
            "args": [f"--bolt-port={BOLT_PORTS['replica_1']}", "--log-level=TRACE", "--data-recovery-on-startup"],
            "log_file": f"{get_logs_path(file, test_name)}/replica1.log",
            "data_directory": f"{get_data_path(file, test_name)}/replica1",
        },
        "main": {
            "args": [
                f"--bolt-port={BOLT_PORTS['main']}",
                "--log-level=TRACE",
                "--data-recovery-on-startup",
                "--replication-restore-state-on-startup",
                *main_args,
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


def cursor(name, authenticated=True):
    credentials = (USER, USER) if authenticated else ("", "")
    return connect(host="localhost", port=BOLT_PORTS[name], username=credentials[0], password=credentials[1]).cursor()


def set_organization_name(main_cursor, organization_name):
    execute_and_fetch_all(main_cursor, f"SET DATABASE SETTING 'organization.name' TO '{organization_name}';")


def node_count(db_cursor, db_name):
    def func():
        execute_and_fetch_all(db_cursor, f"USE DATABASE {db_name};")
        return execute_and_fetch_all(db_cursor, "MATCH (n) RETURN count(n);")[0][0]

    return func


def setup_licensed_cluster(description):
    """Start the replica licensed and MAIN licensed by settings; replicate a tenant with data and an authorized user."""
    interactive_mg_runner.start(description, "replica_1")
    start_unlicensed(description, "main")
    main_cursor = cursor("main", authenticated=False)
    set_organization_name(main_cursor, ORGANIZATION_NAME)
    execute_and_fetch_all(main_cursor, f"SET DATABASE SETTING 'enterprise.license' TO '{LICENSE_KEY}';")

    execute_and_fetch_all(
        cursor("replica_1", authenticated=False),
        f"SET REPLICATION ROLE TO REPLICA WITH PORT {REPLICATION_PORTS['replica_1']};",
    )
    for query in [
        f"REGISTER REPLICA replica_1 SYNC TO '127.0.0.1:{REPLICATION_PORTS['replica_1']}';",
        "CREATE DATABASE tenant;",
        "USE DATABASE tenant;",
        "UNWIND range(1, 10) AS i CREATE (:Node);",
        "CREATE ROLE analyst;",
        f"CREATE USER {USER} IDENTIFIED BY '{USER}';",
        f"GRANT ALL PRIVILEGES TO {USER};",
        f"GRANT DATABASE * TO {USER};",
        f"GRANT READ ON NODES CONTAINING LABELS * TO {USER};",
    ]:
        execute_and_fetch_all(main_cursor, query)
    mg_sleep_and_assert(10, node_count(cursor("replica_1"), "tenant"))


def assert_replica_keeps_state():
    replica_cursor = cursor("replica_1")
    assert [row[0] for row in execute_and_fetch_all(replica_cursor, "SHOW USERS;")] == [USER]
    assert [row[0] for row in execute_and_fetch_all(replica_cursor, "SHOW ROLES;")] == ["analyst"]
    assert node_count(replica_cursor, "tenant")() == 10


def test_replica_connecting_to_unlicensed_main_keeps_state(test_name):
    description = instances(test_name)
    setup_licensed_cluster(description)

    interactive_mg_runner.stop(description, "replica_1")
    set_organization_name(cursor("main"), ORGANIZATION_NAME.swapcase())
    interactive_mg_runner.start(description, "replica_1")
    time.sleep(5)

    assert_replica_keeps_state()


def test_replica_requires_credentials_after_main_loses_licence(test_name):
    description = instances(test_name)
    setup_licensed_cluster(description)

    set_organization_name(cursor("main"), ORGANIZATION_NAME.swapcase())
    time.sleep(5)

    with pytest.raises(mgclient.Error, match="Authentication failure"):
        cursor("replica_1", authenticated=False)


def test_relicensed_main_replica_in_step_without_write(test_name):
    description = instances(test_name)
    setup_licensed_cluster(description)

    set_organization_name(cursor("main"), ORGANIZATION_NAME.swapcase())
    time.sleep(5)
    set_organization_name(cursor("main"), ORGANIZATION_NAME)
    time.sleep(5)

    assert_replica_keeps_state()
    replicas = execute_and_fetch_all(cursor("main"), "SHOW REPLICAS;")
    assert replicas[0][4]["tenant"]["behind"] == 0


def test_fine_grained_rules_not_enforced_without_licence(test_name):
    description = instances(test_name)
    setup_licensed_cluster(description)
    main_cursor = cursor("main")
    for query in [
        "USE DATABASE memgraph;",
        "UNWIND range(1, 3) AS i CREATE (:Node);",
        "CREATE USER restricted IDENTIFIED BY 'restricted';",
        "GRANT ALL PRIVILEGES TO restricted;",
    ]:
        execute_and_fetch_all(main_cursor, query)
    count_default_database = "MATCH (n) RETURN count(n);"

    def visible_to_restricted():
        restricted = connect(host="localhost", port=BOLT_PORTS["main"], username="restricted", password="restricted")
        return execute_and_fetch_all(restricted.cursor(), count_default_database)[0][0]

    assert visible_to_restricted() == 0

    set_organization_name(main_cursor, ORGANIZATION_NAME.swapcase())

    assert visible_to_restricted() == 3


def test_user_profile_not_enforced_without_licence(test_name):
    description = instances(test_name)
    setup_licensed_cluster(description)
    main_cursor = cursor("main")
    for query in [
        "CREATE PROFILE single_session LIMIT sessions 1;",
        "CREATE USER limited IDENTIFIED BY 'limited';",
        "SET PROFILE FOR limited TO single_session;",
    ]:
        execute_and_fetch_all(main_cursor, query)
    set_organization_name(main_cursor, ORGANIZATION_NAME.swapcase())

    first = connect(host="localhost", port=BOLT_PORTS["main"], username="limited", password="limited")
    second = connect(host="localhost", port=BOLT_PORTS["main"], username="limited", password="limited")
    assert execute_and_fetch_all(second.cursor(), "RETURN 1;") == [(1,)]
    second.close()
    first.close()


def test_password_not_masked_in_log_without_licence(test_name):
    description = instances(test_name)
    setup_licensed_cluster(description)
    main_cursor = cursor("main")
    set_organization_name(main_cursor, ORGANIZATION_NAME.swapcase())

    execute_and_fetch_all(main_cursor, "CREATE USER secretive IDENTIFIED BY 'do_not_log_this';")

    log_dir = Path(interactive_mg_runner.BUILD_DIR, "e2e", "logs", get_logs_path(file, test_name))
    assert any("do_not_log_this" in log.read_text() for log in log_dir.glob("main*.log"))


def test_audit_log_stops_without_licence(test_name):
    description = instances(test_name, main_args=("--audit-enabled",))
    setup_licensed_cluster(description)
    main_cursor = cursor("main")
    audit_dir = Path(interactive_mg_runner.BUILD_DIR, "e2e", "data", get_data_path(file, test_name), "main", "audit")

    def audited(text):
        return lambda: any(text in log.read_text() for log in audit_dir.glob("*") if log.is_file())

    execute_and_fetch_all(main_cursor, "RETURN 'audited_licensed';")
    mg_sleep_and_assert(True, audited("audited_licensed"))

    set_organization_name(main_cursor, ORGANIZATION_NAME.swapcase())
    execute_and_fetch_all(main_cursor, "RETURN 'not_audited_unlicensed';")
    set_organization_name(main_cursor, ORGANIZATION_NAME)
    execute_and_fetch_all(main_cursor, "RETURN 'audited_relicensed';")
    mg_sleep_and_assert(True, audited("audited_relicensed"))

    assert not audited("not_audited_unlicensed")()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
