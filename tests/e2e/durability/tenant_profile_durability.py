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

"""
E2E durability test: tenant profiles survive restart.
"""

import os
import sys

import interactive_mg_runner
import mgclient
import pytest

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

BOLT_PORT = 7687

INSTANCE = {
    "main": {
        "args": [
            "--bolt-port",
            str(BOLT_PORT),
            "--log-level=TRACE",
            "--also-log-to-stderr=true",
        ],
        "log_file": "tenant_profile_durability.log",
        "data_directory": "tenant_profile_durability",
        "setup_queries": [],
    }
}


def connect(user=None, password=None):
    if user is None:
        conn = mgclient.connect(host="127.0.0.1", port=BOLT_PORT)
    else:
        conn = mgclient.connect(host="127.0.0.1", port=BOLT_PORT, username=user, password=password)
    conn.autocommit = True
    return conn


def execute(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


@pytest.fixture(autouse=True)
def cleanup():
    interactive_mg_runner.kill_all(keep_directories=False)
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def test_tenant_profile_survives_restart():
    interactive_mg_runner.start(INSTANCE, "main")

    conn = connect()
    cursor = conn.cursor()

    execute(cursor, "CREATE TENANT PROFILE durable_prof LIMIT memory_limit 300 MB")
    execute(cursor, "SET TENANT PROFILE ON DATABASE memgraph TO durable_prof")

    rows = execute(cursor, "SHOW TENANT PROFILES")
    assert len(rows) == 1
    assert rows[0][0] == "durable_prof"

    rows = execute(cursor, "SHOW STORAGE INFO ON DATABASE memgraph")
    info = {r[0]: r[1] for r in rows}
    assert "300" in info["tenant_memory_limit"]

    conn.close()

    # Restart
    interactive_mg_runner.kill(INSTANCE, "main")
    interactive_mg_runner.start(INSTANCE, "main")

    conn = connect()
    cursor = conn.cursor()

    # Profile should still exist
    rows = execute(cursor, "SHOW TENANT PROFILES")
    assert len(rows) == 1
    assert rows[0][0] == "durable_prof"

    # Limit should still be applied
    rows = execute(cursor, "SHOW STORAGE INFO ON DATABASE memgraph")
    info = {r[0]: r[1] for r in rows}
    assert "300" in info["tenant_memory_limit"]

    # Cleanup
    execute(cursor, "REMOVE TENANT PROFILE FROM DATABASE memgraph")
    execute(cursor, "DROP TENANT PROFILE durable_prof")
    conn.close()


def test_user_profile_survives_restart_mixed_case():
    interactive_mg_runner.start(INSTANCE, "main")

    conn = connect()
    cursor = conn.cursor()
    execute(cursor, "CREATE USER admin IDENTIFIED BY 'admin-pw'")
    conn.close()

    admin = connect("admin", "admin-pw")
    admin_cursor = admin.cursor()
    execute(admin_cursor, "CREATE PROFILE durable_one LIMIT sessions 1, transactions_memory 100MB")
    execute(admin_cursor, "CREATE USER TestUser IDENTIFIED BY 'pw'")
    execute(admin_cursor, "GRANT ALL PRIVILEGES TO TestUser")
    execute(admin_cursor, "SET PROFILE FOR TestUser TO durable_one")
    assert execute(admin_cursor, "SHOW PROFILE FOR testuser") == [("durable_one",)]
    admin.close()

    interactive_mg_runner.kill(INSTANCE, "main")
    interactive_mg_runner.start(INSTANCE, "main")

    admin = connect("admin", "admin-pw")
    admin_cursor = admin.cursor()
    for spelling in ("TestUser", "testuser"):
        assert execute(admin_cursor, f"SHOW PROFILE FOR {spelling}") == [("durable_one",)]

    session = connect("TestUser", "pw")
    try:
        for spelling in ("TestUser", "testuser"):
            usage = {row[0]: list(row[1:]) for row in execute(admin_cursor, f"SHOW RESOURCE USAGE FOR {spelling}")}
            assert usage["sessions"] == [1, 1], f"{spelling} sees {usage['sessions']} after restart"

        refused = None
        second = None
        try:
            second = connect("testuser", "pw")
            execute(second.cursor(), "RETURN 1")
        except Exception as exc:  # noqa: BLE001 - the refusal is the assertion
            refused = exc
        finally:
            if second is not None:
                second.close()
        assert refused is not None, "the restored limit did not refuse a second session"
        assert "sessions" in str(refused).lower(), f"unexpected refusal: {refused}"

        memory_refused = None
        try:
            execute(session.cursor(), "UNWIND range(1, 20000000) AS i WITH collect(i) AS c RETURN size(c)")
        except Exception as exc:  # noqa: BLE001 - the refusal is the assertion
            memory_refused = exc
        assert memory_refused is not None, "the restored profile did not enforce transactions_memory after restart"
        assert "user memory limit exceeded!" in str(memory_refused).lower(), f"unexpected error: {memory_refused}"
    finally:
        session.close()
        admin.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
