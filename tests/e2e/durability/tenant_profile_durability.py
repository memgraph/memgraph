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


def connect():
    conn = mgclient.connect(host="127.0.0.1", port=BOLT_PORT)
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


def _user_connect(user=None, password=None):
    if user is None:
        return connect()
    conn = mgclient.connect(host="127.0.0.1", port=BOLT_PORT, username=user, password=password)
    conn.autocommit = True
    return conn


def _user_execute(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def _ensure_admin():
    try:
        conn = _user_connect()
    except Exception:
        conn = _user_connect("admin", "admin-pw")  # this test created it above
    cursor = conn.cursor()
    try:
        _user_execute(cursor, "GRANT ALL PRIVILEGES TO admin")
    except Exception:
        pass
    conn.close()


def _resource_usage(cursor, user):
    return {row[0]: list(row[1:]) for row in _user_execute(cursor, f"SHOW RESOURCE USAGE FOR {user}")}


def _second_session_error(user, password):
    conn = None
    try:
        conn = _user_connect(user, password)
        _user_execute(conn.cursor(), "RETURN 1")
        return None
    except Exception as exc:  # noqa: BLE001 - the refusal is the assertion, not its message
        return exc
    finally:
        if conn is not None:
            conn.close()


def test_user_profile_survives_restart_mixed_case():
    """A profile assigned with a mixed-case spelling survives a restart and throttles the account afterwards."""
    interactive_mg_runner.start(INSTANCE, "main")

    conn = connect()
    cursor = conn.cursor()
    execute(cursor, "CREATE USER admin IDENTIFIED BY 'admin-pw'")
    conn.close()

    _ensure_admin()
    admin = _user_connect("admin", "admin-pw")
    admin_cursor = admin.cursor()
    _user_execute(admin_cursor, "CREATE PROFILE durable_one LIMIT sessions 1")
    _user_execute(admin_cursor, "CREATE USER TestUser IDENTIFIED BY 'pw'")
    _user_execute(admin_cursor, "GRANT ALL PRIVILEGES TO TestUser")
    _user_execute(admin_cursor, "SET PROFILE FOR TestUser TO durable_one")
    assert _user_execute(admin_cursor, "SHOW PROFILE FOR testuser") == [("durable_one",)]
    admin.close()

    interactive_mg_runner.kill(INSTANCE, "main")
    interactive_mg_runner.start(INSTANCE, "main")

    admin = _user_connect("admin", "admin-pw")
    admin_cursor = admin.cursor()
    for spelling in ("TestUser", "testuser"):
        assert _user_execute(admin_cursor, f"SHOW PROFILE FOR {spelling}") == [("durable_one",)]

    session = _user_connect("TestUser", "pw")
    try:
        for spelling in ("TestUser", "testuser"):
            usage = _resource_usage(admin_cursor, spelling)
            assert usage["sessions"] == [1, 1], f"{spelling} sees {usage['sessions']} after restart"
        assert _second_session_error("testuser", "pw") is not None, "the restored limit did not refuse a second session"
    finally:
        session.close()
        admin.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
