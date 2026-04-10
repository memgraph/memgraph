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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
