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

import os
import shutil
import sys

import interactive_mg_runner
import mgclient
import pytest
from common import connect, corrupt_snapshots, execute_and_fetch_all, get_data_path

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))


@pytest.fixture
def test_name(request):
    return request.node.name


@pytest.fixture(autouse=True)
def cleanup_instances():
    interactive_mg_runner.kill_all()
    yield
    interactive_mg_runner.kill_all()


def tenant_dir(full_data_directory):
    """Return the on-disk directory of the single non-default tenant under databases/<uuid>."""
    databases_dir = os.path.join(full_data_directory, "databases")
    tenant_dirs = [
        os.path.join(databases_dir, d)
        for d in os.listdir(databases_dir)
        if d not in ("memgraph", ".durability")
        and not d.startswith(".")
        and os.path.isdir(os.path.join(databases_dir, d))
    ]
    assert len(tenant_dirs) == 1, f"Expected exactly one tenant directory, found {tenant_dirs}"
    return tenant_dirs[0]


def databases(cursor):
    """Return the set of database names reported by SHOW DATABASES."""
    return {row[0] for row in execute_and_fetch_all(cursor, "SHOW DATABASES")}


def test_drop_defunct_database(test_name):
    """An operator can abandon a defunct tenant via DROP DATABASE: the drop succeeds,
    the tenant disappears from SHOW DATABASES, its on-disk directory (including the
    corrupt files) is removed, and the healthy default database is unaffected."""
    data_directory = get_data_path("drop_defunct_database", test_name)
    full_data_directory = os.path.join(interactive_mg_runner.BUILD_DIR, "e2e", "data", data_directory)
    shutil.rmtree(full_data_directory, ignore_errors=True)

    instances = {
        "default": {
            # WAL disabled so the snapshot is the only durability: corrupting it makes
            # recovery fail at the "no usable snapshot" path, yielding a defunct tenant.
            "args": ["--log-level=TRACE", "--data-recovery-on-startup=true", "--storage-wal-enabled=false"],
            "log_file": "drop_defunct_database.log",
            "data_directory": data_directory,
        }
    }

    interactive_mg_runner.start(instances, "default")
    cursor = connect(host="localhost", port=7687).cursor()

    # Multitenancy (CREATE/DROP DATABASE) is Enterprise-only.
    try:
        execute_and_fetch_all(cursor, "CREATE DATABASE broken_db")
    except mgclient.DatabaseError as e:
        msg = str(e).lower()
        if "enterprise" in msg or "not supported" in msg:
            pytest.skip("CREATE DATABASE requires an Enterprise license")
        raise

    # Populate and snapshot the tenant so it has durability to corrupt.
    execute_and_fetch_all(cursor, "USE DATABASE broken_db")
    execute_and_fetch_all(cursor, "UNWIND range(1, 5000) AS i CREATE (:Node {id: i})")
    execute_and_fetch_all(cursor, "CREATE SNAPSHOT")
    interactive_mg_runner.kill_all()

    # Corrupt the tenant's snapshot (each tenant lives under databases/<uuid>/).
    broken_dir = tenant_dir(full_data_directory)
    corrupt_snapshots(broken_dir)
    assert os.path.isdir(broken_dir)

    # Restart with the recovery-failure flag so the tenant comes up defunct.
    instances["default"]["args"].append("--storage-allow-recovery-failure=true")
    interactive_mg_runner.start(instances, "default")

    cursor = connect(host="localhost", port=7687).cursor()

    # Confirm the tenant is defunct: a data query against it throws the defunct error.
    execute_and_fetch_all(cursor, "USE DATABASE broken_db")
    with pytest.raises(mgclient.DatabaseError) as exc_info:
        execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n)")
    assert "defunct" in str(exc_info.value).lower()

    # Switch back to the default database (we cannot drop the database in use).
    execute_and_fetch_all(cursor, "USE DATABASE memgraph")

    # DROP DATABASE on the defunct tenant must succeed.
    execute_and_fetch_all(cursor, "DROP DATABASE broken_db")

    # The tenant is gone from SHOW DATABASES; the default database remains.
    names = databases(cursor)
    assert "broken_db" not in names
    assert "memgraph" in names

    # The default database is unaffected and still serves queries.
    execute_and_fetch_all(cursor, "CREATE (:Healthy {ok: true})")
    assert execute_and_fetch_all(cursor, "MATCH (n:Healthy) RETURN count(n)")[0][0] == 1

    # The corrupt on-disk directory is removed (deferred deletion may lag briefly).
    for _ in range(100):
        if not os.path.exists(broken_dir):
            break
        import time

        time.sleep(0.1)
    assert not os.path.exists(broken_dir), f"Tenant directory {broken_dir} should be removed after DROP DATABASE"

    interactive_mg_runner.stop_all()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
