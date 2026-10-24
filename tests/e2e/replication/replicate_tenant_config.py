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

import json
import sys
import time

import pytest
from common import execute_and_fetch_all


def _replica_sees_tenant(replica, tenant, timeout=20.0):
    """Poll SHOW DATABASES on the replica until the tenant replicated from MAIN is visible."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = execute_and_fetch_all(replica.cursor(), "SHOW DATABASES;")
        if any(tenant in str(row) for row in rows):
            return True
        time.sleep(0.25)
    return False


def _show_schema_info_in(replica, tenant):
    """Return (rows, None) on success or (None, error) when the query is refused."""
    cursor = replica.cursor()
    execute_and_fetch_all(cursor, f"USE DATABASE {tenant};")
    try:
        return execute_and_fetch_all(cursor, "SHOW SCHEMA INFO;"), None
    except Exception as e:  # noqa: BLE001
        return None, e


def _assert_schema_info_works(replica, tenant, context):
    rows, error = _show_schema_info_in(replica, tenant)
    assert error is None, (
        f"SHOW SCHEMA INFO must work on {context}: MAIN was started with --schema-info-enabled and the "
        f"replica was not, so the per-database enable_schema_info must cross the replication wire into the "
        f"replica tenant (gh5190); got: {error!r}"
    )
    schema = json.loads(rows[0][0])
    assert "nodes" in schema and "edges" in schema


def test_replicated_tenant_salient_config(connection):
    """A tenant created on MAIN replicates its complete per-database config to the replica."""
    main = connection(7687, "main")
    replica = connection(7688, "replica")

    execute_and_fetch_all(main.cursor(), "CREATE DATABASE tenant_initial;")
    assert _replica_sees_tenant(replica, "tenant_initial"), "the replica never saw the tenant MAIN created"

    _assert_schema_info_works(replica, "tenant_initial", "a tenant replicated from MAIN")


def test_replicated_tenant_salient_config_after_recreate(connection):
    """Dropping and recreating the tenant on MAIN replays the same config-carrying create."""
    main = connection(7687, "main")
    replica = connection(7688, "replica")

    execute_and_fetch_all(main.cursor(), "CREATE DATABASE tenant_recreated;")
    assert _replica_sees_tenant(replica, "tenant_recreated"), "the replica never saw the tenant MAIN created"

    execute_and_fetch_all(main.cursor(), "DROP DATABASE tenant_recreated;")
    time.sleep(1.0)
    execute_and_fetch_all(main.cursor(), "CREATE DATABASE tenant_recreated;")
    assert _replica_sees_tenant(replica, "tenant_recreated"), "the replica never saw the recreated tenant"

    _assert_schema_info_works(replica, "tenant_recreated", "a tenant dropped and recreated on MAIN")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
