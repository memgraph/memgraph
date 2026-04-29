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
import urllib.request

import pytest
from common import connect, execute_and_fetch_all


def scrape_json_metrics():
    with urllib.request.urlopen("http://localhost:9091/metrics") as response:
        return json.loads(response.read())


def test_json_storage_fields_are_default_db_only(connect):
    cursor = connect.cursor()

    execute_and_fetch_all(cursor, "CREATE DATABASE db2")

    execute_and_fetch_all(cursor, "USE DATABASE memgraph")
    execute_and_fetch_all(cursor, "CREATE (), (), ()")

    execute_and_fetch_all(cursor, "USE DATABASE db2")
    execute_and_fetch_all(cursor, "CREATE (), ()")

    metrics = scrape_json_metrics()
    # Storage fields reflect the default DB only, not an aggregate across all databases
    assert metrics["General"]["vertex_count"] == 3


def test_json_transaction_counters_are_aggregate_across_databases(connect):
    cursor = connect.cursor()

    execute_and_fetch_all(cursor, "CREATE DATABASE db2")

    execute_and_fetch_all(cursor, "USE DATABASE memgraph")
    before = scrape_json_metrics()["Transaction"]["CommitedTransactions"]

    execute_and_fetch_all(cursor, "CREATE ()")
    execute_and_fetch_all(cursor, "USE DATABASE db2")
    execute_and_fetch_all(cursor, "CREATE ()")

    after = scrape_json_metrics()["Transaction"]["CommitedTransactions"]
    # Both commits from both databases are counted
    assert after - before >= 2


def test_json_query_type_counters_are_aggregate_across_databases(connect):
    cursor = connect.cursor()

    execute_and_fetch_all(cursor, "CREATE DATABASE db2")

    execute_and_fetch_all(cursor, "USE DATABASE memgraph")
    before = scrape_json_metrics()["QueryType"]["WriteQuery"]

    execute_and_fetch_all(cursor, "CREATE ()")
    execute_and_fetch_all(cursor, "USE DATABASE db2")
    execute_and_fetch_all(cursor, "CREATE ()")

    after = scrape_json_metrics()["QueryType"]["WriteQuery"]
    # Write queries from both databases are counted
    assert after - before >= 2


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
