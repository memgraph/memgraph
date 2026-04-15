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


def test_json_vertex_count_is_aggregate_across_databases(connect):
    cursor = connect.cursor()

    execute_and_fetch_all(cursor, "CREATE DATABASE db2")

    execute_and_fetch_all(cursor, "USE DATABASE memgraph")
    execute_and_fetch_all(cursor, "CREATE (), (), ()")

    execute_and_fetch_all(cursor, "USE DATABASE db2")
    execute_and_fetch_all(cursor, "CREATE (), ()")

    metrics = scrape_json_metrics()
    assert metrics["General"]["vertex_count"] == 5


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
