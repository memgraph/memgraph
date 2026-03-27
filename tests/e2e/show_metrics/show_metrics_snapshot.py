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

import sys

import pytest
from common import memgraph


def get_metric_value(memgraph, metric_name, on_clause=None):
    query = "SHOW METRICS INFO"
    if on_clause:
        query += f" {on_clause}"
    results = list(memgraph.execute_and_fetch(query))
    for r in results:
        if r["name"] == metric_name:
            return r["value"]
    return None


def test_snapshot_creation_latency_histogram_has_observations(memgraph):
    memgraph.execute("CREATE SNAPSHOT")
    assert get_metric_value(memgraph, "SnapshotCreationLatency_us_50p") > 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
