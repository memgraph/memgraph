# Copyright 2025 Memgraph Ltd.
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


def test_show_query_callable_mappings_returns_correct_columns(memgraph):
    results = list(memgraph.execute_and_fetch("SHOW QUERY CALLABLE MAPPINGS"))
    assert len(results) > 0

    first = results[0]
    assert "alias_name" in first
    assert "source_name" in first
    assert "type" in first


def test_show_query_callable_mappings_contains_expected_entries(memgraph):
    results = list(memgraph.execute_and_fetch("SHOW QUERY CALLABLE MAPPINGS"))
    mappings = {r["alias_name"]: r["source_name"] for r in results}

    assert "apoc.custom.procedure" in mappings
    assert mappings["apoc.custom.procedure"] == "mgps.components"

    assert "apoc.custom.function" in mappings
    assert mappings["apoc.custom.function"] == "math.log"


def test_show_query_callable_mappings_type_field(memgraph):
    results = list(memgraph.execute_and_fetch("SHOW QUERY CALLABLE MAPPINGS"))
    for r in results:
        assert r["type"] in ("procedure", "function", "unknown")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
