# Copyright 2023 Memgraph Ltd.
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


def get_license_info_by_key(results, key):
    return [x["value"] for x in results if x["license info"] == key][0]


def expected_memory_limit_policy(license_type: str, memory_limit: str) -> str:
    # Mirrors src/query/interpreter.cpp SHOW LICENSE INFO handler.
    if memory_limit == "UNLIMITED":
        return "Memory usage is not limited."
    if license_type == "ai_platform":
        return "Graph and query memory are limited. Vector index memory is not limited."
    return "All memory usage is limited."


def test_empty_show_active_users_info(memgraph):
    results = list(memgraph.execute_and_fetch("SHOW LICENSE INFO"))
    assert len(results) == 8

    expected_keys = {
        "organization_name",
        "license_key",
        "is_valid",
        "license_type",
        "valid_until",
        "memory_limit",
        "status",
        "memory_limit_policy",
    }
    assert {row["license info"] for row in results} == expected_keys

    assert get_license_info_by_key(results, "organization_name") == "Memgraph"
    assert get_license_info_by_key(results, "is_valid") is True
    assert get_license_info_by_key(results, "status") == "You are running a valid Memgraph Enterprise License."

    license_type = get_license_info_by_key(results, "license_type")
    memory_limit = get_license_info_by_key(results, "memory_limit")
    assert license_type in {"enterprise", "oem", "ai_platform", "oem_community"}

    policy = get_license_info_by_key(results, "memory_limit_policy")
    assert policy == expected_memory_limit_policy(license_type, memory_limit)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
