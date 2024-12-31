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


def test_empty_show_active_users_info(memgraph):
    results = list(memgraph.execute_and_fetch("SHOW LICENSE INFO"))
    assert len(results) == 7
    assert get_license_info_by_key(results, "organization_name") == "Memgraph"
    assert get_license_info_by_key(results, "is_valid") is True
    assert get_license_info_by_key(results, "status") == "You are running a valid Memgraph Enterprise License."


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
