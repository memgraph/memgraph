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

import common
import pytest


def user_cursor():
    conn = common.connect(username="user", password="test")
    return conn.cursor()


def test_all_properties_visible_when_flag_disabled():
    result = common.execute_and_fetch_all(user_cursor(), "MATCH (n:Employee) RETURN n.name AS name, n.ssn AS ssn;")
    assert len(result) == 1
    assert result[0][0] == "Alice"
    assert result[0][1] == "123-45-6789"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
