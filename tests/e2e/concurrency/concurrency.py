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
from common import execute_and_fetch_all, first_connection, second_connection


def test_concurrency_if_no_delta_on_same_property_update(first_connection, second_connection):
    m1c = first_connection.cursor()
    m2c = second_connection.cursor()

    execute_and_fetch_all(m1c, "CREATE (:Node {prop: 1})")
    first_connection.commit()

    test_has_error = False
    try:
        m1c.execute("MATCH (n) SET n.prop = 1")
        m2c.execute("MATCH (n) SET n.prop = 1")
        first_connection.commit()
        second_connection.commit()
    except Exception as e:
        test_has_error = True

    assert test_has_error is False


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
