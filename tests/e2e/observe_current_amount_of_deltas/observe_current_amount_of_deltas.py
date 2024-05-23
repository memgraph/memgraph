# Copyright 2024 Memgraph Ltd.
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
from common import connect, execute_and_fetch_all


def get_current_amount_of_deltas():
    storage_cursor = connect().cursor()
    result = execute_and_fetch_all(storage_cursor, "SHOW STORAGE INFO")

    return result


def test_amount_of_deltas_drops_after_commit(connection):
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "BEGIN")
    execute_and_fetch_all(cursor, "CREATE ()")

    current_amount_of_deltas_before_commit = get_current_amount_of_deltas()
    assert current_amount_of_deltas_before_commit == 1

    execute_and_fetch_all(cursor, "COMMIT")

    current_amount_of_deltas_after_commit = get_current_amount_of_deltas()
    assert current_amount_of_deltas_after_commit == 0


def test_amount_of_deltas_drops_after_rollback(connection):
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "BEGIN")
    execute_and_fetch_all(cursor, "CREATE ()")

    current_amount_of_deltas_before_commit = get_current_amount_of_deltas()
    assert current_amount_of_deltas_before_commit == 1

    execute_and_fetch_all(cursor, "ROLLBACK")

    current_amount_of_deltas_after_commit = get_current_amount_of_deltas()
    assert current_amount_of_deltas_after_commit == 0


def test_amount_of_deltas_with_2_transactions(connection):
    cursor1 = connect().cursor()
    cursor2 = connect().cursor()
    execute_and_fetch_all(cursor1, "BEGIN")
    execute_and_fetch_all(cursor2, "BEGIN")
    execute_and_fetch_all(cursor1, "CREATE ()")
    execute_and_fetch_all(cursor2, "CREATE ()")

    current_amount_of_deltas_before_commit = get_current_amount_of_deltas()
    assert current_amount_of_deltas_before_commit == 2

    execute_and_fetch_all(cursor1, "COMMIT")
    execute_and_fetch_all(cursor2, "COMMIT")
    execute_and_fetch_all(cursor1, "FREE MEMORY")

    current_amount_of_deltas_after_commit = get_current_amount_of_deltas()
    assert current_amount_of_deltas_after_commit == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
