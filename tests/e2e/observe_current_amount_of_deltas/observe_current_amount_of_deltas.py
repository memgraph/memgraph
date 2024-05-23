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
from common import connect, connect_with_autocommit, execute_and_fetch_all


def get_current_amount_of_deltas():
    storage_info_connection = connect_with_autocommit()

    storage_cursor = storage_info_connection.cursor()
    result = execute_and_fetch_all(storage_cursor, "SHOW STORAGE INFO")

    result = [x for x in result if x[0] == "unreleased_delta_objects"][0][1]

    return result


def free_memory():
    execute_and_fetch_all(connect_with_autocommit().cursor(), "FREE MEMORY")


def test_amount_of_deltas_drops_after_commit(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "CREATE ()")

    current_amount_of_deltas_before_commit = get_current_amount_of_deltas()
    assert current_amount_of_deltas_before_commit == 1

    connection.commit()

    current_amount_of_deltas_after_commit = get_current_amount_of_deltas()
    assert current_amount_of_deltas_after_commit == 0


def test_amount_of_deltas_drops_after_rollback(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "CREATE ()")

    current_amount_of_deltas_before_commit = get_current_amount_of_deltas()
    assert current_amount_of_deltas_before_commit == 1

    connection.rollback()

    current_amount_of_deltas_after_commit = get_current_amount_of_deltas()
    assert current_amount_of_deltas_after_commit == 0


def test_amount_of_deltas_with_2_transactions():
    connection1 = connect()
    connection2 = connect()
    cursor1 = connection1.cursor()
    cursor2 = connection2.cursor()
    execute_and_fetch_all(cursor1, "CREATE ()")
    execute_and_fetch_all(cursor2, "CREATE ()")

    current_amount_of_deltas_before_commit = get_current_amount_of_deltas()
    assert current_amount_of_deltas_before_commit == 2

    connection1.commit()
    connection2.commit()

    free_memory()

    current_amount_of_deltas_after_commit = get_current_amount_of_deltas()
    assert current_amount_of_deltas_after_commit == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
