# Copyright 2022 Memgraph Ltd.
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
import threading
from typing import List

import mgclient
import pytest
from common import connect, execute_and_fetch_all

# Utility functions
# -------------------------


def get_non_show_transaction_id(results):
    """Returns transaction id of the first transaction that is not SHOW TRANSACTIONS;"""
    print(f"Results: {results}")
    for res in results:
        if res[1] != "SHOW TRANSACTIONS":
            return res[0]


def show_transactions_test(cursor, expected_num_results: int):
    results = execute_and_fetch_all(cursor, "SHOW TRANSACTIONS")
    assert len(results) == expected_num_results
    return results


def run_query_in_separate_thread(username: str, password: str, query: str):
    cursor = mgclient.connect(host="localhost", port=7687, username=username, password=password).cursor()
    cursor.execute(query, {})


# Tests
# -------------------------


def test_self_transaction():
    """Tests that simple show transactions work when no other is running."""
    cursor = connect().cursor()
    results = execute_and_fetch_all(cursor, "SHOW TRANSACTIONS")
    assert len(results) == 1


def test_admin_has_one_transaction():
    """Creates admin and tests that he sees only one transaction."""
    # a_cursor is used for creating admin user, simulates main thread
    a_cursor = connect().cursor()
    execute_and_fetch_all(a_cursor, "CREATE USER admin")
    execute_and_fetch_all(a_cursor, "GRANT ALL PRIVILEGES TO admin")
    admin_cursor = connect(username="admin", password="").cursor()
    admin_connection = threading.Thread(target=show_transactions_test, args=(admin_cursor, 1))
    admin_connection.start()
    admin_connection.join()
    execute_and_fetch_all(a_cursor, "DROP USER admin")


def test_long_running_query():
    main_cursor = connect().cursor()
    # run_query_in_separate_thread("", "", "CALL infinite_query.long_query() RETURN 1")
    thread_connection = threading.Thread(
        target=run_query_in_separate_thread, args=("", "", "CALL infinite_query.long_query() YIELD my_id RETURN my_id")
    )
    thread_connection.start()
    print("After run query")
    show_results = show_transactions_test(main_cursor, 2)
    long_query_id = get_non_show_transaction_id(show_results)
    print(f"Long query id: {long_query_id}")
    results = execute_and_fetch_all(main_cursor, f"TERMINATE TRANSACTIONS {long_query_id}")
    assert len(results) == 1
    # TODO: add the test that transaction was actually killed


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
