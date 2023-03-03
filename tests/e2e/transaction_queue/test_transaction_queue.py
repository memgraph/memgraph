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


import multiprocessing
import sys
import threading
import time
from typing import List

import mgclient
import pytest
from common import connect, execute_and_fetch_all

# Utility functions
# -------------------------


def get_non_show_transaction_id(results):
    """Returns transaction id of the first transaction that is not SHOW TRANSACTIONS;"""
    for res in results:
        if res[2] != ["SHOW TRANSACTIONS"]:
            return res[1]


def show_transactions_test(cursor, expected_num_results: int):
    results = execute_and_fetch_all(cursor, "SHOW TRANSACTIONS")
    assert len(results) == expected_num_results
    return results


def process_function(cursor, query: str):
    try:
        cursor.execute(query, {})
    except mgclient.DatabaseError:
        pass


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
    admin_cursor = connect().cursor()
    execute_and_fetch_all(admin_cursor, "CREATE USER admin")
    execute_and_fetch_all(admin_cursor, "GRANT ALL PRIVILEGES TO admin")
    admin_cursor = connect(username="admin", password="").cursor()
    admin_connection = threading.Thread(target=show_transactions_test, args=(admin_cursor, 1))
    admin_connection.start()
    admin_connection.join()
    execute_and_fetch_all(admin_cursor, "DROP USER admin")


def test_superadmin_cannot_see_admin_can_see_admin():
    """Tests that superadmin cannot see the transaction created by admin but two admins can see and kill each other's transactions."""
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin1")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO admin1")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin2")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO admin2")
    # Admin starts running infinite query
    admin_connection_1 = connect(username="admin1", password="")
    admin_cursor_1 = admin_connection_1.cursor()
    admin_connection_2 = connect(username="admin2", password="")
    admin_cursor_2 = admin_connection_2.cursor()
    process = multiprocessing.Process(
        target=process_function, args=(admin_cursor_1, "CALL infinite_query.long_query() YIELD my_id RETURN my_id")
    )
    process.start()
    time.sleep(0.5)
    # Superadmin shouldn't see the execution of the admin
    show_transactions_test(superadmin_cursor, 1)
    show_results = show_transactions_test(admin_cursor_2, 2)
    # Don't rely on the order of intepreters in Memgraph
    if show_results[0][2] == ["SHOW TRANSACTIONS"]:
        executing_index = 0
    else:
        executing_index = 1
    assert show_results[executing_index][0] == "admin2"
    assert show_results[executing_index][2] == ["SHOW TRANSACTIONS"]
    assert show_results[1 - executing_index][0] == "admin1"
    assert show_results[1 - executing_index][2] == ["CALL infinite_query.long_query() YIELD my_id RETURN my_id"]
    # Kill transaction
    long_transaction_id = show_results[1 - executing_index][1]
    execute_and_fetch_all(admin_cursor_2, f"TERMINATE TRANSACTIONS '{long_transaction_id}'")
    execute_and_fetch_all(superadmin_cursor, "DROP USER admin1")
    execute_and_fetch_all(superadmin_cursor, "DROP USER admin2")
    admin_connection_1.close()
    admin_connection_2.close()


def test_admin_sees_superadmin():
    """Tests that admin created by superadmin can see the superadmin's transaction."""
    superadmin_connection = connect()
    superadmin_cursor = superadmin_connection.cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO admin")
    # Admin starts running infinite query
    process = multiprocessing.Process(
        target=process_function, args=(superadmin_cursor, "CALL infinite_query.long_query() YIELD my_id RETURN my_id")
    )
    process.start()
    time.sleep(0.5)
    admin_cursor = connect(username="admin", password="").cursor()
    show_results = show_transactions_test(admin_cursor, 2)
    # show_results_2 = show_transactions_test(admin_cursor, 2)
    # Don't rely on the order of intepreters in Memgraph
    if show_results[0][2] == ["SHOW TRANSACTIONS"]:
        executing_index = 0
    else:
        executing_index = 1
    assert show_results[executing_index][0] == "admin"
    assert show_results[executing_index][2] == ["SHOW TRANSACTIONS"]
    assert show_results[1 - executing_index][0] == ""
    assert show_results[1 - executing_index][2] == ["CALL infinite_query.long_query() YIELD my_id RETURN my_id"]
    # Kill transaction
    long_transaction_id = show_results[1 - executing_index][1]
    execute_and_fetch_all(admin_cursor, f"TERMINATE TRANSACTIONS '{long_transaction_id}'")
    execute_and_fetch_all(admin_cursor, "DROP USER admin")
    superadmin_connection.close()


def test_admin_can_see_user_transaction():
    """Tests that admin can see user's transaction and kill it."""
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO admin")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER user")
    # Admin starts running infinite query
    admin_connection = connect(username="admin", password="")
    admin_cursor = admin_connection.cursor()
    user_connection = connect(username="user", password="")
    user_cursor = user_connection.cursor()
    process = multiprocessing.Process(
        target=process_function, args=(user_cursor, "CALL infinite_query.long_query() YIELD my_id RETURN my_id")
    )
    process.start()
    time.sleep(0.5)
    # Admin should see the user's transaction.
    show_results = show_transactions_test(admin_cursor, 2)
    # Don't rely on the order of intepreters in Memgraph
    if show_results[0][2] == ["SHOW TRANSACTIONS"]:
        executing_index = 0
    else:
        executing_index = 1
    assert show_results[executing_index][0] == "admin"
    assert show_results[executing_index][2] == ["SHOW TRANSACTIONS"]
    assert show_results[1 - executing_index][0] == "user"
    assert show_results[1 - executing_index][2] == ["CALL infinite_query.long_query() YIELD my_id RETURN my_id"]
    # Kill transaction
    long_transaction_id = show_results[1 - executing_index][1]
    execute_and_fetch_all(admin_cursor, f"TERMINATE TRANSACTIONS '{long_transaction_id}'")
    execute_and_fetch_all(superadmin_cursor, "DROP USER admin")
    execute_and_fetch_all(superadmin_cursor, "DROP USER user")
    admin_connection.close()
    user_connection.close()


def test_user_cannot_see_admin_transaction():
    """User cannot see admin's transaction but other admin can and he can kill it."""
    # Superadmin creates two admins and one user
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin1")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO admin1")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin2")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO admin2")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER user")
    admin_connection_1 = connect(username="admin1", password="")
    admin_cursor_1 = admin_connection_1.cursor()
    admin_connection_2 = connect(username="admin2", password="")
    admin_cursor_2 = admin_connection_2.cursor()
    user_connection = connect(username="user", password="")
    user_cursor = user_connection.cursor()
    # Admin1 starts running long running query
    process = multiprocessing.Process(
        target=process_function, args=(admin_cursor_1, "CALL infinite_query.long_query() YIELD my_id RETURN my_id")
    )
    process.start()
    time.sleep(0.5)
    # User should not see the admin's transaction.
    show_transactions_test(user_cursor, 1)
    # Second admin should see other admin's transactions
    show_results = show_transactions_test(admin_cursor_2, 2)
    # Don't rely on the order of intepreters in Memgraph
    if show_results[0][2] == ["SHOW TRANSACTIONS"]:
        executing_index = 0
    else:
        executing_index = 1
    assert show_results[executing_index][0] == "admin2"
    assert show_results[executing_index][2] == ["SHOW TRANSACTIONS"]
    assert show_results[1 - executing_index][0] == "admin1"
    assert show_results[1 - executing_index][2] == ["CALL infinite_query.long_query() YIELD my_id RETURN my_id"]
    # Kill transaction
    long_transaction_id = show_results[1 - executing_index][1]
    execute_and_fetch_all(admin_cursor_2, f"TERMINATE TRANSACTIONS '{long_transaction_id}'")
    execute_and_fetch_all(superadmin_cursor, "DROP USER admin1")
    execute_and_fetch_all(superadmin_cursor, "DROP USER admin2")
    execute_and_fetch_all(superadmin_cursor, "DROP USER user")
    admin_connection_1.close()
    admin_connection_2.close()
    user_connection.close()


def test_killing_non_existing_transaction():
    cursor = connect().cursor()
    results = execute_and_fetch_all(cursor, "TERMINATE TRANSACTIONS '1'")
    assert len(results) == 1
    assert results[0][0] == "1"  # transaction id
    assert results[0][1] == False  # not killed


def test_killing_multiple_non_existing_transactions():
    cursor = connect().cursor()
    transactions_id = ["'1'", "'2'", "'3'"]
    results = execute_and_fetch_all(cursor, f"TERMINATE TRANSACTIONS {','.join(transactions_id)}")
    assert len(results) == 3
    for i in range(len(results)):
        assert results[i][0] == eval(transactions_id[i])  # transaction id
        assert results[i][1] == False  # not killed


def test_admin_killing_multiple_non_existing_transactions():
    # Starting, superadmin admin
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO admin")
    # Connect with admin
    admin_cursor = connect(username="admin", password="").cursor()
    transactions_id = ["'1'", "'2'", "'3'"]
    results = execute_and_fetch_all(admin_cursor, f"TERMINATE TRANSACTIONS {','.join(transactions_id)}")
    assert len(results) == 3
    for i in range(len(results)):
        assert results[i][0] == eval(transactions_id[i])  # transaction id
        assert results[i][1] == False  # not killed
    execute_and_fetch_all(admin_cursor, "DROP USER admin")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
