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
import time
from typing import List

import mgclient
import pytest
from common import connect, execute_and_fetch_all

# Module-level setup
# -------------------------


@pytest.fixture(scope="module", autouse=True)
def suppress_builtin_roles():
    """Create a dummy role before any users so builtin role creation is suppressed."""
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE ROLE _dummy_role;")


# Utility functions
# -------------------------

LONG_QUERY = "CALL infinite_query.long_query() YIELD my_id RETURN my_id"


def get_non_show_transaction_id(results):
    """Returns transaction id of the first transaction that is not SHOW TRANSACTIONS;"""
    for res in results:
        if res[2] != ["SHOW TRANSACTIONS"]:
            return res[1]


def show_transactions_test(cursor, expected_num_results: int):
    results = execute_and_fetch_all(cursor, "SHOW TRANSACTIONS")
    assert len(results) == expected_num_results
    return results


def wait_for_transaction_count(cursor, expected_num_results: int, timeout_s: int = 10):
    """Polls SHOW TRANSACTIONS until the expected number of transactions is visible."""
    deadline = time.time() + timeout_s
    while True:
        results = execute_and_fetch_all(cursor, "SHOW TRANSACTIONS")
        if len(results) == expected_num_results:
            return
        if time.time() >= deadline:
            assert False, f"Timed out waiting for {expected_num_results} transactions, saw {results}"
        time.sleep(0.1)


def wait_for_query_count(cursor, query: str, expected_num_results: int, timeout_s: int = 10):
    """Polls SHOW TRANSACTIONS until the expected number of transactions are running query.

    A victim that runs several queries in turn is visible between them, so counting transactions
    alone can be satisfied while a victim still has the query of interest ahead of it.
    """
    deadline = time.time() + timeout_s
    while True:
        results = execute_and_fetch_all(cursor, "SHOW TRANSACTIONS")
        if sum(1 for result in results if query in result[2]) == expected_num_results:
            return
        if time.time() >= deadline:
            assert False, f"Timed out waiting for {expected_num_results} transactions running {query}, saw {results}"
        time.sleep(0.1)


def process_function(cursor, queries: List[str]):
    try:
        for query in queries:
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


def test_multitenant_transactions():
    """Tests that show transactions work on another database"""
    test_cursor = connect().cursor()
    execute_and_fetch_all(test_cursor, "CREATE DATABASE testing")
    tx_connection = connect()
    tx_cursor = tx_connection.cursor()
    tx_process = multiprocessing.Process(
        target=process_function, args=(tx_cursor, ["USE DATABASE testing", "MATCH (n) RETURN n"])
    )
    tx_process.start()
    time.sleep(0.5)
    show_transactions_test(test_cursor, 1)
    # TODO Add SHOW TRANSACTIONS ON * that should return all transactions


def test_admin_has_one_transaction(request):
    """Creates admin and tests that he sees only one transaction."""
    # a_cursor is used for creating admin user, simulates main thread
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER admin")

    request.addfinalizer(on_exit)

    admin_cursor = connect(username="admin", password="").cursor()
    process = multiprocessing.Process(target=show_transactions_test, args=(admin_cursor, 1))
    process.start()
    process.join()


def test_user_can_see_its_transaction(request):
    """Tests that user without privileges can see its own transaction"""
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER user")
    execute_and_fetch_all(superadmin_cursor, "REVOKE ALL PRIVILEGES FROM user")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER admin")
        execute_and_fetch_all(superadmin_cursor, "DROP USER user")

    request.addfinalizer(on_exit)

    user_cursor = connect(username="user", password="").cursor()
    process = multiprocessing.Process(target=show_transactions_test, args=(user_cursor, 1))
    process.start()
    process.join()
    admin_cursor = connect(username="admin", password="").cursor()


def test_explicit_transaction_output(request):
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER admin")

    request.addfinalizer(on_exit)

    admin_connection = connect(username="admin", password="")
    admin_cursor = admin_connection.cursor()
    # Admin starts running explicit transaction
    process = multiprocessing.Process(
        target=process_function,
        args=(superadmin_cursor, ["BEGIN", "CREATE (n:Person {id_: 1})", "CREATE (n:Person {id_: 2})"]),
    )
    process.start()
    time.sleep(0.5)
    show_results = show_transactions_test(admin_cursor, 2)
    if show_results[0][2] == ["SHOW TRANSACTIONS"]:
        executing_index = 0
    else:
        executing_index = 1
    assert show_results[1 - executing_index][2] == ["CREATE (n:Person {id_: 1})", "CREATE (n:Person {id_: 2})"]

    execute_and_fetch_all(superadmin_cursor, "ROLLBACK")


def test_superadmin_cannot_see_admin_can_see_admin(request):
    """Tests that superadmin cannot see the transaction created by admin but two admins can see and kill each other's transactions."""
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin1")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO admin1")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin1")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin2")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT  TO admin2")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin2")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER admin1")
        execute_and_fetch_all(superadmin_cursor, "DROP USER admin2")

    request.addfinalizer(on_exit)

    # Admin starts running infinite query
    admin_connection_1 = connect(username="admin1", password="")
    admin_cursor_1 = admin_connection_1.cursor()
    admin_connection_2 = connect(username="admin2", password="")
    admin_cursor_2 = admin_connection_2.cursor()
    process = multiprocessing.Process(
        target=process_function, args=(admin_cursor_1, ["CALL infinite_query.long_query() YIELD my_id RETURN my_id"])
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
    admin_connection_1.close()
    admin_connection_2.close()


def test_admin_sees_superadmin(request):
    """Tests that admin created by superadmin can see the superadmin's transaction."""
    superadmin_connection = connect()
    superadmin_cursor = superadmin_connection.cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin")

    def on_exit():
        execute_and_fetch_all(admin_cursor, "DROP USER admin")

    request.addfinalizer(on_exit)

    # Admin starts running infinite query
    process = multiprocessing.Process(
        target=process_function, args=(superadmin_cursor, ["CALL infinite_query.long_query() YIELD my_id RETURN my_id"])
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
    superadmin_connection.close()


def test_admin_can_see_user_transaction(request):
    """Tests that admin can see user's transaction and kill it."""
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER user")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER admin")
        execute_and_fetch_all(superadmin_cursor, "DROP USER user")

    request.addfinalizer(on_exit)

    # Admin starts running infinite query
    admin_connection = connect(username="admin", password="")
    admin_cursor = admin_connection.cursor()
    user_connection = connect(username="user", password="")
    user_cursor = user_connection.cursor()
    process = multiprocessing.Process(
        target=process_function, args=(user_cursor, ["CALL infinite_query.long_query() YIELD my_id RETURN my_id"])
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
    admin_connection.close()
    user_connection.close()


def test_user_cannot_see_admin_transaction(request):
    """User cannot see admin's transaction but other admin can and he can kill it."""
    # Superadmin creates two admins and one user
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin1")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO admin1")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin1")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin2")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO admin2")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin2")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER user")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER admin1")
        execute_and_fetch_all(superadmin_cursor, "DROP USER admin2")
        execute_and_fetch_all(superadmin_cursor, "DROP USER user")

    request.addfinalizer(on_exit)

    admin_connection_1 = connect(username="admin1", password="")
    admin_cursor_1 = admin_connection_1.cursor()
    admin_connection_2 = connect(username="admin2", password="")
    admin_cursor_2 = admin_connection_2.cursor()
    user_connection = connect(username="user", password="")
    user_cursor = user_connection.cursor()
    # Admin1 starts running long running query
    process = multiprocessing.Process(
        target=process_function, args=(admin_cursor_1, ["CALL infinite_query.long_query() YIELD my_id RETURN my_id"])
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
    admin_connection_1.close()
    admin_connection_2.close()
    user_connection.close()


def test_wildcard_admin_kills_all(request):
    """An admin's TERMINATE TRANSACTIONS "*" kills every other transaction it can see."""
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER admin")

    request.addfinalizer(on_exit)

    victim_connections = [connect(username="admin", password="") for _ in range(2)]
    processes = [
        multiprocessing.Process(
            target=process_function,
            args=(connection.cursor(), [LONG_QUERY]),
        )
        for connection in victim_connections
    ]
    for process in processes:
        process.start()

    admin_cursor = connect(username="admin", password="").cursor()
    wait_for_transaction_count(admin_cursor, 3)  # two victims plus the SHOW itself

    results = execute_and_fetch_all(admin_cursor, 'TERMINATE TRANSACTIONS "*"')
    assert len(results) == 2
    assert all(result[1] == True for result in results)
    # Rows come back ordered by ascending transaction id, one row per distinct victim.
    reported_ids = [result[0] for result in results]
    assert reported_ids == sorted(reported_ids, key=int)
    assert len(set(reported_ids)) == 2

    # Only the sweeping session is left.
    wait_for_transaction_count(admin_cursor, 1)

    for process in processes:
        process.join()
    for connection in victim_connections:
        connection.close()


def test_wildcard_no_transactions_returns_empty():
    """With nothing else running the sweep returns no rows, and the statement still succeeds
    (its own transaction is excluded, so its commit is not aborted)."""
    cursor = connect().cursor()
    results = execute_and_fetch_all(cursor, 'TERMINATE TRANSACTIONS "*"')
    assert len(results) == 0
    # The session survived its own sweep.
    assert len(execute_and_fetch_all(cursor, "SHOW TRANSACTIONS")) == 1


def test_wildcard_unprivileged_kills_only_own(request):
    """An unprivileged user's wildcard reaches only its own transactions, never anyone else's."""
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER user")
    execute_and_fetch_all(superadmin_cursor, "REVOKE ALL PRIVILEGES FROM user")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER admin")
        execute_and_fetch_all(superadmin_cursor, "DROP USER user")

    request.addfinalizer(on_exit)

    admin_connection = connect(username="admin", password="")
    user_victim_connection = connect(username="user", password="")
    admin_process = multiprocessing.Process(
        target=process_function,
        args=(admin_connection.cursor(), [LONG_QUERY]),
    )
    user_process = multiprocessing.Process(
        target=process_function,
        args=(user_victim_connection.cursor(), [LONG_QUERY]),
    )
    admin_process.start()
    user_process.start()

    admin_observer_cursor = connect(username="admin", password="").cursor()
    wait_for_transaction_count(admin_observer_cursor, 3)  # both victims plus the SHOW itself

    def admin_long_query_id():
        """The admin's long query, told apart from the observer's own SHOW row by its query text."""
        return next(
            result[1]
            for result in execute_and_fetch_all(admin_observer_cursor, "SHOW TRANSACTIONS")
            if result[0] == "admin" and result[2] == [LONG_QUERY]
        )

    admin_transaction_id = admin_long_query_id()

    # The user sweeps: it kills its own long query and cannot touch the admin's.
    user_cursor = connect(username="user", password="").cursor()
    results = execute_and_fetch_all(user_cursor, 'TERMINATE TRANSACTIONS "*"')
    assert len(results) == 1
    assert results[0][1] == True
    assert results[0][0] != admin_transaction_id

    # The admin's transaction is untouched and still visible to the admin.
    wait_for_transaction_count(admin_observer_cursor, 2)
    assert admin_long_query_id() == admin_transaction_id

    # The survivor has to be killed here: the long query runs in a forked child, so closing the
    # connection from this process would leave it running and later tests would count it.
    execute_and_fetch_all(admin_observer_cursor, f"TERMINATE TRANSACTIONS '{admin_transaction_id}'")
    wait_for_transaction_count(admin_observer_cursor, 1)

    admin_process.join()
    user_process.join()
    admin_connection.close()
    user_victim_connection.close()


def test_wildcard_across_databases(request):
    """The wildcard is not scoped to the caller's database: one sweep kills transactions on
    every database the caller has TRANSACTION_MANAGEMENT for."""
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE DATABASE wildcard_db_a")
    execute_and_fetch_all(superadmin_cursor, "CREATE DATABASE wildcard_db_b")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER admin")
        execute_and_fetch_all(superadmin_cursor, "DROP DATABASE wildcard_db_a")
        execute_and_fetch_all(superadmin_cursor, "DROP DATABASE wildcard_db_b")

    request.addfinalizer(on_exit)

    victim_connections = [connect(username="admin", password="") for _ in range(2)]
    processes = [
        multiprocessing.Process(
            target=process_function,
            args=(
                connection.cursor(),
                [f"USE DATABASE {db_name}", LONG_QUERY],
            ),
        )
        for connection, db_name in zip(victim_connections, ["wildcard_db_a", "wildcard_db_b"])
    ]
    for process in processes:
        process.start()

    # The sweeping session stays on the default database.
    admin_cursor = connect(username="admin", password="").cursor()
    # Each victim runs USE DATABASE before the long query, so wait for the long queries themselves
    # rather than for a count that the USE DATABASE transactions can satisfy on their own.
    wait_for_query_count(admin_cursor, LONG_QUERY, 2)

    results = execute_and_fetch_all(admin_cursor, 'TERMINATE TRANSACTIONS "*"')
    assert len(results) == 2
    assert all(result[1] == True for result in results)
    wait_for_transaction_count(admin_cursor, 1)

    # The victim sessions have to be gone before the databases they used can be dropped.
    for process in processes:
        process.join()
    for connection in victim_connections:
        connection.close()


def test_wildcard_rejects_mixed_list():
    """The wildcard must be the sole argument, so its meaning never depends on the rest."""
    cursor = connect().cursor()
    for query in [
        "TERMINATE TRANSACTIONS \"*\", '1'",
        "TERMINATE TRANSACTIONS '1', \"*\"",
        'TERMINATE TRANSACTIONS "*", "*"',
    ]:
        with pytest.raises(mgclient.DatabaseError):
            execute_and_fetch_all(cursor, query)


def test_wildcard_and_id_share_the_cached_query():
    """Query stripping replaces the id literal with a parameter, so the wildcard and a named id
    strip to the same query and share one cached AST. Each execution must follow its own literal
    instead of the one that populated the cache."""
    cursor = connect().cursor()
    assert len(execute_and_fetch_all(cursor, 'TERMINATE TRANSACTIONS "*"')) == 0

    # Same cache entry, different parameter: read as a named id, not as a sweep.
    results = execute_and_fetch_all(cursor, "TERMINATE TRANSACTIONS '1'")
    assert len(results) == 1
    assert results[0][0] == "1"
    assert results[0][1] == False

    # And back, so a wildcard decision cached from the first execution would show up here.
    assert len(execute_and_fetch_all(cursor, 'TERMINATE TRANSACTIONS "*"')) == 0


def test_unauthorized_terminate_reports_not_killed(request):
    """An unprivileged user naming someone else's transaction id gets killed=false, and the
    transaction survives. Reporting killed=true would both lie and confirm the id exists."""
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER user")
    execute_and_fetch_all(superadmin_cursor, "REVOKE ALL PRIVILEGES FROM user")

    def on_exit():
        execute_and_fetch_all(superadmin_cursor, "DROP USER admin")
        execute_and_fetch_all(superadmin_cursor, "DROP USER user")

    request.addfinalizer(on_exit)

    admin_connection = connect(username="admin", password="")
    admin_cursor = admin_connection.cursor()
    user_connection = connect(username="user", password="")
    user_cursor = user_connection.cursor()

    # Admin runs a long query; the admin's own second session reads its id.
    process = multiprocessing.Process(target=process_function, args=(admin_cursor, [LONG_QUERY]))
    process.start()
    admin_observer_cursor = connect(username="admin", password="").cursor()
    wait_for_transaction_count(admin_observer_cursor, 2)
    admin_transaction_id = get_non_show_transaction_id(show_transactions_test(admin_observer_cursor, 2))

    # The unprivileged user cannot kill it, and must not be told it exists.
    results = execute_and_fetch_all(user_cursor, f"TERMINATE TRANSACTIONS '{admin_transaction_id}'")
    assert len(results) == 1
    assert results[0][0] == admin_transaction_id
    assert results[0][1] == False  # not killed, indistinguishable from a missing id

    # The transaction survived and is still visible to the admin.
    assert get_non_show_transaction_id(show_transactions_test(admin_observer_cursor, 2)) == admin_transaction_id

    # The admin can still kill it.
    results = execute_and_fetch_all(admin_observer_cursor, f"TERMINATE TRANSACTIONS '{admin_transaction_id}'")
    assert results[0][1] == True
    wait_for_transaction_count(admin_observer_cursor, 1)

    admin_connection.close()
    user_connection.close()


def test_terminate_rejects_malformed_ids():
    """Ids must parse in full; a numeric prefix must not silently target another transaction."""
    cursor = connect().cursor()
    for bad_id in ["'1abc'", "'ALL'", "''", "'0x10'", "'1 '", "123"]:
        with pytest.raises(mgclient.DatabaseError):
            execute_and_fetch_all(cursor, f"TERMINATE TRANSACTIONS {bad_id}")


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


def test_admin_killing_multiple_non_existing_transactions(request):
    # Starting, superadmin admin
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT TRANSACTION_MANAGEMENT TO admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin")

    def on_exit():
        execute_and_fetch_all(admin_cursor, "DROP USER admin")

    request.addfinalizer(on_exit)

    # Connect with admin
    admin_cursor = connect(username="admin", password="").cursor()
    transactions_id = ["'1'", "'2'", "'3'"]
    results = execute_and_fetch_all(admin_cursor, f"TERMINATE TRANSACTIONS {','.join(transactions_id)}")
    assert len(results) == 3
    for i in range(len(results)):
        assert results[i][0] == eval(transactions_id[i])  # transaction id
        assert results[i][1] == False  # not killed


def test_user_killing_some_transactions():
    """Tests what happens when user can kill only some of the transactions given."""
    superadmin_cursor = connect().cursor()
    execute_and_fetch_all(superadmin_cursor, "CREATE USER admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT ALL PRIVILEGES TO admin")
    execute_and_fetch_all(superadmin_cursor, "GRANT DATABASE * TO admin")
    execute_and_fetch_all(superadmin_cursor, "CREATE USER user1")
    execute_and_fetch_all(superadmin_cursor, "REVOKE ALL PRIVILEGES FROM user1")

    # Connect with user in two different sessions
    admin_cursor = connect(username="admin", password="").cursor()
    execute_and_fetch_all(admin_cursor, "CREATE USER user2")
    execute_and_fetch_all(admin_cursor, "GRANT ALL PRIVILEGES TO user2")
    user_connection_1 = connect(username="user1", password="")
    user_cursor_1 = user_connection_1.cursor()
    user_connection_2 = connect(username="user2", password="")
    user_cursor_2 = user_connection_2.cursor()
    process_1 = multiprocessing.Process(
        target=process_function, args=(user_cursor_1, ["CALL infinite_query.long_query() YIELD my_id RETURN my_id"])
    )
    process_2 = multiprocessing.Process(target=process_function, args=(user_cursor_2, ["BEGIN", "MATCH (n) RETURN n"]))
    process_1.start()
    process_2.start()
    # Create another user1 connections
    user_connection_1_copy = connect(username="user1", password="")
    user_cursor_1_copy = user_connection_1_copy.cursor()
    # Run in this while loop since it is possible that process 1 hasn't started yet.
    query_not_started = True
    time_passed = 0
    while query_not_started:
        query_not_started = len(execute_and_fetch_all(user_cursor_1_copy, "SHOW TRANSACTIONS")) != 2
        time.sleep(1)
        # Avoid running same test forever
        time_passed += 1
        if time_passed > 10:
            assert False

    show_user_1_results = show_transactions_test(user_cursor_1_copy, 2)
    if show_user_1_results[0][2] == ["SHOW TRANSACTIONS"]:
        execution_index = 0
    else:
        execution_index = 1
    assert show_user_1_results[1 - execution_index][2] == ["CALL infinite_query.long_query() YIELD my_id RETURN my_id"]
    # Connect with admin
    time.sleep(0.5)
    show_admin_results = show_transactions_test(admin_cursor, 3)
    for show_admin_res in show_admin_results:
        if show_admin_res[2] != "[SHOW TRANSACTIONS]":
            execute_and_fetch_all(admin_cursor, f"TERMINATE TRANSACTIONS '{show_admin_res[1]}'")
    user_connection_1.close()
    user_connection_2.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
