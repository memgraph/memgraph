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

import multiprocessing
import sys
import time

import mgclient
import pytest
from common import connect, execute_and_fetch_all


def tx1_lock_vertex(conn_params, locked_barrier, done_barrier):
    connection = mgclient.connect(**conn_params)
    connection.autocommit = False
    cursor = connection.cursor()

    try:
        cursor.execute("MATCH (v:TestNode {id: 1}) SET v.prop = 'locked'")
        cursor.fetchall()
        locked_barrier.wait()
        done_barrier.wait()
    finally:
        try:
            connection.rollback()
        except Exception:
            pass
        connection.close()


def tx2_query_module(conn_params, locked_barrier, done_barrier):
    locked_barrier.wait()

    connection = mgclient.connect(**conn_params)
    connection.autocommit = False
    cursor = connection.cursor()

    error_occurred = False
    try:
        cursor.execute(
            "MATCH (v:TestNode {id: 1}) "
            "CALL write.set_property_wrapped(v, 'prop', 'modified_by_tx2') "
            "YIELD success, error "
            "RETURN success, error"
        )
        cursor.fetchall()
        connection.commit()
    except Exception as e:
        error_occurred = True
    finally:
        done_barrier.wait()


def test_serialization_error_in_query_module():
    """Tests for a specific issue: when a query module catches a serialization
    error itself, the transaction enters a poisoned state where it cannot
    be committed, and aborts the server. This test just ensures that the
    server is still running after a serialization error is caught.
    """
    cleanup_cursor = connect().cursor()
    execute_and_fetch_all(cleanup_cursor, "MATCH (n:TestNode) DELETE n")

    execute_and_fetch_all(cleanup_cursor, "CREATE (v:TestNode {id: 1})")

    conn_params = {"host": "localhost", "port": 7687}
    locked_barrier = multiprocessing.Barrier(2)
    done_barrier = multiprocessing.Barrier(2)

    tx1_process = multiprocessing.Process(target=tx1_lock_vertex, args=(conn_params, locked_barrier, done_barrier))
    tx1_process.start()

    tx2_process = multiprocessing.Process(target=tx2_query_module, args=(conn_params, locked_barrier, done_barrier))
    tx2_process.start()

    tx2_process.join(timeout=5)
    tx1_process.join(timeout=5)

    # Check server is still running.
    execute_and_fetch_all(cleanup_cursor, "MATCH (n:TestNode) DELETE n")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
