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
import threading
import time

import mgclient
import pytest
from common import connect, execute_and_fetch_all
from show_metrics_aborted_queries import aborted_queries

LONG_QUERY = "UNWIND range(1, 100000) AS x UNWIND range(1, 100000) AS y RETURN count(*)"


def run_until_terminated(errors):
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    try:
        execute_and_fetch_all(connection.cursor(), LONG_QUERY)
    except mgclient.DatabaseError as e:
        errors.append(e)


def wait_for_transaction_id(cursor, query, timeout_s=10):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        for _, transaction_id, queries, *_ in execute_and_fetch_all(cursor, "SHOW TRANSACTIONS"):
            if query in queries:
                return transaction_id
        time.sleep(0.1)
    assert False, f"Timed out waiting for a transaction running {query}"


def test_terminated_query_is_counted(connect):
    cursor = connect.cursor()
    before = aborted_queries("terminated")
    assert before is not None

    errors = []
    victim = threading.Thread(target=run_until_terminated, args=(errors,))
    victim.start()
    execute_and_fetch_all(cursor, f"TERMINATE TRANSACTIONS '{wait_for_transaction_id(cursor, LONG_QUERY)}'")
    victim.join(timeout=10)

    assert not victim.is_alive()
    assert len(errors) == 1
    assert aborted_queries("terminated") == before + 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
