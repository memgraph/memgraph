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

import time
import typing

import mgclient
import pytest


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def wait_for_results(
    cursor: mgclient.Cursor, query: str, timeout_sec: float = 2.0, poll_interval: float = 0.1
) -> typing.List[tuple]:
    """Poll query until non-empty results (for async AFTER COMMIT triggers)."""
    start_time = time.time()
    while time.time() - start_time < timeout_sec:
        results = execute_and_fetch_all(cursor, query)
        if len(results) > 0:
            return results
        time.sleep(poll_interval)
    return []


@pytest.fixture
def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    execute_and_fetch_all(connection.cursor(), "USE DATABASE memgraph")
    try:
        execute_and_fetch_all(connection.cursor(), "DROP DATABASE clean")
    except:
        pass
    execute_and_fetch_all(connection.cursor(), "MATCH (n) DETACH DELETE n")
    triggers_list = execute_and_fetch_all(connection.cursor(), "SHOW TRIGGERS;")
    for trigger in triggers_list:
        execute_and_fetch_all(connection.cursor(), f"DROP TRIGGER {trigger[0]}")
    execute_and_fetch_all(connection.cursor(), "MATCH (n) DETACH DELETE n")
    yield connection
