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
from typing import List, Optional

import mgclient
import pytest


@pytest.fixture(autouse=True)
def connection():
    connection = connect()
    return connection


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: Optional[dict] = None) -> List[tuple]:
    cursor.execute(query, params if params else {})
    return cursor.fetchall()


def has_n_result_row(cursor: mgclient.Cursor, query: str, n: int) -> bool:
    results = execute_and_fetch_all(cursor, query)
    return len(results) == n


def wait_for_shard_manager_to_initialize():
    # The ShardManager in memgraph takes some time to initialize
    # the shards, thus we cannot just run the queries right away
    time.sleep(3)
