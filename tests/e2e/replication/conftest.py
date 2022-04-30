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

import pytest

from common import execute_and_fetch_all, connect


@pytest.fixture(scope="function")
def connection():
    connection = None
    replication_role = None

    def _connection(port, role):
        nonlocal connection, replication_role
        connection = connect(host="localhost", port=port)
        replication_role = role
        return connection

    yield _connection
    if replication_role == "main":
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
