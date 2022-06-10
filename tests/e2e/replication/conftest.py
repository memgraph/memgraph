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


# The fixture here is more complex because the connection has to be
# parameterized based on the test parameters (info has to be available on both
# sides).
#
# https://docs.pytest.org/en/latest/example/parametrize.html#indirect-parametrization
# is not an elegant/feasible solution here.
#
# The solution was independently developed and then I stumbled upon the same
# approach here https://stackoverflow.com/a/68286553/4888809 which I think is
# optimal.
@pytest.fixture(scope="function")
def connection():
    connection_holder = None
    role_holder = None

    def inner_connection(port, role):
        nonlocal connection_holder, role_holder
        connection_holder = connect(host="localhost", port=port)
        role_holder = role
        return connection_holder

    yield inner_connection

    # Only main instance can be cleaned up because replicas do NOT accept
    # writes.
    if role_holder == "main":
        cursor = connection_holder.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
