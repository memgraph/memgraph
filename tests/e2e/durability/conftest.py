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

import pytest
from common import connect, execute_and_fetch_all


@pytest.fixture(scope="function")
def connection():
    connection_holder = None
    role_holder = None

    def inner_connection(port, role, username="", password=""):
        nonlocal connection_holder, role_holder
        connection_holder = connect(host="localhost", port=port, username=username, password=password)
        role_holder = role
        return connection_holder

    yield inner_connection
