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

import mgclient
import pytest
from common import execute_and_fetch_all


def storage_mode(cursor):
    rows = execute_and_fetch_all(cursor, "SHOW STORAGE INFO ON CURRENT DATABASE")
    return {row[0]: row[1] for row in rows}["storage_mode"]


def test_data_instance_switches_analytical_without_replicas():
    # A MAIN data instance with no registered replicas may enter analytical mode: nothing replicates
    # from it, so no replica can miss the writes that analytical mode keeps out of the WAL. Registering
    # replicas back is what the operator does after switching to transactional again.
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_ANALYTICAL")
    assert storage_mode(cursor) == "IN_MEMORY_ANALYTICAL"

    execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_TRANSACTIONAL")
    assert storage_mode(cursor) == "IN_MEMORY_TRANSACTIONAL"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
