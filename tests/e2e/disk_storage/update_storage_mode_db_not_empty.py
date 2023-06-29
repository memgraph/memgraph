# Copyright 2023 Memgraph Ltd.
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

import pytest
from common import connect, execute_and_fetch_all


def test_forbid_switching_from_memory_to_disk_when_database_is_not_empty(connect):
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "CREATE (:DiskLabel {id: 1})")
    try:
        execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
        assert False
    except:
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
        execute_and_fetch_all(cursor, "FREE MEMORY")  # to enforce garbage collection
        assert True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
