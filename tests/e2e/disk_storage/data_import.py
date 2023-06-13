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

num_entries = 100000


def test_disk_import_fail(connect):
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    try:
        execute_and_fetch_all(cursor, "FOREACH (i IN range(1, {num_entries}) | CREATE (n:DiskLabel {{id: i}}));")
        assert False
    except:
        assert True


def test_batched_disk_import_passes(connect):
    step = int(num_entries / 5)
    for i in range(1, num_entries, step):
        cursor = connect.cursor()
        execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
        query = "FOREACH (i IN range({i}, {i} + {step}) | CREATE (n:DiskLabel {{id: {i}}}));".format(i=i, step=step)
        try:
            execute_and_fetch_all(cursor, query)
        except:
            assert False
        cursor.close()
    assert True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
