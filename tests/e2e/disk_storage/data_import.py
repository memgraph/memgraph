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

import mgclient
import pytest
from common import connect, execute_and_fetch_all

# TODO: andi You should add configuration flag that you can start Memgraph with disk storage as default
# def test_disk_import_fail(connect):
#     cursor = connect.cursor()
#     execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
#     # TODO:andi when merged master to this epic, tes that show storage info shows the correct storage mode
#     try:
#         execute_and_fetch_all(cursor, "FOREACH (i IN range(1, 100000) | CREATE (n:DiskLabel {id: i}));")
#         assert True
#     except:
#         assert False


# TODO: andi You should add configuration flag that you can start Memgraph with disk storage as default
# TODO: this should also be tested with unit tests
def test_batched_disk_import_passes(connect):
    num_entries = 1000000
    step = int(num_entries / 100)
    for i in range(1, num_entries, step):
        cursor = connect.cursor()
        execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
        query = "FOREACH (i IN range({i}, {i} + {step}) | CREATE (n:DiskLabel {{id: {i}}}));".format(i=i, step=step)
        print(f"Query: {query}")
        tx = (i - 1) / step + 1
        try:
            execute_and_fetch_all(cursor, query)
            print(f"Finished tx: {tx}/{num_entries/step}")
        except:
            print(f"Failed in tx: {tx}/{num_entries/step}")
            assert False
        cursor.close()
    assert True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
