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


def test_empty_show_storage_info(connect):
    cursor = connect.cursor()
    execute_and_fetch_all(cursor, "STORAGE MODE ON_DISK_TRANSACTIONAL")
    results = execute_and_fetch_all(cursor, "SHOW ACTIVE USERS INFO")
    print(results)
    # results = dict(map(lambda pair: (pair[0], pair[1]), results))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
