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

import typing
import mgclient
import sys
import pytest
import time
from common import *


def test_collect_unwind(connection):
    wait_for_shard_manager_to_initialize()
    cursor = connection.cursor()

    assert has_n_result_row(cursor, "CREATE (n :label {property:1})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:2})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:3})", 0)
    assert has_n_result_row(cursor, "CREATE (n :label {property:4})", 0)

    assert has_n_result_row(cursor, "MATCH (n) WITH collect(n) AS result RETURN result", 1)
    assert has_n_result_row(cursor, "MATCH (n) WITH collect(n) AS nd UNWIND nd AS result RETURN result", 4)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
