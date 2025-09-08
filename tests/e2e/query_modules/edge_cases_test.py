# Copyright 2025 Memgraph Ltd.
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

# this would cause a crash in the past
def test_nested_calls():
    cursor = connect().cursor()

    query = "CREATE (n) WITH n CALL { WITH n UNWIND [n,n] AS m WITH m CALL { WITH m CALL module.procedure(m) YIELD result RETURN result } RETURN result } RETURN result;"
    result = execute_and_fetch_all(cursor, query)
    assert len(result) == 1
    assert result[0][0] == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
