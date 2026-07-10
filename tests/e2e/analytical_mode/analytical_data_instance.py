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


def test_data_instance_cannot_switch_analytical():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    cursor = connection.cursor()
    with pytest.raises(mgclient.DatabaseError) as e:
        execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_ANALYTICAL")
    assert "Data instances cannot use analytical mode" in str(e.value)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
